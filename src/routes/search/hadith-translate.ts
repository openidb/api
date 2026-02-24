import { Hono } from "hono";
import { z } from "zod";
import { prisma } from "../../db";
import { callOpenRouter } from "../../lib/openrouter";

const RequestSchema = z.object({
  hadiths: z
    .array(
      z.object({
        bookId: z.number(),
        hadithNumber: z.string(),
        collectionSlug: z.string(),
        text: z.string().max(30_000),
      })
    )
    .min(1)
    .max(10),
  language: z.string().min(2).max(5),
});

const LANGUAGE_NAMES: Record<string, string> = {
  en: "English",
  fr: "French",
  es: "Spanish",
  id: "Indonesian",
  ur: "Urdu",
  zh: "Chinese",
  pt: "Portuguese",
  ru: "Russian",
  ja: "Japanese",
  ko: "Korean",
  it: "Italian",
  bn: "Bengali",
  de: "German",
  fa: "Persian",
  ha: "Hausa",
  hi: "Hindi",
  ku: "Kurdish",
  ms: "Malay",
  nl: "Dutch",
  pa: "Punjabi",
  ps: "Pashto",
  so: "Somali",
  sw: "Swahili",
  ta: "Tamil",
  tr: "Turkish",
  uz: "Uzbek",
  yo: "Yoruba",
};

// Languages that use non-Latin scripts — we can detect if the LLM accidentally returned English
const NON_LATIN_LANGS = new Set(["ru", "zh", "ja", "ko", "bn", "hi", "ur", "fa", "pa", "ps", "ta"]);

/** Returns true if the text is mostly Latin letters but the target language uses a non-Latin script. */
function isWrongLanguage(text: string, targetLang: string): boolean {
  if (!NON_LATIN_LANGS.has(targetLang)) return false;
  const letters = [...text].filter((ch) => /\p{L}/u.test(ch));
  if (letters.length < 10) return false;
  const latinRatio = letters.filter((ch) => /[a-zA-Z]/.test(ch)).length / letters.length;
  return latinRatio > 0.7;
}

/** Structured translation fields stored in HadithTranslation */
interface StructuredTranslation {
  isnadTranslation: string | null;
  matnTranslation: string | null;
  footnotesTranslation: string | null;
  kitabTranslation: string | null;
  chapterTranslation: string | null;
  gradeExplanationTranslation: string | null;
}

/** Response item for a translated hadith */
interface TranslationResponseItem {
  bookId: number;
  hadithNumber: string;
  translation: string;
  source: string;
  isnadTranslation?: string | null;
  matnTranslation?: string | null;
  footnotesTranslation?: string | null;
  kitabTranslation?: string | null;
  chapterTranslation?: string | null;
  gradeExplanationTranslation?: string | null;
}

export const hadithTranslateRoutes = new Hono();

hadithTranslateRoutes.post("/translate-hadiths", async (c) => {
  const body = await c.req.json();
  const parsed = RequestSchema.safeParse(body);
  if (!parsed.success) {
    return c.json({ error: "Invalid request", details: parsed.error.issues }, 400);
  }

  const { hadiths, language } = parsed.data;
  const languageName = LANGUAGE_NAMES[language] || language;

  // 1. Check cache — find existing translations for all requested hadiths
  const orConditions = hadiths.map((h) => ({
    bookId: h.bookId,
    hadithNumber: h.hadithNumber,
  }));

  const cached = await prisma.hadithTranslation.findMany({
    where: {
      language,
      OR: orConditions,
    },
    select: {
      bookId: true, hadithNumber: true, text: true, source: true,
      isnadTranslation: true, matnTranslation: true, footnotesTranslation: true,
      kitabTranslation: true, chapterTranslation: true, gradeExplanationTranslation: true,
    },
  });

  const cachedMap = new Map(
    cached.map((t) => [`${t.bookId}-${t.hadithNumber}`, t])
  );

  // Separate cached vs uncached
  const translations: TranslationResponseItem[] = [];

  const toTranslate: typeof hadiths = [];

  for (const h of hadiths) {
    const key = `${h.bookId}-${h.hadithNumber}`;
    const hit = cachedMap.get(key);
    if (hit) {
      // If cached LLM translation is in the wrong language, re-translate instead
      if (hit.source === "llm" && isWrongLanguage(hit.text, language)) {
        toTranslate.push(h);
      } else {
        translations.push({
          bookId: h.bookId,
          hadithNumber: h.hadithNumber,
          translation: hit.text,
          source: hit.source || "llm",
          isnadTranslation: hit.isnadTranslation,
          matnTranslation: hit.matnTranslation,
          footnotesTranslation: hit.footnotesTranslation,
          kitabTranslation: hit.kitabTranslation,
          chapterTranslation: hit.chapterTranslation,
          gradeExplanationTranslation: hit.gradeExplanationTranslation,
        });
      }
    } else {
      toTranslate.push(h);
    }
  }

  // 2. If all cached, return immediately
  if (toTranslate.length === 0) {
    return c.json({ translations });
  }

  // 3. Fetch hadith records for structured fields (isnad, matn, footnotes, kitab, chapter, gradeExplanation)
  const hadithRecords = await prisma.hadith.findMany({
    where: {
      OR: toTranslate.map((h) => ({ bookId: h.bookId, hadithNumber: h.hadithNumber })),
    },
    select: {
      bookId: true, hadithNumber: true,
      isnad: true, matn: true, footnotes: true,
      kitabArabic: true, chapterArabic: true, gradeExplanation: true,
    },
  });

  const hadithRecordMap = new Map(
    hadithRecords.map((h) => [`${h.bookId}-${h.hadithNumber}`, h])
  );

  // 4. Build structured LLM prompt
  const isEnglish = language === "en";

  // Build input blocks with labeled fields per hadith
  const numberedInputs = toTranslate.map((h, i) => {
    const record = hadithRecordMap.get(`${h.bookId}-${h.hadithNumber}`);
    const parts: string[] = [`[${i}]`];

    if (record?.isnad) {
      parts.push(`ISNAD: ${record.isnad}`);
    }
    if (record?.matn) {
      parts.push(`MATN: ${record.matn}`);
    }
    // If neither isnad nor matn, use the full text
    if (!record?.isnad && !record?.matn) {
      parts.push(`MATN: ${h.text}`);
    }
    if (record?.footnotes) {
      parts.push(`FOOTNOTES: ${record.footnotes}`);
    }
    if (record?.kitabArabic) {
      parts.push(`KITAB: ${record.kitabArabic}`);
    }
    if (record?.chapterArabic) {
      parts.push(`CHAPTER: ${record.chapterArabic}`);
    }
    if (record?.gradeExplanation) {
      parts.push(`GRADE_EXPLANATION: ${record.gradeExplanation}`);
    }
    return parts.join("\n");
  }).join("\n\n");

  const prompt = `Translate the following Arabic hadith fields to ${languageName}.
Each hadith is numbered with [N] and has labeled fields (ISNAD, MATN, FOOTNOTES, KITAB, CHAPTER, GRADE_EXPLANATION).
Return a JSON array where each element has:
- "index": the hadith number [N]
- "isnad": translated chain of narrators (if ISNAD was provided)
- "matn": translated hadith body text (if MATN was provided)
- "footnotes": translated scholarly footnotes (if FOOTNOTES was provided)
- "kitab": translated book/section heading (if KITAB was provided)
- "chapter": translated chapter heading (if CHAPTER was provided)
- "gradeExplanation": translated grade reasoning (if GRADE_EXPLANATION was provided)

Only include fields that were present in the input. Do not include the original Arabic or the field labels.

Context: These hadiths are the recorded sayings and actions of the Prophet Muhammad ﷺ, the final Prophet and Messenger of Allah sent as a mercy to all of creation. His character was described by his wife Aisha (may Allah be pleased with her) as "the Quran" — he embodied its teachings — and Allah praised him saying "You are truly of outstanding character" (Quran 68:4).

Guidelines:
- Translate each field faithfully, preserving the meaning and tone.
- Keep narrator names in standard transliterated forms (e.g. Abu Hurayrah, Ibn Abbas, Aisha).${isEnglish ? `
- "حدثنا" / "أخبرنا" → "narrated to us" / "informed us"
- "عن" → "from" or "on the authority of" (in isnad context)` : `
- Use the conventional ${languageName} hadith narration terms for "حدثنا", "أخبرنا", "عن".`}
- Keep "Allah" as-is. Keep Islamic terms (Salah, Zakat, Hajj, Sunnah, Jannah, etc.) in their transliterated or conventional ${languageName} forms.
- Use ﷺ or the conventional ${languageName} honorific for "صلى الله عليه وسلم".

Arabic hadiths:
${numberedInputs}

Translate to ${languageName}. Respond with ONLY a valid JSON array. Example:
[{"index": 0, "isnad": "Narrated to us by...", "matn": "The Prophet ﷺ said...", "footnotes": "Also narrated by...", "kitab": "Book of Prayer", "chapter": "Chapter on the Night Prayer"}]`;

  const model = "google/gemini-3-flash-preview";
  const modelKey = "gemini-flash";

  const result = await callOpenRouter({
    model,
    messages: [{ role: "user", content: prompt }],
    temperature: 0.3,
    timeoutMs: 60_000,
  });

  if (!result) {
    // Return whatever was cached; skip LLM translations
    return c.json({ translations });
  }

  // 5. Parse JSON response
  interface LLMTranslationItem {
    index: number;
    isnad?: string;
    matn?: string;
    footnotes?: string;
    kitab?: string;
    chapter?: string;
    gradeExplanation?: string;
    translation?: string; // fallback for backward compat
  }

  let llmTranslations: LLMTranslationItem[] = [];
  try {
    let cleaned = result.content.trim();
    if (cleaned.startsWith("```json")) cleaned = cleaned.slice(7);
    else if (cleaned.startsWith("```")) cleaned = cleaned.slice(3);
    if (cleaned.endsWith("```")) cleaned = cleaned.slice(0, -3);
    const parsed = JSON.parse(cleaned.trim());
    if (!Array.isArray(parsed)) {
      return c.json({ translations, error: "Failed to parse LLM response" });
    }
    for (const item of parsed.slice(0, toTranslate.length)) {
      if (
        typeof item?.index === "number" &&
        Number.isFinite(item.index)
      ) {
        llmTranslations.push({
          index: item.index,
          isnad: typeof item.isnad === "string" ? item.isnad.slice(0, 30_000) : undefined,
          matn: typeof item.matn === "string" ? item.matn.slice(0, 30_000) : undefined,
          footnotes: typeof item.footnotes === "string" ? item.footnotes.slice(0, 30_000) : undefined,
          kitab: typeof item.kitab === "string" ? item.kitab.slice(0, 1000) : undefined,
          chapter: typeof item.chapter === "string" ? item.chapter.slice(0, 1000) : undefined,
          gradeExplanation: typeof item.gradeExplanation === "string" ? item.gradeExplanation.slice(0, 30_000) : undefined,
          translation: typeof item.translation === "string" ? item.translation.slice(0, 30_000) : undefined,
        });
      }
    }
  } catch {
    // Return cached translations even if LLM parsing fails
    return c.json({ translations, error: "Failed to parse LLM response" });
  }

  // 6. Persist and collect results
  for (const t of llmTranslations) {
    const hadith = toTranslate[t.index];
    if (!hadith) continue;

    // Compose full text from structured parts (isnad + matn) for backward compat
    const composedText = [t.isnad, t.matn].filter(Boolean).join(" ") || t.translation || "";

    // Validate the translation is in the target language (for non-Latin-script languages)
    if (isWrongLanguage(composedText, language)) {
      console.warn(`[hadith-translate] Wrong language detected for ${hadith.bookId}:${hadith.hadithNumber} (expected ${language}), skipping cache`);
      translations.push({
        bookId: hadith.bookId,
        hadithNumber: hadith.hadithNumber,
        translation: composedText,
        source: "llm",
        isnadTranslation: t.isnad || null,
        matnTranslation: t.matn || null,
        footnotesTranslation: t.footnotes || null,
        kitabTranslation: t.kitab || null,
        chapterTranslation: t.chapter || null,
        gradeExplanationTranslation: t.gradeExplanation || null,
      });
      continue;
    }

    try {
      // Only insert if no translation exists — never overwrite human translations
      const existing = await prisma.hadithTranslation.findUnique({
        where: {
          bookId_hadithNumber_language: {
            bookId: hadith.bookId,
            hadithNumber: hadith.hadithNumber,
            language,
          },
        },
        select: { source: true },
      });

      if (existing && existing.source !== "llm") {
        // Human translation exists — use it instead, don't overwrite
        translations.push({
          bookId: hadith.bookId,
          hadithNumber: hadith.hadithNumber,
          translation: composedText,
          source: "llm",
          isnadTranslation: t.isnad || null,
          matnTranslation: t.matn || null,
          footnotesTranslation: t.footnotes || null,
          kitabTranslation: t.kitab || null,
          chapterTranslation: t.chapter || null,
          gradeExplanationTranslation: t.gradeExplanation || null,
        });
        continue;
      }

      await prisma.hadithTranslation.upsert({
        where: {
          bookId_hadithNumber_language: {
            bookId: hadith.bookId,
            hadithNumber: hadith.hadithNumber,
            language,
          },
        },
        update: {
          text: composedText,
          source: "llm",
          model: modelKey,
          isnadTranslation: t.isnad || null,
          matnTranslation: t.matn || null,
          footnotesTranslation: t.footnotes || null,
          kitabTranslation: t.kitab || null,
          chapterTranslation: t.chapter || null,
          gradeExplanationTranslation: t.gradeExplanation || null,
        },
        create: {
          bookId: hadith.bookId,
          hadithNumber: hadith.hadithNumber,
          language,
          text: composedText,
          source: "llm",
          model: modelKey,
          isnadTranslation: t.isnad || null,
          matnTranslation: t.matn || null,
          footnotesTranslation: t.footnotes || null,
          kitabTranslation: t.kitab || null,
          chapterTranslation: t.chapter || null,
          gradeExplanationTranslation: t.gradeExplanation || null,
        },
      });
    } catch (err) {
      console.error(`[hadith-translate] Failed to persist translation for ${hadith.bookId}:${hadith.hadithNumber}:`, err);
      // Still return the translation even if caching failed
    }

    translations.push({
      bookId: hadith.bookId,
      hadithNumber: hadith.hadithNumber,
      translation: composedText,
      source: "llm",
      isnadTranslation: t.isnad || null,
      matnTranslation: t.matn || null,
      footnotesTranslation: t.footnotes || null,
      kitabTranslation: t.kitab || null,
      chapterTranslation: t.chapter || null,
      gradeExplanationTranslation: t.gradeExplanation || null,
    });
  }

  return c.json({ translations });
});
