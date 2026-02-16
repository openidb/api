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
    select: { bookId: true, hadithNumber: true, text: true, source: true },
  });

  const cachedMap = new Map(
    cached.map((t) => [`${t.bookId}-${t.hadithNumber}`, t])
  );

  // Separate cached vs uncached
  const translations: Array<{
    bookId: number;
    hadithNumber: string;
    translation: string;
    source: string;
  }> = [];

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

  // 3. Batch LLM call for untranslated hadiths
  const numberedHadiths = toTranslate
    .map((h, i) => `[${i}] ${h.text}`)
    .join("\n\n");

  const isEnglish = language === "en";

  const prompt = `Translate the following Arabic hadiths to ${languageName}.
Each hadith is numbered with [N]. Return a JSON array where each element has "index" (the hadith number) and "translation" (the ${languageName} text).
Only output the translation — do not include the original Arabic or the [N] markers.

Guidelines:
- Translate the full hadith faithfully, preserving isnad (chain of narrators) and matn (body text).
- Keep narrator names in standard transliterated forms (e.g. Abu Hurayrah, Ibn Abbas, Aisha).${isEnglish ? `
- "حدثنا" / "أخبرنا" → "narrated to us" / "informed us"
- "عن" → "from" or "on the authority of" (in isnad context)` : `
- Use the conventional ${languageName} hadith narration terms for "حدثنا", "أخبرنا", "عن".`}
- Keep "Allah" as-is. Keep Islamic terms (Salah, Zakat, Hajj, Sunnah, Jannah, etc.) in their transliterated or conventional ${languageName} forms.
- Use ﷺ or the conventional ${languageName} honorific for "صلى الله عليه وسلم".

Arabic hadiths:
${numberedHadiths}

Translate to ${languageName}. Respond with ONLY a valid JSON array. Example:
[{"index": 0, "translation": "..."}, {"index": 1, "translation": "..."}]`;

  const model = "google/gemini-2.0-flash-001";
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

  // 4. Parse JSON response
  let llmTranslations: Array<{ index: number; translation: string }> = [];
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
        Number.isFinite(item.index) &&
        typeof item?.translation === "string"
      ) {
        llmTranslations.push({
          index: item.index,
          translation: item.translation.slice(0, 30_000),
        });
      }
    }
  } catch {
    // Return cached translations even if LLM parsing fails
    return c.json({ translations, error: "Failed to parse LLM response" });
  }

  // 5. Persist and collect results
  for (const t of llmTranslations) {
    const hadith = toTranslate[t.index];
    if (!hadith) continue;

    // Validate the translation is in the target language (for non-Latin-script languages)
    if (isWrongLanguage(t.translation, language)) {
      console.warn(`[hadith-translate] Wrong language detected for ${hadith.bookId}:${hadith.hadithNumber} (expected ${language}), skipping cache`);
      // Still return it to the user (better than nothing) but don't cache it
      translations.push({
        bookId: hadith.bookId,
        hadithNumber: hadith.hadithNumber,
        translation: t.translation,
        source: "llm",
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
          translation: t.translation,
          source: "llm",
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
        update: { text: t.translation, source: "llm", model: modelKey },
        create: {
          bookId: hadith.bookId,
          hadithNumber: hadith.hadithNumber,
          language,
          text: t.translation,
          source: "llm",
          model: modelKey,
        },
      });
    } catch (err) {
      console.error(`[hadith-translate] Failed to persist translation for ${hadith.bookId}:${hadith.hadithNumber}:`, err);
      // Still return the translation even if caching failed
    }

    translations.push({
      bookId: hadith.bookId,
      hadithNumber: hadith.hadithNumber,
      translation: t.translation,
      source: "llm",
    });
  }

  return c.json({ translations });
});
