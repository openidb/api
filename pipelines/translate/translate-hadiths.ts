/**
 * Batch Hadith Translation Pipeline
 *
 * Translates hadiths into target language using Gemini Flash via OpenRouter.
 * Stores structured fields (isnad, matn, footnotes, kitab, chapter, gradeExplanation)
 * in HadithTranslation table.
 *
 * Usage:
 *   bun run pipelines/translate/translate-hadiths.ts --lang=en
 *   bun run pipelines/translate/translate-hadiths.ts --lang=en --collection=bukhari
 *   bun run pipelines/translate/translate-hadiths.ts --lang=en --concurrency=10 --batch-size=5
 *   bun run pipelines/translate/translate-hadiths.ts --lang=en --force
 *   bun run pipelines/translate/translate-hadiths.ts --lang=en --dry-run
 */

import "../env";
import { prisma } from "../../src/db";
import { callOpenRouter } from "../../src/lib/openrouter";
import { COLLECTIONS, type CollectionConfig } from "../import/turath-hadith-configs";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const LANGUAGE_NAMES: Record<string, string> = {
  en: "English", fr: "French", id: "Indonesian", ur: "Urdu",
  es: "Spanish", zh: "Chinese", pt: "Portuguese", ru: "Russian",
  ja: "Japanese", ko: "Korean", it: "Italian", bn: "Bengali",
  ha: "Hausa", sw: "Swahili", nl: "Dutch", de: "German",
  tr: "Turkish", fa: "Persian", hi: "Hindi", ms: "Malay",
  pa: "Punjabi", ku: "Kurdish", ps: "Pashto", so: "Somali",
  uz: "Uzbek", yo: "Yoruba", ta: "Tamil",
};

const MODEL = "google/gemini-3-flash-preview";
const MODEL_KEY = "gemini-flash";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface CLIArgs {
  lang: string;
  collectionSlug: string | null;
  concurrency: number;
  batchSize: number;
  force: boolean;
  dryRun: boolean;
}

interface HadithRow {
  bookId: number;
  hadithNumber: string;
  textArabic: string;
  isnad: string | null;
  matn: string | null;
  footnotes: string | null;
  kitabArabic: string | null;
  chapterArabic: string | null;
  gradeExplanation: string | null;
}

interface LLMTranslationItem {
  index: number;
  isnad?: string;
  matn?: string;
  footnotes?: string;
  kitab?: string;
  chapter?: string;
  gradeExplanation?: string;
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function parseArgs(): CLIArgs {
  const args = process.argv.slice(2);
  let lang = "";
  let collectionSlug: string | null = null;
  let concurrency = 10;
  let batchSize = 5;
  let force = false;
  let dryRun = false;

  for (const arg of args) {
    if (arg.startsWith("--lang=")) lang = arg.slice(7);
    else if (arg.startsWith("--collection=")) collectionSlug = arg.slice(13);
    else if (arg.startsWith("--concurrency=")) concurrency = parseInt(arg.slice(14), 10) || 10;
    else if (arg.startsWith("--batch-size=")) batchSize = parseInt(arg.slice(13), 10) || 5;
    else if (arg === "--force") force = true;
    else if (arg === "--dry-run") dryRun = true;
  }

  if (!lang) { console.error("Error: --lang=<code> is required"); process.exit(1); }
  if (!LANGUAGE_NAMES[lang]) {
    console.error(`Error: unsupported language "${lang}". Supported: ${Object.keys(LANGUAGE_NAMES).join(", ")}`);
    process.exit(1);
  }

  return { lang, collectionSlug, concurrency, batchSize, force, dryRun };
}

// ---------------------------------------------------------------------------
// LLM prompt (reused from hadith-translate.ts)
// ---------------------------------------------------------------------------

function buildPrompt(hadiths: HadithRow[], indices: number[], languageName: string, lang: string): string {
  const isEnglish = lang === "en";

  const numberedInputs = indices.map((idx, i) => {
    const h = hadiths[idx];
    const parts: string[] = [`[${i}]`];

    if (h.isnad) parts.push(`ISNAD: ${h.isnad}`);
    if (h.matn) parts.push(`MATN: ${h.matn}`);
    if (!h.isnad && !h.matn) parts.push(`MATN: ${h.textArabic}`);
    if (h.footnotes) parts.push(`FOOTNOTES: ${h.footnotes}`);
    if (h.kitabArabic) parts.push(`KITAB: ${h.kitabArabic}`);
    if (h.chapterArabic) parts.push(`CHAPTER: ${h.chapterArabic}`);
    if (h.gradeExplanation) parts.push(`GRADE_EXPLANATION: ${h.gradeExplanation}`);

    return parts.join("\n");
  }).join("\n\n");

  return `Translate the following Arabic hadith fields to ${languageName}.
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
}

// ---------------------------------------------------------------------------
// LLM translation with retry
// ---------------------------------------------------------------------------

function cleanLLMResponse(content: string): string {
  let cleaned = content.trim();
  if (cleaned.startsWith("```json")) cleaned = cleaned.slice(7);
  else if (cleaned.startsWith("```")) cleaned = cleaned.slice(3);
  if (cleaned.endsWith("```")) cleaned = cleaned.slice(0, -3);
  return cleaned.trim();
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function translateBatchWithRetry(
  hadiths: HadithRow[],
  indices: number[],
  languageName: string,
  lang: string,
  maxRetries = 3,
): Promise<LLMTranslationItem[]> {
  const prompt = buildPrompt(hadiths, indices, languageName, lang);
  let lastError: Error | null = null;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const result = await callOpenRouter({
        model: MODEL,
        messages: [{ role: "user", content: prompt }],
        temperature: 0.3,
        timeoutMs: 120_000,
      });

      if (!result) throw new Error("No response from OpenRouter");

      const cleaned = cleanLLMResponse(result.content);
      const parsed = JSON.parse(cleaned);
      if (!Array.isArray(parsed)) throw new Error("LLM response is not an array");

      const translations: LLMTranslationItem[] = [];
      for (const item of parsed) {
        if (typeof item?.index === "number" && Number.isFinite(item.index)) {
          translations.push({
            index: item.index,
            isnad: typeof item.isnad === "string" ? item.isnad.slice(0, 30_000) : undefined,
            matn: typeof item.matn === "string" ? item.matn.slice(0, 30_000) : undefined,
            footnotes: typeof item.footnotes === "string" ? item.footnotes.slice(0, 30_000) : undefined,
            kitab: typeof item.kitab === "string" ? item.kitab.slice(0, 1000) : undefined,
            chapter: typeof item.chapter === "string" ? item.chapter.slice(0, 1000) : undefined,
            gradeExplanation: typeof item.gradeExplanation === "string" ? item.gradeExplanation.slice(0, 30_000) : undefined,
          });
        }
      }

      if (translations.length === 0) throw new Error("No valid translations parsed");
      return translations;
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));
      const isRateLimit = lastError.message.includes("429") || lastError.message.includes("rate");
      const backoff = isRateLimit ? 3000 * Math.pow(2, attempt) : 1000 * (attempt + 1);

      if (attempt < maxRetries - 1) {
        console.warn(`    [retry ${attempt + 1}/${maxRetries}] ${lastError.message}, waiting ${backoff}ms...`);
        await sleep(backoff);
      }
    }
  }

  throw lastError || new Error("Translation failed after retries");
}

// ---------------------------------------------------------------------------
// Persist translations
// ---------------------------------------------------------------------------

async function persistTranslations(
  hadiths: HadithRow[],
  indices: number[],
  translations: LLMTranslationItem[],
  lang: string,
): Promise<number> {
  let saved = 0;

  for (const t of translations) {
    const hadith = hadiths[indices[t.index]];
    if (!hadith) continue;

    const composedText = [t.isnad, t.matn].filter(Boolean).join(" ") || "";
    if (!composedText) continue;

    try {
      await prisma.hadithTranslation.upsert({
        where: {
          bookId_hadithNumber_language: {
            bookId: hadith.bookId,
            hadithNumber: hadith.hadithNumber,
            language: lang,
          },
        },
        update: {
          text: composedText,
          source: "llm",
          model: MODEL_KEY,
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
          language: lang,
          text: composedText,
          source: "llm",
          model: MODEL_KEY,
          isnadTranslation: t.isnad || null,
          matnTranslation: t.matn || null,
          footnotesTranslation: t.footnotes || null,
          kitabTranslation: t.kitab || null,
          chapterTranslation: t.chapter || null,
          gradeExplanationTranslation: t.gradeExplanation || null,
        },
      });
      saved++;
    } catch (err) {
      console.error(`    Failed to persist ${hadith.bookId}:${hadith.hadithNumber}:`, err);
    }
  }

  return saved;
}

// ---------------------------------------------------------------------------
// Process one collection
// ---------------------------------------------------------------------------

async function translateCollection(
  config: CollectionConfig,
  args: CLIArgs,
): Promise<{ translated: number; skipped: number; failed: number }> {
  const { lang, batchSize, concurrency, force, dryRun } = args;
  const languageName = LANGUAGE_NAMES[lang];

  // Find the collection + all its book IDs
  const collection = await prisma.hadithCollection.findUnique({
    where: { slug: config.slug },
    select: { id: true, books: { select: { id: true } } },
  });

  if (!collection) {
    console.error(`  Collection "${config.slug}" not found in DB`);
    return { translated: 0, skipped: 0, failed: 0 };
  }

  const bookIds = collection.books.map((b) => b.id);

  // Fetch all hadiths for this collection
  const allHadiths: HadithRow[] = await prisma.hadith.findMany({
    where: { bookId: { in: bookIds } },
    orderBy: [{ bookId: "asc" }, { hadithNumber: "asc" }],
    select: {
      bookId: true,
      hadithNumber: true,
      textArabic: true,
      isnad: true,
      matn: true,
      footnotes: true,
      kitabArabic: true,
      chapterArabic: true,
      gradeExplanation: true,
    },
  });

  if (allHadiths.length === 0) {
    console.log(`  No hadiths found for ${config.slug}`);
    return { translated: 0, skipped: 0, failed: 0 };
  }

  // Filter out already-translated hadiths (unless --force)
  let toTranslateIndices: number[];
  if (force) {
    toTranslateIndices = allHadiths.map((_, i) => i);
  } else {
    const existingTranslations = await prisma.hadithTranslation.findMany({
      where: {
        bookId: { in: bookIds },
        language: lang,
      },
      select: { bookId: true, hadithNumber: true },
    });
    const existingKeys = new Set(existingTranslations.map((t) => `${t.bookId}-${t.hadithNumber}`));
    toTranslateIndices = allHadiths
      .map((h, i) => ({ key: `${h.bookId}-${h.hadithNumber}`, i }))
      .filter(({ key }) => !existingKeys.has(key))
      .map(({ i }) => i);

    if (toTranslateIndices.length < allHadiths.length) {
      console.log(`  Skipping ${allHadiths.length - toTranslateIndices.length} already-translated hadiths`);
    }
  }

  if (toTranslateIndices.length === 0) {
    console.log(`  All ${allHadiths.length} hadiths already translated`);
    return { translated: 0, skipped: allHadiths.length, failed: 0 };
  }

  console.log(`  ${toTranslateIndices.length} hadiths to translate (batch size: ${batchSize}, concurrency: ${concurrency})`);

  if (dryRun) {
    const batches = Math.ceil(toTranslateIndices.length / batchSize);
    console.log(`  [DRY RUN] Would send ${batches} batches`);
    return { translated: 0, skipped: 0, failed: 0 };
  }

  // Build batches
  const batches: number[][] = [];
  for (let i = 0; i < toTranslateIndices.length; i += batchSize) {
    batches.push(toTranslateIndices.slice(i, i + batchSize));
  }

  // Worker pool
  let nextBatch = 0;
  let totalTranslated = 0;
  let totalFailed = 0;

  async function worker() {
    while (nextBatch < batches.length) {
      const batchIdx = nextBatch++;
      const batch = batches[batchIdx];
      const first = allHadiths[batch[0]];
      const last = allHadiths[batch[batch.length - 1]];

      try {
        const translations = await translateBatchWithRetry(allHadiths, batch, languageName, lang);
        const saved = await persistTranslations(allHadiths, batch, translations, lang);
        totalTranslated += saved;

        if ((batchIdx + 1) % 10 === 0 || batchIdx === batches.length - 1) {
          console.log(`  [${batchIdx + 1}/${batches.length}] ${totalTranslated} saved so far (${first.hadithNumber}-${last.hadithNumber})`);
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        console.error(`  [batch ${batchIdx}] FAILED (${first.hadithNumber}-${last.hadithNumber}): ${msg}`);
        totalFailed += batch.length;
      }
    }
  }

  const workers = Array.from({ length: Math.min(concurrency, batches.length) }, () => worker());
  await Promise.all(workers);

  return { translated: totalTranslated, skipped: allHadiths.length - toTranslateIndices.length, failed: totalFailed };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const args = parseArgs();

  console.log("Hadith Translation Pipeline");
  console.log(`Language: ${args.lang} | Concurrency: ${args.concurrency} | Batch size: ${args.batchSize}`);
  if (args.force) console.log("Force mode: re-translating existing translations");
  if (args.dryRun) console.log("Dry run mode: no translations will be performed");

  // Determine which collections to process
  const slugs = args.collectionSlug
    ? [args.collectionSlug]
    : Object.keys(COLLECTIONS);

  let grandTotal = 0;
  let grandFailed = 0;

  for (const slug of slugs) {
    const config = COLLECTIONS[slug];
    if (!config) {
      console.error(`Unknown collection: ${slug}`);
      continue;
    }

    console.log(`\n${"=".repeat(60)}`);
    console.log(`${config.name} (${config.slug})`);
    console.log(`${"=".repeat(60)}`);

    const startTime = Date.now();
    const { translated, skipped, failed } = await translateCollection(config, args);
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

    console.log(`  Done in ${elapsed}s — ${translated} translated, ${skipped} skipped, ${failed} failed`);
    grandTotal += translated;
    grandFailed += failed;
  }

  console.log(`\nTotal: ${grandTotal} translations saved, ${grandFailed} failed`);
  await prisma.$disconnect();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  prisma.$disconnect();
  process.exit(1);
});
