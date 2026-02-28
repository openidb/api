/**
 * Batch Hadith Translation Pipeline
 *
 * For each hadith: identifies the isnad/matn split point in Arabic,
 * then translates to the target language. Stores both the Arabic split
 * and the structured English translation.
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
import { appendFileSync } from "fs";

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

const DEFAULT_MODEL = "google/gemini-3-flash-preview";
const FALLBACK_MODEL = "google/gemini-2.5-pro-preview";
const MODEL_KEY = "gemini-flash";

// Use --model=fallback to switch to Claude Haiku (for hadiths that trigger Gemini safety filters)
const usesFallback = process.argv.includes("--model=fallback");
const MODEL = usesFallback ? FALLBACK_MODEL : DEFAULT_MODEL;

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
  id: number;
  bookId: number;
  hadithNumber: string;
  textArabic: string;
  footnotes: string | null;
  kitabArabic: string | null;
  chapterArabic: string | null;
  gradeExplanation: string | null;
}

interface LLMResultItem {
  index: number;
  matnStart?: string | null;
  isnad?: string | null;
  matn?: string | null;
  text: string;
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
  let concurrency = 30;
  let batchSize = 3;
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
// LLM prompt
// ---------------------------------------------------------------------------

function buildPrompt(hadiths: HadithRow[], indices: number[], languageName: string, lang: string): string {
  const isEnglish = lang === "en";

  const numberedInputs = indices.map((idx, i) => {
    const h = hadiths[idx];
    const parts: string[] = [`[${i}]`];

    parts.push(`TEXT: ${h.textArabic}`);
    if (h.footnotes) parts.push(`FOOTNOTES: ${h.footnotes}`);
    if (h.kitabArabic) parts.push(`KITAB: ${h.kitabArabic}`);
    if (h.chapterArabic) parts.push(`CHAPTER: ${h.chapterArabic}`);
    if (h.gradeExplanation) parts.push(`GRADE_EXPLANATION: ${h.gradeExplanation}`);

    return parts.join("\n");
  }).join("\n\n");

  return `Translate the following Arabic hadiths to ${languageName}, and identify the isnad/matn split point.

Each hadith is numbered with [N] and has a TEXT field (the full Arabic text), plus optional FOOTNOTES, KITAB, CHAPTER, GRADE_EXPLANATION fields.

For each hadith, return a JSON object with:
- "index": the hadith number [N]
- "matnStart": the first 5-8 words of the matn (body text) copied EXACTLY from the Arabic TEXT — same diacritics, same spelling, same characters. Must be long enough to be unique within the hadith. If no isnad/matn split applies, set to null.
- "isnad": translated chain of narrators (everything before matnStart), or null if no split
- "matn": translated body text (everything from matnStart onward), or null if no split
- "text": full translated text (always provided — used when no split, or as fallback)
- "footnotes": translated scholarly footnotes (if FOOTNOTES was provided)
- "kitab": translated book/section heading (if KITAB was provided)
- "chapter": translated chapter heading (if CHAPTER was provided)
- "gradeExplanation": translated grade reasoning (if GRADE_EXPLANATION was provided)

How to identify the isnad/matn split:
- The isnad is the chain of transmission: scholars narrating from one another using "حدثنا" / "أخبرنا" / "عن" etc. It ends with the final narrator (usually a companion) and their "قال/قالت".
- The isnad may contain multiple instances of "قال" within the chain itself (e.g. "حدثنا يعقوب قال حدثنا إسماعيل"). These are all part of the isnad.
- The matn begins where the actual narrated CONTENT starts — what the companion witnessed, experienced, or heard. This includes:
  - "قال رسول الله ﷺ: إنما الأعمال..." (the Prophet speaking is narrated content)
  - "غزوت مع النبي ﷺ..." (companion describing events)
  - "كان النبي ﷺ يصلي..." (companion describing the Prophet's habits)
  - "رأيت النبي ﷺ..." (companion reporting what they saw)
- In short: the isnad = WHO transmitted it. The matn = WHAT was transmitted (including "the Prophet ﷺ said" when present).

Not all hadiths have an isnad/matn split. Set matnStart to null when:
- The text is a du'a (supplication) with no narrator chain, e.g. "اللهم إني أعوذ بك..."
- The text is editorial or commentary with no narration structure
In these cases, return only "text" (the full translation) and set isnad, matn, and matnStart to null.

Context: These hadiths are the recorded sayings and actions of the Prophet Muhammad ﷺ, the final Prophet and Messenger of Allah sent as a mercy to all of creation. His character was described by his wife Aisha (may Allah be pleased with her) as "the Quran" — he embodied its teachings — and Allah praised him saying "You are truly of outstanding character" (Quran 68:4).

Translation guidelines:
- Translate each field faithfully, preserving the meaning and tone.
- Keep narrator names in standard transliterated forms (e.g. Abu Hurayrah, Ibn Abbas, Aisha).${isEnglish ? `
- "حدثنا" / "أخبرنا" → "narrated to us" / "informed us"
- "عن" → "from" or "on the authority of" (in isnad context)` : `
- Use the conventional ${languageName} hadith narration terms for "حدثنا", "أخبرنا", "عن".`}
- Keep "Allah" as-is. Keep Islamic terms (Salah, Zakat, Hajj, Sunnah, Jannah, etc.) in their transliterated or conventional ${languageName} forms.
- Use ﷺ or the conventional ${languageName} honorific for "صلى الله عليه وسلم".

Consistency rules (IMPORTANT — follow these strictly for every hadith in the batch):
- Clarifying markers: When you add words not explicitly in the Arabic to clarify meaning, wrap them in ˹...˺ (Unicode angle brackets). Use these for implied subjects, contextual glosses, or disambiguations that aid the reader. Do NOT overuse them — only when the added word is genuinely absent from the Arabic but needed for natural ${languageName}.
- KITAB headings: Translate the meaning into natural ${languageName}. Do NOT transliterate Arabic titles. E.g. "كتاب الصلاة" → "Book of Prayer", not "Kitab as-Salah".
- Quoting: When the Prophet ﷺ or anyone is quoted speaking, always use double quotes ("...") consistently. Never use single quotes or unquoted speech.
- Translate the Arabic text faithfully as-is. Do not rearrange, omit, or move any part of it.
- CRITICAL: Only translate the exact text provided in each field. Do NOT complete, extend, or add content from memory even if you recognize a well-known hadith. If the TEXT appears truncated or incomplete, translate only what is given — do not fill in the rest.

Arabic hadiths:
${numberedInputs}

Translate to ${languageName}. Respond with ONLY a valid JSON array. Example:
[{"index": 0, "matnStart": "قَالَ رَسُولُ اللَّهِ صلى الله عليه", "isnad": "On the authority of Umar bin al-Khattab (may Allah be pleased with him) who said:", "matn": "The Messenger of Allah ﷺ said: \\"Verily actions are by intentions...\\"", "text": "On the authority of Umar bin al-Khattab (may Allah be pleased with him) who said: The Messenger of Allah ﷺ said: \\"Verily actions are by intentions...\\"", "kitab": "Book of Faith", "chapter": "Chapter on Intentions"}]`;
}

// ---------------------------------------------------------------------------
// Arabic split logic
// ---------------------------------------------------------------------------

// Tashkeel (diacritics) + tatweel
const DIACRITICS_RE = /[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7\u06E8\u06EA-\u06ED\u0640]/;

// Arabic ligature expansions (single-char honorifics → spelled-out forms)
const LIGATURE_MAP: Record<string, string> = {
  "\uFDFA": "صلى الله عليه وسلم",   // ﷺ
  "\uFD41": "رضي الله عنه",          // ﵁
  "\uFD42": "رضي الله عنها",         // ﵂
  "\uFD43": "رضي الله عنهم",         // ﵃
  "\uFD44": "رضي الله عنهما",        // ﵄
  "\uFDFD": "بسم الله الرحمن الرحيم", // ﷽
};

// Match any ligature char (including unknown ones we just strip)
const LIGATURE_RE = /[\uFD40-\uFD4F\uFDF0-\uFDFF]/g;

function expandLigatures(text: string): string {
  return text.replace(LIGATURE_RE, (ch) => LIGATURE_MAP[ch] ?? "");
}

function normalize(text: string): string {
  return expandLigatures(text)
    .replace(new RegExp(DIACRITICS_RE.source, "g"), "")
    .replace(/\s+/g, " ")
    .trim();
}

// For index mapping, we need to know how many normalized chars each original char produces.
// Walk original text char by char, tracking the cumulative normalized length.
function findSplitPoint(textArabic: string, matnStart: string): number {
  // 1. Try exact substring match
  const exactIdx = textArabic.indexOf(matnStart);
  if (exactIdx > 0) return exactIdx;

  // 2. Normalize both and match
  const normText = normalize(textArabic);
  const normMatn = normalize(matnStart);
  const normIdx = normText.indexOf(normMatn);
  if (normIdx <= 0) return -1;

  // 3. Map normalized index back to original position
  // Build a mapping: for each original char, track how many normalized chars it contributes
  let normCount = 0;
  let i = 0;
  while (i < textArabic.length) {
    if (normCount >= normIdx) return i;
    const ch = textArabic[i];
    const cp = ch.codePointAt(0)!;

    if (DIACRITICS_RE.test(ch)) {
      // Diacritic/tatweel — stripped, contributes 0
      i++;
    } else if (cp >= 0xFD40 && cp <= 0xFDFF) {
      // Ligature — expands to multiple chars (or 0 if unknown)
      const expansion = LIGATURE_MAP[ch];
      if (expansion) {
        // The expansion after stripping diacritics and collapsing whitespace
        const normExpansion = expansion
          .replace(new RegExp(DIACRITICS_RE.source, "g"), "");
        normCount += normExpansion.length;
      }
      // Unknown ligatures contribute 0
      i++;
    } else if (/\s/.test(ch)) {
      // Whitespace run → 1 normalized char
      normCount++;
      i++;
      while (i < textArabic.length && /\s/.test(textArabic[i])) i++;
    } else {
      normCount++;
      i++;
    }
  }
  return -1;
}

// ---------------------------------------------------------------------------
// LLM call with retry
// ---------------------------------------------------------------------------

function cleanLLMResponse(content: string): string {
  let cleaned = content.trim();
  if (cleaned.startsWith("```json")) cleaned = cleaned.slice(7);
  else if (cleaned.startsWith("```")) cleaned = cleaned.slice(3);
  if (cleaned.endsWith("```")) cleaned = cleaned.slice(0, -3);
  // Fix LLM outputting JS string concatenation inside JSON values: "foo" + "bar" → "foobar"
  cleaned = cleaned.replace(/"\s*\+\s*"/g, "");
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
): Promise<LLMResultItem[]> {
  const prompt = buildPrompt(hadiths, indices, languageName, lang);
  let lastError: Error | null = null;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const result = await callOpenRouter({
        model: MODEL,
        messages: [{ role: "user", content: prompt }],
        temperature: 0.3,
        timeoutMs: 120_000,
        maxTokens: 16_384,
      });

      if (!result) throw new Error("No response from OpenRouter");

      const cleaned = cleanLLMResponse(result.content);
      let parsed: any;
      try {
        parsed = JSON.parse(cleaned);
      } catch (parseErr) {
        // Dump raw response for debugging
        const debugFile = `/tmp/debug-llm-response-${Date.now()}.txt`;
        await Bun.write(debugFile, `=== RAW ===\n${result.content}\n\n=== CLEANED ===\n${cleaned}`);
        console.warn(`    [debug] Raw LLM response saved to ${debugFile}`);
        throw parseErr;
      }
      if (!Array.isArray(parsed)) throw new Error("LLM response is not an array");

      const results: LLMResultItem[] = [];
      for (const item of parsed) {
        if (typeof item?.index === "number" && Number.isFinite(item.index)) {
          results.push({
            index: item.index,
            matnStart: typeof item.matnStart === "string" ? item.matnStart : null,
            isnad: typeof item.isnad === "string" ? item.isnad.slice(0, 30_000) : null,
            matn: typeof item.matn === "string" ? item.matn.slice(0, 30_000) : null,
            text: typeof item.text === "string" ? item.text.slice(0, 60_000) : "",
            footnotes: typeof item.footnotes === "string" ? item.footnotes.slice(0, 30_000) : undefined,
            kitab: typeof item.kitab === "string" ? item.kitab.slice(0, 1000) : undefined,
            chapter: typeof item.chapter === "string" ? item.chapter.slice(0, 1000) : undefined,
            gradeExplanation: typeof item.gradeExplanation === "string" ? item.gradeExplanation.slice(0, 30_000) : undefined,
          });
        }
      }

      if (results.length === 0) throw new Error("No valid results parsed");
      return results;
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
// Persist results (Arabic split + translation)
// ---------------------------------------------------------------------------

let splitFound = 0;
let splitNotFound = 0;
let noSplitApplicable = 0;
let splitMissLogPath = "";

async function persistResults(
  hadiths: HadithRow[],
  indices: number[],
  results: LLMResultItem[],
  lang: string,
): Promise<number> {
  let saved = 0;

  for (const r of results) {
    const hadith = hadiths[indices[r.index]];
    if (!hadith) continue;
    if (!r.text && !r.isnad && !r.matn) continue;

    // 1. Apply Arabic isnad/matn split
    if (r.matnStart) {
      const splitIdx = findSplitPoint(hadith.textArabic, r.matnStart);
      if (splitIdx > 0) {
        const arabicIsnad = hadith.textArabic.slice(0, splitIdx).trim();
        const arabicMatn = hadith.textArabic.slice(splitIdx).trim();
        try {
          await prisma.hadith.update({
            where: { id: hadith.id },
            data: { isnad: arabicIsnad, matn: arabicMatn },
          });
          splitFound++;
        } catch (err) {
          console.error(`    Failed to update Arabic split for ${hadith.bookId}:${hadith.hadithNumber}:`, err);
        }
      } else {
        splitNotFound++;
        const missLine = `${hadith.bookId}\t${hadith.hadithNumber}\t${r.matnStart.slice(0, 80)}\t${hadith.textArabic.slice(0, 120)}`;
        if (splitMissLogPath) {
          try { appendFileSync(splitMissLogPath, missLine + "\n"); } catch {}
        }
        console.warn(`    [split miss] #${hadith.hadithNumber}: matnStart="${r.matnStart.slice(0, 60)}" not found`);
      }
    } else {
      noSplitApplicable++;
    }

    // 2. Save translation
    const composedText = r.text || [r.isnad, r.matn].filter(Boolean).join(" ") || "";
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
          isnadTranslation: r.isnad || null,
          matnTranslation: r.matn || null,
          footnotesTranslation: r.footnotes || null,
          kitabTranslation: r.kitab || null,
          chapterTranslation: r.chapter || null,
          gradeExplanationTranslation: r.gradeExplanation || null,
        },
        create: {
          bookId: hadith.bookId,
          hadithNumber: hadith.hadithNumber,
          language: lang,
          text: composedText,
          source: "llm",
          model: MODEL_KEY,
          isnadTranslation: r.isnad || null,
          matnTranslation: r.matn || null,
          footnotesTranslation: r.footnotes || null,
          kitabTranslation: r.kitab || null,
          chapterTranslation: r.chapter || null,
          gradeExplanationTranslation: r.gradeExplanation || null,
        },
      });
      saved++;
    } catch (err) {
      console.error(`    Failed to persist translation ${hadith.bookId}:${hadith.hadithNumber}:`, err);
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

  const collection = await prisma.hadithCollection.findUnique({
    where: { slug: config.slug },
    select: { id: true, books: { select: { id: true } } },
  });

  if (!collection) {
    console.error(`  Collection "${config.slug}" not found in DB`);
    return { translated: 0, skipped: 0, failed: 0 };
  }

  const bookIds = collection.books.map((b) => b.id);

  const allHadiths: HadithRow[] = await prisma.hadith.findMany({
    where: { bookId: { in: bookIds } },
    orderBy: [{ bookId: "asc" }, { hadithNumber: "asc" }],
    select: {
      id: true,
      bookId: true,
      hadithNumber: true,
      textArabic: true,
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
        const results = await translateBatchWithRetry(allHadiths, batch, languageName, lang);
        const saved = await persistResults(allHadiths, batch, results, lang);
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

  console.log("Hadith Translation Pipeline (with Arabic split)");
  console.log(`Language: ${args.lang} | Concurrency: ${args.concurrency} | Batch size: ${args.batchSize}`);
  if (args.force) console.log("Force mode: re-translating existing translations");
  if (args.dryRun) console.log("Dry run mode: no translations will be performed");

  const slugs = args.collectionSlug
    ? [args.collectionSlug]
    : Object.keys(COLLECTIONS);

  let grandTotal = 0;
  let grandFailed = 0;

  // Reset split counters and init miss log
  splitFound = 0;
  splitNotFound = 0;
  noSplitApplicable = 0;
  splitMissLogPath = `/tmp/split-misses-${args.lang}-${Date.now()}.tsv`;
  appendFileSync(splitMissLogPath, "bookId\thadithNumber\tmatnStart\ttextStart\n");
  console.log(`Split miss log: ${splitMissLogPath}`);

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
  console.log(`Arabic splits: ${splitFound} found, ${splitNotFound} not found, ${noSplitApplicable} not applicable`);
  await prisma.$disconnect();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  prisma.$disconnect();
  process.exit(1);
});
