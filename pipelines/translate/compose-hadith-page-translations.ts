/**
 * Compose PageTranslation entries for hadith source books.
 *
 * Maps existing hadith translations (from HadithTranslation) back to their
 * source book pages, matching paragraph text to hadith fields. Unmatched
 * paragraphs (editorial content, headings) are batch-translated via LLM.
 *
 * Usage:
 *   bun run pipelines/translate/compose-hadith-page-translations.ts --lang=en
 *   bun run pipelines/translate/compose-hadith-page-translations.ts --lang=en --book=12836
 *   bun run pipelines/translate/compose-hadith-page-translations.ts --lang=en --dry-run
 *   bun run pipelines/translate/compose-hadith-page-translations.ts --lang=en --force
 */

import "../env";
import { prisma } from "../../src/db";
import { callOpenRouter } from "../../src/lib/openrouter";
import { hashPageTranslation } from "../../src/utils/content-hash";
import { extractParagraphs, stripHtmlEntities } from "../../src/utils/paragraphs";
import { normalizeArabic } from "../../src/utils/arabic-text";
import { COLLECTIONS, type CollectionConfig } from "../import/turath-hadith-configs";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const LANGUAGE_NAMES: Record<string, string> = {
  en: "English", fr: "French", id: "Indonesian", ur: "Urdu",
  es: "Spanish", zh: "Chinese", pt: "Portuguese", ru: "Russian",
  ja: "Japanese", ko: "Korean", it: "Italian", bn: "Bengali",
};

const MODEL = "google/gemini-3-flash-preview";
const MODEL_KEY = "gemini-flash";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface CLIArgs {
  lang: string;
  bookId: string | null;
  concurrency: number;
  force: boolean;
  dryRun: boolean;
}

interface HadithWithTranslation {
  bookId: number;
  hadithNumber: string;
  isnad: string | null;
  matn: string | null;
  footnotes: string | null;
  kitabArabic: string | null;
  chapterArabic: string | null;
  gradeExplanation: string | null;
  sourcePageStart: number | null;
  sourcePageEnd: number | null;
  textArabic: string;
  // Translation fields
  isnadTranslation: string | null;
  matnTranslation: string | null;
  footnotesTranslation: string | null;
  kitabTranslation: string | null;
  chapterTranslation: string | null;
  gradeExplanationTranslation: string | null;
}

interface PageRow {
  id: number;
  pageNumber: number;
  contentHtml: string;
}

interface ParagraphTranslation {
  index: number;
  translation: string;
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function parseArgs(): CLIArgs {
  const args = process.argv.slice(2);
  let lang = "";
  let bookId: string | null = null;
  let concurrency = 5;
  let force = false;
  let dryRun = false;

  for (const arg of args) {
    if (arg.startsWith("--lang=")) lang = arg.slice(7);
    else if (arg.startsWith("--book=")) bookId = arg.slice(7);
    else if (arg.startsWith("--concurrency=")) concurrency = parseInt(arg.slice(14), 10) || 5;
    else if (arg === "--force") force = true;
    else if (arg === "--dry-run") dryRun = true;
  }

  if (!lang) { console.error("Error: --lang=<code> is required"); process.exit(1); }
  if (!LANGUAGE_NAMES[lang]) {
    console.error(`Error: unsupported language "${lang}". Supported: ${Object.keys(LANGUAGE_NAMES).join(", ")}`);
    process.exit(1);
  }

  return { lang, bookId, concurrency, force, dryRun };
}

// ---------------------------------------------------------------------------
// Arabic text matching
// ---------------------------------------------------------------------------

function normalizeForMatch(text: string): string {
  return normalizeArabic(text).replace(/\s+/g, " ").trim();
}

/**
 * Check if `needle` is a substring of `haystack` after normalization.
 * Returns true if >=60% of the needle's normalized words appear in the haystack.
 */
function fuzzyContains(haystack: string, needle: string): boolean {
  if (!needle || !haystack) return false;
  const normH = normalizeForMatch(haystack);
  const normN = normalizeForMatch(needle);
  if (normH.includes(normN)) return true;

  // Fallback: word overlap — if the needle is long, check if most words match
  const needleWords = normN.split(" ").filter((w) => w.length > 2);
  if (needleWords.length < 3) return false;
  const matched = needleWords.filter((w) => normH.includes(w)).length;
  return matched / needleWords.length >= 0.6;
}

// ---------------------------------------------------------------------------
// Match paragraphs to hadith fields
// ---------------------------------------------------------------------------

interface MatchResult {
  index: number;
  translation: string | null; // null = unmatched, needs LLM
  source: "hadith" | "unmatched";
}

function matchParagraphs(
  paragraphs: { index: number; text: string }[],
  hadiths: HadithWithTranslation[],
): MatchResult[] {
  const results: MatchResult[] = [];

  for (const para of paragraphs) {
    let matched = false;

    for (const h of hadiths) {
      // Try matching each hadith field
      const fieldPairs: [string | null, string | null][] = [
        [h.isnad, h.isnadTranslation],
        [h.matn, h.matnTranslation],
        [h.footnotes, h.footnotesTranslation],
        [h.kitabArabic, h.kitabTranslation],
        [h.chapterArabic, h.chapterTranslation],
        [h.gradeExplanation, h.gradeExplanationTranslation],
      ];

      for (const [arabicField, translationField] of fieldPairs) {
        if (!arabicField || !translationField) continue;
        if (fuzzyContains(para.text, arabicField) || fuzzyContains(arabicField, para.text)) {
          results.push({ index: para.index, translation: translationField, source: "hadith" });
          matched = true;
          break;
        }
      }

      if (matched) break;

      // Try full textArabic as fallback
      if (fuzzyContains(para.text, h.textArabic) || fuzzyContains(h.textArabic, para.text)) {
        const composed = [h.isnadTranslation, h.matnTranslation].filter(Boolean).join(" ");
        if (composed) {
          results.push({ index: para.index, translation: composed, source: "hadith" });
          matched = true;
          break;
        }
      }
    }

    if (!matched) {
      results.push({ index: para.index, translation: null, source: "unmatched" });
    }
  }

  return results;
}

// ---------------------------------------------------------------------------
// LLM fallback for unmatched paragraphs
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function cleanLLMResponse(content: string): string {
  let cleaned = content.trim();
  if (cleaned.startsWith("```json")) cleaned = cleaned.slice(7);
  else if (cleaned.startsWith("```")) cleaned = cleaned.slice(3);
  if (cleaned.endsWith("```")) cleaned = cleaned.slice(0, -3);
  return cleaned.trim();
}

async function translateUnmatchedParagraphs(
  paragraphs: { index: number; text: string }[],
  bookTitle: string,
  languageName: string,
): Promise<Map<number, string>> {
  if (paragraphs.length === 0) return new Map();

  const numbered = paragraphs.map((p, i) => `[${i}] ${p.text}`).join("\n\n");
  const prompt = `You are translating editorial content from the Arabic hadith book "${bookTitle}".
These are headings, editorial notes, or commentary — NOT hadith text.

Translate to ${languageName}. Return a JSON array: [{"index": N, "translation": "..."}].
Keep Islamic terminology in standard transliterated forms.

${numbered}

Respond with ONLY a valid JSON array.`;

  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const result = await callOpenRouter({
        model: MODEL,
        messages: [{ role: "user", content: prompt }],
        temperature: 0.25,
        timeoutMs: 60_000,
      });

      if (!result) throw new Error("No response");

      const cleaned = cleanLLMResponse(result.content);
      const parsed = JSON.parse(cleaned);
      if (!Array.isArray(parsed)) throw new Error("Not an array");

      const map = new Map<number, string>();
      for (const item of parsed) {
        if (typeof item?.index === "number" && typeof item?.translation === "string") {
          const originalPara = paragraphs[item.index];
          if (originalPara) {
            map.set(originalPara.index, item.translation);
          }
        }
      }
      return map;
    } catch (err) {
      if (attempt < 2) {
        await sleep(1000 * (attempt + 1));
      }
    }
  }

  return new Map(); // give up, leave unmatched paragraphs without translation
}

// ---------------------------------------------------------------------------
// Process one book
// ---------------------------------------------------------------------------

async function processBook(
  config: CollectionConfig,
  args: CLIArgs,
): Promise<{ saved: number; skipped: number }> {
  const { lang, force, dryRun, concurrency } = args;
  const languageName = LANGUAGE_NAMES[lang];
  const bookId = config.bookId;

  // 1. Get collection + book IDs for hadiths
  const collection = await prisma.hadithCollection.findUnique({
    where: { slug: config.slug },
    select: { id: true, books: { select: { id: true } } },
  });

  if (!collection) {
    console.error(`  Collection "${config.slug}" not found`);
    return { saved: 0, skipped: 0 };
  }

  const hadithBookIds = collection.books.map((b) => b.id);

  // 2. Fetch hadiths with translations
  const hadiths = await prisma.hadith.findMany({
    where: { bookId: { in: hadithBookIds } },
    select: {
      bookId: true,
      hadithNumber: true,
      isnad: true,
      matn: true,
      footnotes: true,
      kitabArabic: true,
      chapterArabic: true,
      gradeExplanation: true,
      sourcePageStart: true,
      sourcePageEnd: true,
      textArabic: true,
    },
  });

  const translations = await prisma.hadithTranslation.findMany({
    where: {
      bookId: { in: hadithBookIds },
      language: lang,
    },
    select: {
      bookId: true,
      hadithNumber: true,
      isnadTranslation: true,
      matnTranslation: true,
      footnotesTranslation: true,
      kitabTranslation: true,
      chapterTranslation: true,
      gradeExplanationTranslation: true,
    },
  });

  const transMap = new Map(
    translations.map((t) => [`${t.bookId}-${t.hadithNumber}`, t])
  );

  // Merge hadiths with their translations
  const hadithsWithTrans: HadithWithTranslation[] = hadiths
    .filter((h) => transMap.has(`${h.bookId}-${h.hadithNumber}`))
    .map((h) => {
      const t = transMap.get(`${h.bookId}-${h.hadithNumber}`)!;
      return { ...h, ...t };
    });

  if (hadithsWithTrans.length === 0) {
    console.log(`  No translated hadiths found for ${config.slug} — run translate-hadiths.ts first`);
    return { saved: 0, skipped: 0 };
  }

  console.log(`  ${hadithsWithTrans.length} hadiths with translations (of ${hadiths.length} total)`);

  // 3. Fetch pages for this source book
  const allPages: PageRow[] = await prisma.page.findMany({
    where: { bookId, pageNumber: { gte: 1 } },
    orderBy: { pageNumber: "asc" },
    select: { id: true, pageNumber: true, contentHtml: true },
  });

  if (allPages.length === 0) {
    console.log(`  No pages found for book ${bookId}`);
    return { saved: 0, skipped: 0 };
  }

  // Filter out already-translated pages (unless --force)
  let pages: PageRow[];
  if (force) {
    pages = allPages;
  } else {
    const existing = await prisma.pageTranslation.findMany({
      where: {
        pageId: { in: allPages.map((p) => p.id) },
        language: lang,
      },
      select: { pageId: true },
    });
    const translatedIds = new Set(existing.map((t) => t.pageId));
    pages = allPages.filter((p) => !translatedIds.has(p.id));

    if (pages.length < allPages.length) {
      console.log(`  Skipping ${allPages.length - pages.length} already-translated pages`);
    }
  }

  if (pages.length === 0) {
    console.log(`  All ${allPages.length} pages already translated`);
    return { saved: 0, skipped: allPages.length };
  }

  console.log(`  ${pages.length} pages to compose (book ${bookId}, ${allPages.length} total)`);

  if (dryRun) {
    console.log(`  [DRY RUN] Would compose ${pages.length} page translations`);
    return { saved: 0, skipped: 0 };
  }

  // 4. Group hadiths by page
  const hadithsByPage = new Map<number, HadithWithTranslation[]>();
  for (const h of hadithsWithTrans) {
    if (h.sourcePageStart == null) continue;
    const end = h.sourcePageEnd ?? h.sourcePageStart;
    for (let p = h.sourcePageStart; p <= end; p++) {
      if (!hadithsByPage.has(p)) hadithsByPage.set(p, []);
      hadithsByPage.get(p)!.push(h);
    }
  }

  // 5. Process pages
  const bookMeta = await prisma.book.findUnique({
    where: { id: bookId },
    select: { titleLatin: true, titleArabic: true },
  });
  const bookTitle = bookMeta?.titleLatin || bookMeta?.titleArabic || config.name;

  let saved = 0;
  let batchIdx = 0;

  // Process in parallel batches
  for (let i = 0; i < pages.length; i += concurrency) {
    const batch = pages.slice(i, i + concurrency);

    await Promise.all(batch.map(async (page) => {
      const paragraphs = extractParagraphs(page.contentHtml);
      if (paragraphs.length === 0) return;

      const pageHadiths = hadithsByPage.get(page.pageNumber) || [];
      const matches = matchParagraphs(paragraphs, pageHadiths);

      // Collect unmatched paragraphs for LLM translation
      const unmatched = matches
        .filter((m) => m.translation === null)
        .map((m) => {
          const para = paragraphs.find((p) => p.index === m.index);
          return para ? { index: m.index, text: para.text } : null;
        })
        .filter((p): p is { index: number; text: string } => p !== null);

      let unmatchedTranslations = new Map<number, string>();
      if (unmatched.length > 0) {
        unmatchedTranslations = await translateUnmatchedParagraphs(unmatched, bookTitle, languageName);
      }

      // Build final paragraph translations
      const paraTranslations: ParagraphTranslation[] = [];
      for (const m of matches) {
        const translation = m.translation || unmatchedTranslations.get(m.index);
        if (translation) {
          paraTranslations.push({ index: m.index, translation });
        }
      }

      if (paraTranslations.length === 0) return;

      paraTranslations.sort((a, b) => a.index - b.index);
      const contentHash = hashPageTranslation(bookId, page.pageNumber, lang, paraTranslations);

      try {
        await prisma.pageTranslation.upsert({
          where: { pageId_language: { pageId: page.id, language: lang } },
          update: { model: MODEL_KEY, paragraphs: paraTranslations, contentHash },
          create: { pageId: page.id, language: lang, model: MODEL_KEY, paragraphs: paraTranslations, contentHash },
        });
        saved++;
      } catch (err) {
        console.error(`    Failed to save page ${page.pageNumber}:`, err);
      }
    }));

    batchIdx++;
    if (batchIdx % 5 === 0 || i + concurrency >= pages.length) {
      console.log(`  [${Math.min(i + concurrency, pages.length)}/${pages.length}] ${saved} pages saved`);
    }
  }

  return { saved, skipped: allPages.length - pages.length };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const args = parseArgs();

  console.log("Hadith Page Translation Composer");
  console.log(`Language: ${args.lang} | Concurrency: ${args.concurrency}`);
  if (args.force) console.log("Force mode: re-composing existing translations");
  if (args.dryRun) console.log("Dry run mode: no translations will be saved");

  // Determine which books to process
  let configs: CollectionConfig[];
  if (args.bookId) {
    const match = Object.values(COLLECTIONS).find((c) => c.bookId === args.bookId);
    if (!match) {
      console.error(`No collection found with bookId=${args.bookId}`);
      console.error(`Available: ${Object.values(COLLECTIONS).map((c) => `${c.slug}=${c.bookId}`).join(", ")}`);
      process.exit(1);
    }
    configs = [match];
  } else {
    configs = Object.values(COLLECTIONS);
  }

  let grandSaved = 0;

  for (const config of configs) {
    console.log(`\n${"=".repeat(60)}`);
    console.log(`${config.name} (book ${config.bookId})`);
    console.log(`${"=".repeat(60)}`);

    const startTime = Date.now();
    const { saved, skipped } = await processBook(config, args);
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

    console.log(`  Done in ${elapsed}s — ${saved} pages saved, ${skipped} skipped`);
    grandSaved += saved;
  }

  console.log(`\nTotal: ${grandSaved} page translations saved`);
  await prisma.$disconnect();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  prisma.$disconnect();
  process.exit(1);
});
