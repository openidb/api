/**
 * Batch Book Translation Pipeline
 *
 * Translates entire books by stitching pages into token-budget chunks (~30K tokens),
 * sending each chunk to the LLM with full surrounding context, then mapping
 * translations back to individual PageTranslation records.
 *
 * Usage:
 *   bun run pipelines/translate/translate-book.ts --book=4 --lang=en
 *   bun run pipelines/translate/translate-book.ts --book=4,10,200 --lang=en
 *   bun run pipelines/translate/translate-book.ts --book=4 --lang=en --concurrency=5 --max-tokens=30000
 *   bun run pipelines/translate/translate-book.ts --book=4 --lang=en --force
 *   bun run pipelines/translate/translate-book.ts --book=4 --lang=en --dry-run
 *   bun run pipelines/translate/translate-book.ts --book=4 --lang=en --model=gemini-flash
 *   bun run pipelines/translate/translate-book.ts --book=4 --lang=en --start-page=50
 */

import "../env";
import { prisma } from "../../src/db";
import { callOpenRouter } from "../../src/lib/openrouter";
import { hashPageTranslation } from "../../src/utils/content-hash";
import { extractParagraphs } from "../../src/utils/paragraphs";

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

const MODEL_MAP: Record<string, string> = {
  "gemini-flash": "google/gemini-3-flash-preview",
  "gpt-oss-120b": "openai/gpt-oss-120b",
};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface PageData {
  id: number;
  pageNumber: number;
  contentHtml: string;
  hasHadith: boolean;
}

interface TaggedParagraph {
  pageNumber: number;
  originalIndex: number; // index within extractParagraphs() for this page
  text: string;
}

interface MergedParagraph {
  text: string;
  segments: Array<{
    pageNumber: number;
    originalIndex: number;
    charStart: number;
    charEnd: number;
  }>;
}

interface Chunk {
  index: number;
  pages: PageData[];
  estimatedTokens: number;
}

interface CLIArgs {
  bookIds: string[];
  lang: string;
  modelKey: string;
  concurrency: number;
  maxTokens: number;
  force: boolean;
  dryRun: boolean;
  startPage: number;
  delay: number;
}

// ---------------------------------------------------------------------------
// CLI argument parsing
// ---------------------------------------------------------------------------

function parseArgs(): CLIArgs {
  const args = process.argv.slice(2);
  let bookIds: string[] = [];
  let lang = "";
  let modelKey = "gemini-flash";
  let concurrency = 5;
  let maxTokens = 30000;
  let force = false;
  let dryRun = false;
  let startPage = 1;
  let delay = 200;

  for (const arg of args) {
    if (arg.startsWith("--book=")) {
      bookIds = arg.slice(7).split(",").map((s) => s.trim()).filter(Boolean);
    } else if (arg.startsWith("--lang=")) {
      lang = arg.slice(7);
    } else if (arg.startsWith("--model=")) {
      modelKey = arg.slice(8);
    } else if (arg.startsWith("--concurrency=")) {
      concurrency = parseInt(arg.slice(14), 10) || 5;
    } else if (arg.startsWith("--max-tokens=")) {
      maxTokens = parseInt(arg.slice(13), 10) || 30000;
    } else if (arg === "--force") {
      force = true;
    } else if (arg === "--dry-run") {
      dryRun = true;
    } else if (arg.startsWith("--start-page=")) {
      startPage = parseInt(arg.slice(13), 10) || 1;
    } else if (arg.startsWith("--delay=")) {
      delay = parseInt(arg.slice(8), 10) || 200;
    }
  }

  if (bookIds.length === 0) {
    console.error("Error: --book=<id> is required");
    process.exit(1);
  }
  if (!lang) {
    console.error("Error: --lang=<code> is required");
    process.exit(1);
  }
  if (!LANGUAGE_NAMES[lang]) {
    console.error(`Error: unsupported language "${lang}". Supported: ${Object.keys(LANGUAGE_NAMES).join(", ")}`);
    process.exit(1);
  }
  if (!MODEL_MAP[modelKey]) {
    console.error(`Error: unknown model "${modelKey}". Supported: ${Object.keys(MODEL_MAP).join(", ")}`);
    process.exit(1);
  }

  return { bookIds, lang, modelKey, concurrency, maxTokens, force, dryRun, startPage, delay };
}

// ---------------------------------------------------------------------------
// Token estimation
// ---------------------------------------------------------------------------

function estimateTokens(text: string): number {
  // ~3 chars per token for Arabic text
  return Math.ceil(text.length / 3);
}

// ---------------------------------------------------------------------------
// Chunking
// ---------------------------------------------------------------------------

function buildChunks(pages: PageData[], maxTokens: number): Chunk[] {
  const chunks: Chunk[] = [];
  let currentPages: PageData[] = [];
  let currentTokens = 0;

  for (const page of pages) {
    const pageTokens = estimateTokens(page.contentHtml);

    if (currentTokens + pageTokens > maxTokens && currentPages.length > 0) {
      chunks.push({ index: chunks.length, pages: currentPages, estimatedTokens: currentTokens });
      currentPages = [];
      currentTokens = 0;
    }

    currentPages.push(page);
    currentTokens += pageTokens;
  }

  if (currentPages.length > 0) {
    chunks.push({ index: chunks.length, pages: currentPages, estimatedTokens: currentTokens });
  }

  return chunks;
}

// ---------------------------------------------------------------------------
// Paragraph stitching
// ---------------------------------------------------------------------------

function shouldMerge(lastParagraph: string, firstParagraph: string): boolean {
  if (lastParagraph.includes('data-type="title"')) return false;
  if (firstParagraph.includes('data-type="title"')) return false;
  const sentenceEnders = /[.،؛:؟!\u06D4]\s*$/;
  if (sentenceEnders.test(lastParagraph)) return false;
  return true;
}

function buildMergedParagraphs(pages: PageData[]): MergedParagraph[] {
  // Extract and tag paragraphs per page
  const tagged: TaggedParagraph[] = [];
  for (const page of pages) {
    const paras = extractParagraphs(page.contentHtml);
    for (const p of paras) {
      tagged.push({ pageNumber: page.pageNumber, originalIndex: p.index, text: p.text });
    }
  }

  if (tagged.length === 0) return [];

  // Build merged paragraphs, joining cross-page continuations
  const merged: MergedParagraph[] = [];

  for (let i = 0; i < tagged.length; i++) {
    const t = tagged[i];
    const prev = i > 0 ? tagged[i - 1] : null;

    // Check if this paragraph should be merged with the previous one
    // (cross-page continuation: different page, no sentence-ending punctuation)
    if (
      prev &&
      prev.pageNumber !== t.pageNumber &&
      merged.length > 0 &&
      shouldMerge(prev.text, t.text)
    ) {
      // Merge into the last merged paragraph
      const last = merged[merged.length - 1];
      const charStart = last.text.length + 1; // +1 for the space
      last.text += " " + t.text;
      last.segments.push({
        pageNumber: t.pageNumber,
        originalIndex: t.originalIndex,
        charStart,
        charEnd: last.text.length,
      });
    } else {
      // New paragraph
      merged.push({
        text: t.text,
        segments: [{
          pageNumber: t.pageNumber,
          originalIndex: t.originalIndex,
          charStart: 0,
          charEnd: t.text.length,
        }],
      });
    }
  }

  return merged;
}

// ---------------------------------------------------------------------------
// Oversized page splitting
// ---------------------------------------------------------------------------

function splitOversizedPage(paragraphs: TaggedParagraph[], maxTokens: number): TaggedParagraph[][] {
  const subChunks: TaggedParagraph[][] = [];
  let current: TaggedParagraph[] = [];
  let currentTokens = 0;

  for (const p of paragraphs) {
    const pTokens = estimateTokens(p.text);

    // If a single paragraph exceeds maxTokens, truncate it
    if (pTokens > maxTokens && current.length === 0) {
      const maxChars = maxTokens * 3;
      console.warn(`  [warn] Paragraph on page ${p.pageNumber} index ${p.originalIndex} exceeds max tokens (${pTokens}), truncating to ${maxTokens}`);
      subChunks.push([{ ...p, text: p.text.slice(0, maxChars) }]);
      continue;
    }

    if (currentTokens + pTokens > maxTokens && current.length > 0) {
      subChunks.push(current);
      current = [];
      currentTokens = 0;
    }
    current.push(p);
    currentTokens += pTokens;
  }

  if (current.length > 0) subChunks.push(current);
  return subChunks;
}

// ---------------------------------------------------------------------------
// LLM translation
// ---------------------------------------------------------------------------

function cleanLLMResponse(content: string): string {
  let cleaned = content.trim();
  if (cleaned.startsWith("```json")) cleaned = cleaned.slice(7);
  else if (cleaned.startsWith("```")) cleaned = cleaned.slice(3);
  if (cleaned.endsWith("```")) cleaned = cleaned.slice(0, -3);
  return cleaned.trim();
}

async function translateChunkWithRetry(
  paragraphs: MergedParagraph[],
  bookTitle: string,
  authorName: string,
  targetLanguage: string,
  modelKey: string,
  maxRetries: number = 3,
): Promise<{ index: number; translation: string }[]> {
  const model = MODEL_MAP[modelKey];
  const numberedParagraphs = paragraphs.map((p, i) => `[${i}] ${p.text}`).join("\n\n");

  const prompt = `You are translating a section of the Arabic Islamic text "${bookTitle}" by ${authorName}.

Translate the following Arabic paragraphs to ${targetLanguage}.
Each paragraph is numbered [N]. Return a JSON array: [{"index": N, "translation": "..."}].

IMPORTANT — All paragraphs come from consecutive pages of the same book.
Use the full context to disambiguate pronouns, maintain consistent terminology,
and understand technical terms in context.

Preserve Islamic terminology in their conventional ${targetLanguage === "English" ? "English/transliterated" : targetLanguage} forms:
- Surah names: keep the standard transliteration (e.g. al-Baqarah, al-Qasas) — do NOT translate surah names into literal meanings
- "الله" → "Allah", "محمد" → "Muhammad" or "the Prophet Muhammad"
- "القرآن" → "Quran", "الرسول" → "the Messenger" or "the Prophet"
- "صلى الله عليه وسلم" → "peace be upon him" or "ﷺ"
- Keep: Salah, Zakat, Hajj, Iman, Taqwa, Sunnah, Hadith, Fiqh, Tafsir, Ijma, Qiyas, etc.

Arabic paragraphs:
${numberedParagraphs}

Respond with ONLY a valid JSON array.`;

  let lastError: Error | null = null;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const result = await callOpenRouter({
        model,
        messages: [{ role: "user", content: prompt }],
        temperature: 0.25,
        timeoutMs: 1800_000, // 30 minutes for large chunks
      });

      if (!result) {
        throw new Error("Translation service unavailable (no response)");
      }

      const cleaned = cleanLLMResponse(result.content);
      const parsed = JSON.parse(cleaned);

      if (!Array.isArray(parsed)) {
        throw new Error("LLM response is not an array");
      }

      const translations: { index: number; translation: string }[] = [];
      for (const item of parsed) {
        if (
          typeof item?.index === "number" && Number.isFinite(item.index) &&
          typeof item?.translation === "string"
        ) {
          translations.push({ index: item.index, translation: item.translation.slice(0, 5000) });
        }
      }

      if (translations.length === 0) {
        throw new Error("No valid translations parsed from LLM response");
      }

      return translations;
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));

      // Check for rate limit (429) — exponential backoff
      const isRateLimit = lastError.message.includes("429") || lastError.message.includes("rate");
      const backoff = isRateLimit ? 2000 * Math.pow(2, attempt) : 1000 * (attempt + 1);

      if (attempt < maxRetries - 1) {
        console.warn(`  [retry ${attempt + 1}/${maxRetries}] ${lastError.message}, waiting ${backoff}ms...`);
        await sleep(backoff);
      }
    }
  }

  throw lastError || new Error("Translation failed after retries");
}

// ---------------------------------------------------------------------------
// Map translations back to pages
// ---------------------------------------------------------------------------

function mapTranslationsToPages(
  mergedParagraphs: MergedParagraph[],
  translations: { index: number; translation: string }[],
): Map<number, { index: number; translation: string }[]> {
  const pageMap = new Map<number, { index: number; translation: string }[]>();

  for (const t of translations) {
    const mp = mergedParagraphs[t.index];
    if (!mp) continue;

    if (mp.segments.length === 1) {
      // Single-page paragraph — assign to that page
      const seg = mp.segments[0];
      if (!pageMap.has(seg.pageNumber)) pageMap.set(seg.pageNumber, []);
      pageMap.get(seg.pageNumber)!.push({ index: seg.originalIndex, translation: t.translation });
    } else {
      // Cross-page paragraph — full translation on starting page
      const firstSeg = mp.segments[0];
      if (!pageMap.has(firstSeg.pageNumber)) pageMap.set(firstSeg.pageNumber, []);
      pageMap.get(firstSeg.pageNumber)!.push({ index: firstSeg.originalIndex, translation: t.translation });

      // Proportional word-split for continuation pages
      const words = t.translation.split(/\s+/);
      const totalChars = mp.text.length;

      for (let s = 1; s < mp.segments.length; s++) {
        const seg = mp.segments[s];
        const segChars = seg.charEnd - seg.charStart;
        const proportion = segChars / totalChars;
        const startWord = Math.round(words.length * (seg.charStart / totalChars));
        const wordCount = Math.max(1, Math.round(words.length * proportion));
        const segTranslation = words.slice(startWord, startWord + wordCount).join(" ");

        if (!pageMap.has(seg.pageNumber)) pageMap.set(seg.pageNumber, []);
        pageMap.get(seg.pageNumber)!.push({ index: seg.originalIndex, translation: segTranslation });
      }
    }
  }

  return pageMap;
}

// ---------------------------------------------------------------------------
// DB save
// ---------------------------------------------------------------------------

async function savePageTranslations(
  pageMap: Map<number, { index: number; translation: string }[]>,
  pages: PageData[],
  bookId: string,
  lang: string,
  modelKey: string,
): Promise<number> {
  let saved = 0;

  for (const page of pages) {
    const paragraphs = pageMap.get(page.pageNumber);
    if (!paragraphs || paragraphs.length === 0) continue;

    // Sort by index
    paragraphs.sort((a, b) => a.index - b.index);

    const contentHash = hashPageTranslation(bookId, page.pageNumber, lang, paragraphs);

    await prisma.pageTranslation.upsert({
      where: { pageId_language: { pageId: page.id, language: lang } },
      update: { model: modelKey, paragraphs, contentHash },
      create: { pageId: page.id, language: lang, model: modelKey, paragraphs, contentHash },
    });

    saved++;
  }

  return saved;
}

// ---------------------------------------------------------------------------
// Chunk processor
// ---------------------------------------------------------------------------

async function processChunk(
  chunk: Chunk,
  bookId: string,
  bookTitle: string,
  authorName: string,
  lang: string,
  modelKey: string,
  maxTokens: number,
): Promise<{ saved: number; failed: boolean }> {
  const pageRange = `${chunk.pages[0].pageNumber}-${chunk.pages[chunk.pages.length - 1].pageNumber}`;

  // Check for oversized pages
  const oversizedPages: PageData[] = [];
  const normalPages: PageData[] = [];

  for (const page of chunk.pages) {
    if (estimateTokens(page.contentHtml) > maxTokens) {
      oversizedPages.push(page);
    } else {
      normalPages.push(page);
    }
  }

  let totalSaved = 0;

  // Handle oversized pages separately
  for (const page of oversizedPages) {
    const paras = extractParagraphs(page.contentHtml);
    const tagged: TaggedParagraph[] = paras.map((p) => ({
      pageNumber: page.pageNumber,
      originalIndex: p.index,
      text: p.text,
    }));

    const subChunks = splitOversizedPage(tagged, maxTokens);
    console.log(`  [chunk ${chunk.index}] Page ${page.pageNumber} oversized, split into ${subChunks.length} sub-chunks`);

    const allTranslations: { index: number; translation: string }[] = [];

    for (let i = 0; i < subChunks.length; i++) {
      const subMerged: MergedParagraph[] = subChunks[i].map((t) => ({
        text: t.text,
        segments: [{ pageNumber: t.pageNumber, originalIndex: t.originalIndex, charStart: 0, charEnd: t.text.length }],
      }));

      const subTranslations = await translateChunkWithRetry(
        subMerged, bookTitle, authorName, LANGUAGE_NAMES[lang], modelKey,
      );

      // Remap sub-chunk indices to original indices
      for (const st of subTranslations) {
        const originalPara = subChunks[i][st.index];
        if (originalPara) {
          allTranslations.push({ index: originalPara.originalIndex, translation: st.translation });
        }
      }
    }

    if (allTranslations.length > 0) {
      allTranslations.sort((a, b) => a.index - b.index);
      const contentHash = hashPageTranslation(bookId, page.pageNumber, lang, allTranslations);
      await prisma.pageTranslation.upsert({
        where: { pageId_language: { pageId: page.id, language: lang } },
        update: { model: modelKey, paragraphs: allTranslations, contentHash },
        create: { pageId: page.id, language: lang, model: modelKey, paragraphs: allTranslations, contentHash },
      });
      totalSaved++;
    }
  }

  // Process normal pages as a group with paragraph stitching
  if (normalPages.length > 0) {
    const mergedParagraphs = buildMergedParagraphs(normalPages);

    if (mergedParagraphs.length === 0) {
      console.log(`  [chunk ${chunk.index}] pages ${pageRange}: no translatable content`);
      return { saved: totalSaved, failed: false };
    }

    const translations = await translateChunkWithRetry(
      mergedParagraphs, bookTitle, authorName, LANGUAGE_NAMES[lang], modelKey,
    );

    const pageMap = mapTranslationsToPages(mergedParagraphs, translations);
    const saved = await savePageTranslations(pageMap, normalPages, bookId, lang, modelKey);
    totalSaved += saved;
  }

  return { saved: totalSaved, failed: false };
}

// ---------------------------------------------------------------------------
// Worker pool
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function processChunkQueue(
  chunks: Chunk[],
  bookId: string,
  bookTitle: string,
  authorName: string,
  lang: string,
  modelKey: string,
  maxTokens: number,
  concurrency: number,
  delay: number,
): Promise<{ totalSaved: number; failedChunks: number[] }> {
  let nextIndex = 0;
  let totalSaved = 0;
  const failedChunks: number[] = [];

  async function worker() {
    while (nextIndex < chunks.length) {
      const idx = nextIndex++;
      const chunk = chunks[idx];
      const pageRange = `${chunk.pages[0].pageNumber}-${chunk.pages[chunk.pages.length - 1].pageNumber}`;

      try {
        console.log(`  [chunk ${chunk.index}] pages ${pageRange} (${chunk.pages.length} pages, ~${chunk.estimatedTokens} tokens)`);
        const result = await processChunk(chunk, bookId, bookTitle, authorName, lang, modelKey, maxTokens);
        totalSaved += result.saved;
        console.log(`  [chunk ${chunk.index}] done — ${result.saved} pages saved`);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        console.error(`  [chunk ${chunk.index}] FAILED pages ${pageRange}: ${msg}`);
        failedChunks.push(chunk.index);
      }

      if (delay > 0 && nextIndex < chunks.length) {
        await sleep(delay);
      }
    }
  }

  const workers = Array.from({ length: Math.min(concurrency, chunks.length) }, () => worker());
  await Promise.all(workers);

  return { totalSaved, failedChunks };
}

// ---------------------------------------------------------------------------
// Main: translate one book
// ---------------------------------------------------------------------------

async function translateBook(bookId: string, args: CLIArgs): Promise<void> {
  const { lang, modelKey, concurrency, maxTokens, force, dryRun, startPage, delay } = args;

  // Fetch book metadata
  const book = await prisma.book.findUnique({
    where: { id: bookId },
    select: {
      id: true,
      titleArabic: true,
      titleLatin: true,
      totalPages: true,
      author: { select: { nameArabic: true, nameLatin: true } },
    },
  });

  if (!book) {
    console.error(`Book ${bookId} not found`);
    return;
  }

  const bookTitle = book.titleLatin || book.titleArabic || `Book ${bookId}`;
  const authorName = book.author?.nameLatin || book.author?.nameArabic || "Unknown";

  console.log(`\n${"=".repeat(70)}`);
  console.log(`Book ${bookId}: ${bookTitle}`);
  console.log(`Author: ${authorName} | Language: ${lang} | Model: ${modelKey}`);
  console.log(`${"=".repeat(70)}`);

  // Fetch all pages (skip page 0 — auto-generated TOC)
  const allPages = await prisma.page.findMany({
    where: {
      bookId,
      pageNumber: { gte: startPage },
    },
    orderBy: { pageNumber: "asc" },
    select: {
      id: true,
      pageNumber: true,
      contentHtml: true,
      hasHadith: true,
    },
  });

  if (allPages.length === 0) {
    console.log("  No pages to translate");
    return;
  }

  // Filter out already-translated pages (unless --force)
  let pages: PageData[];
  if (force) {
    pages = allPages;
  } else {
    const existingTranslations = await prisma.pageTranslation.findMany({
      where: {
        pageId: { in: allPages.map((p) => p.id) },
        language: lang,
      },
      select: { pageId: true },
    });
    const translatedPageIds = new Set(existingTranslations.map((t) => t.pageId));
    pages = allPages.filter((p) => !translatedPageIds.has(p.id));

    if (pages.length < allPages.length) {
      console.log(`  Skipping ${allPages.length - pages.length} already-translated pages (use --force to re-translate)`);
    }
  }

  if (pages.length === 0) {
    console.log("  All pages already translated");
    return;
  }

  console.log(`  ${pages.length} pages to translate (pages ${pages[0].pageNumber}-${pages[pages.length - 1].pageNumber})`);

  // Build chunks
  const chunks = buildChunks(pages, maxTokens);
  console.log(`  ${chunks.length} chunks (max ~${maxTokens} tokens each)`);

  if (dryRun) {
    console.log("\n  [DRY RUN] Chunk plan:");
    for (const chunk of chunks) {
      const pageRange = `${chunk.pages[0].pageNumber}-${chunk.pages[chunk.pages.length - 1].pageNumber}`;
      const mergedCount = buildMergedParagraphs(chunk.pages).length;
      console.log(`    chunk ${chunk.index}: pages ${pageRange} (${chunk.pages.length} pages, ~${chunk.estimatedTokens} tokens, ${mergedCount} paragraphs)`);
    }
    return;
  }

  // Process chunks
  const startTime = Date.now();
  const { totalSaved, failedChunks } = await processChunkQueue(
    chunks, bookId, bookTitle, authorName, lang, modelKey, maxTokens, concurrency, delay,
  );
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log(`\n  Summary: ${totalSaved} pages saved in ${elapsed}s`);
  if (failedChunks.length > 0) {
    console.log(`  Failed chunks: ${failedChunks.join(", ")}`);
    // Find the first page of the first failed chunk for resume hint
    const firstFailedChunk = chunks[failedChunks[0]];
    if (firstFailedChunk) {
      console.log(`  Resume with: --start-page=${firstFailedChunk.pages[0].pageNumber}`);
    }
  }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

async function main() {
  const args = parseArgs();

  console.log("Batch Book Translation Pipeline");
  console.log(`Books: ${args.bookIds.join(", ")} | Lang: ${args.lang} | Model: ${args.modelKey}`);
  console.log(`Concurrency: ${args.concurrency} | Max tokens: ${args.maxTokens} | Delay: ${args.delay}ms`);
  if (args.force) console.log("Force mode: re-translating existing translations");
  if (args.dryRun) console.log("Dry run mode: no translations will be performed");
  if (args.startPage > 1) console.log(`Starting from page ${args.startPage}`);

  for (const bookId of args.bookIds) {
    await translateBook(bookId, args);
  }

  console.log("\nDone.");
  await prisma.$disconnect();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  prisma.$disconnect();
  process.exit(1);
});
