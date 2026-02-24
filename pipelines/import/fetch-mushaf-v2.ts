/**
 * Fetch mushaf word layout from quran.com API v4 with correct QPC V2 glyph codes
 *
 * This is the authoritative source for V2 font glyph codes (code_v2),
 * text_uthmani, line numbers, and word positions for all 604 mushaf pages.
 *
 * Usage:
 *   bun run pipelines/import/fetch-mushaf-v2.ts [--pages=1-604] [--concurrency=3]
 */

import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";

const API_BASE = "https://api.quran.com/api/v4";
const OUTPUT_DIR = join(import.meta.dirname, "../../data/mushaf");
const OUTPUT_PATH = join(OUTPUT_DIR, "words-v2.json");
const TOTAL_PAGES = 604;
const MAX_RETRIES = 3;

interface WordV2 {
  pageNumber: number;
  lineNumber: number;
  lineType: string;
  positionInLine: number;
  charTypeName: string;
  surahNumber: number;
  ayahNumber: number;
  wordPosition: number;
  textUthmani: string;
  glyphCode: string;
}

function parsePageRange(): [number, number] {
  const arg = process.argv.find((a) => a.startsWith("--pages="));
  if (arg) {
    const [start, end] = arg.replace("--pages=", "").split("-").map(Number);
    return [start, end || start];
  }
  return [1, TOTAL_PAGES];
}

function getConcurrency(): number {
  const arg = process.argv.find((a) => a.startsWith("--concurrency="));
  return arg ? Number(arg.replace("--concurrency=", "")) : 3;
}

async function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchWithRetry(url: string, retries = MAX_RETRIES): Promise<Response> {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, {
        headers: { Accept: "application/json" },
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return res;
    } catch (err: any) {
      if (attempt === retries) throw err;
      const delay = attempt * 1000;
      console.warn(`    Retry ${attempt}/${retries} for ${url} (${err.message}), waiting ${delay}ms`);
      await sleep(delay);
    }
  }
  throw new Error("unreachable");
}

async function fetchPage(pageNum: number): Promise<WordV2[]> {
  const url = `${API_BASE}/verses/by_page/${pageNum}?words=true&word_fields=code_v2,line_number,position,char_type_name,text_uthmani&per_page=50&translations=0`;
  const res = await fetchWithRetry(url);
  const data = await res.json();

  const words: WordV2[] = [];

  for (const verse of data.verses) {
    const [surahStr, ayahStr] = verse.verse_key.split(":");
    const surahNumber = parseInt(surahStr);
    const ayahNumber = parseInt(ayahStr);

    for (const w of verse.words) {
      words.push({
        pageNumber: pageNum,
        lineNumber: w.line_number,
        lineType: "text",
        positionInLine: w.position,
        charTypeName: w.char_type_name,
        surahNumber,
        ayahNumber,
        wordPosition: w.position,
        textUthmani: w.text_uthmani || "",
        glyphCode: w.code_v2 || "",
      });
    }
  }

  return words;
}

function hasBismillah(surahNum: number): boolean {
  return surahNum !== 1 && surahNum !== 9;
}

async function main() {
  const [startPage, endPage] = parsePageRange();
  const concurrency = getConcurrency();

  console.log("Quran.com V2 Mushaf Layout Fetcher");
  console.log("===================================");
  console.log(`Pages: ${startPage}-${endPage}`);
  console.log(`Concurrency: ${concurrency}`);
  console.log(`Output: ${OUTPUT_PATH}`);
  console.log();

  mkdirSync(OUTPUT_DIR, { recursive: true });

  const allWords: WordV2[] = [];
  let fetched = 0;
  let errors = 0;

  for (let batch = startPage; batch <= endPage; batch += concurrency) {
    const batchEnd = Math.min(batch + concurrency - 1, endPage);
    const promises = [];

    for (let p = batch; p <= batchEnd; p++) {
      promises.push(
        fetchPage(p)
          .then((words) => {
            if (words.length === 0) {
              console.warn(`  Warning: No words found on page ${p}`);
              errors++;
            }
            return { page: p, words };
          })
          .catch((err) => {
            console.error(`  Error on page ${p}: ${err.message}`);
            errors++;
            return { page: p, words: [] as WordV2[] };
          })
      );
    }

    const results = await Promise.all(promises);
    for (const { words } of results) {
      allWords.push(...words);
    }

    fetched += batchEnd - batch + 1;
    if (fetched % 50 === 0 || batchEnd === endPage) {
      console.log(
        `  Progress: ${fetched}/${endPage - startPage + 1} pages, ${allWords.length} words`
      );
    }

    // Rate limit — quran.com has rate limits
    if (batchEnd < endPage) {
      await sleep(500);
    }
  }

  // Add surah_name and bismillah synthetic entries
  // Detect surah starts: first occurrence of ayah=1, wordPosition=1 per surah
  const surahFirstWord = new Map<number, { page: number; line: number }>();
  for (const w of allWords) {
    if (w.ayahNumber === 1 && w.wordPosition === 1 && w.charTypeName === "word") {
      if (!surahFirstWord.has(w.surahNumber)) {
        surahFirstWord.set(w.surahNumber, { page: w.pageNumber, line: w.lineNumber });
      }
    }
  }

  const syntheticWords: WordV2[] = [];
  for (const [surahNum, { page, line }] of surahFirstWord) {
    // Surah header line is before first text line
    const headerLine = line - (hasBismillah(surahNum) ? 2 : 1);
    if (headerLine >= 1) {
      syntheticWords.push({
        pageNumber: page,
        lineNumber: headerLine,
        lineType: "surah_name",
        positionInLine: 1,
        charTypeName: "surah_name",
        surahNumber: surahNum,
        ayahNumber: 0,
        wordPosition: 0,
        textUthmani: `surah${String(surahNum).padStart(3, "0")}`,
        glyphCode: `surah${String(surahNum).padStart(3, "0")}`,
      });
    }

    if (hasBismillah(surahNum) && line - 1 >= 1) {
      syntheticWords.push({
        pageNumber: page,
        lineNumber: line - 1,
        lineType: "bismillah",
        positionInLine: 1,
        charTypeName: "bismillah",
        surahNumber: surahNum,
        ayahNumber: 0,
        wordPosition: 0,
        textUthmani: "بِسْمِ ٱللَّهِ ٱلرَّحْمَـٰنِ ٱلرَّحِيمِ",
        glyphCode: "﷽",
      });
    }
  }

  allWords.push(...syntheticWords);
  allWords.sort(
    (a, b) =>
      a.pageNumber - b.pageNumber ||
      a.lineNumber - b.lineNumber ||
      a.positionInLine - b.positionInLine
  );

  console.log(`\nTotal words: ${allWords.length}`);
  console.log(`Synthetic entries: ${syntheticWords.length}`);
  console.log(`Surahs detected: ${surahFirstWord.size}`);
  console.log(`Errors: ${errors}`);

  const charTypes = new Map<string, number>();
  for (const w of allWords) {
    charTypes.set(w.charTypeName, (charTypes.get(w.charTypeName) || 0) + 1);
  }
  console.log("Char types:", Object.fromEntries(charTypes));

  const pages = new Set(allWords.map((w) => w.pageNumber));
  console.log(`Pages with data: ${pages.size}`);

  if (errors > 0) {
    console.error(`\nWARNING: ${errors} errors occurred. Data may be incomplete.`);
  }

  writeFileSync(OUTPUT_PATH, JSON.stringify(allWords));
  console.log(`\nSaved to ${OUTPUT_PATH}`);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
