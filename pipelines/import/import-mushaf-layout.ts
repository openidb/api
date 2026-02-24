/**
 * Import mushaf word layout data into Postgres
 *
 * Merges two data sources:
 * - words-v2.json (quran.com API): correct V2 glyph codes for word/end entries
 * - words-qul.json (QUL scrape): correct surah_name + bismillah line positions
 *
 * Usage:
 *   bun run pipelines/import/import-mushaf-layout.ts [--force]
 */

import "../env";
import { prisma } from "../../src/db";
import { readFileSync } from "fs";
import { join } from "path";

const V2_PATH = join(import.meta.dirname, "../../data/mushaf/words-v2.json");
const QUL_PATH = join(import.meta.dirname, "../../data/mushaf/words-qul.json");

interface MushafWordData {
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

async function main() {
  const force = process.argv.includes("--force");

  console.log("Mushaf Layout Import (V2 glyphs + QUL layout)");
  console.log("===============================================");

  const existing = await prisma.mushafWord.count();
  if (existing > 0 && !force) {
    console.log(`Already have ${existing} mushaf words. Use --force to reimport.`);
    await prisma.$disconnect();
    return;
  }

  if (existing > 0 && force) {
    console.log(`Deleting ${existing} existing mushaf words...`);
    await prisma.mushafWord.deleteMany();
  }

  // Load V2 word data (word + end entries with correct glyph codes)
  console.log(`Reading V2 data from ${V2_PATH}...`);
  const v2Words: MushafWordData[] = JSON.parse(readFileSync(V2_PATH, "utf-8"));
  const textWords = v2Words.filter((w) => w.charTypeName === "word" || w.charTypeName === "end");
  console.log(`  V2 text words: ${textWords.length}`);

  // Load QUL data (surah_name + bismillah entries with correct line positions)
  console.log(`Reading QUL data from ${QUL_PATH}...`);
  const qulWords: MushafWordData[] = JSON.parse(readFileSync(QUL_PATH, "utf-8"));
  const syntheticWords = qulWords.filter(
    (w) => w.charTypeName === "surah_name" || w.charTypeName === "bismillah"
  );
  console.log(`  QUL surah headers: ${syntheticWords.filter((w) => w.charTypeName === "surah_name").length}`);
  console.log(`  QUL bismillah: ${syntheticWords.filter((w) => w.charTypeName === "bismillah").length}`);

  // Merge: text words from V2 + synthetic from QUL
  const allWords = [...textWords, ...syntheticWords];

  // Sort by page, line, then Quran order (surah, ayah, word position)
  // NOTE: positionInLine from V2 data is per-ayah (resets at each ayah),
  // so we MUST sort by surah/ayah/wordPosition to avoid interleaving
  // words from different ayahs that share the same line.
  allWords.sort(
    (a, b) =>
      a.pageNumber - b.pageNumber ||
      a.lineNumber - b.lineNumber ||
      a.surahNumber - b.surahNumber ||
      a.ayahNumber - b.ayahNumber ||
      a.wordPosition - b.wordPosition
  );

  // Renumber positionInLine sequentially per page/line
  let prevPage = -1;
  let prevLine = -1;
  let seq = 0;

  for (const w of allWords) {
    if (w.pageNumber !== prevPage || w.lineNumber !== prevLine) {
      seq = 0;
      prevPage = w.pageNumber;
      prevLine = w.lineNumber;
    }
    seq++;
    w.positionInLine = seq;
  }

  console.log(`\nTotal words to import: ${allWords.length}`);

  // Batch insert
  const BATCH_SIZE = 5000;
  let imported = 0;

  for (let i = 0; i < allWords.length; i += BATCH_SIZE) {
    const batch = allWords.slice(i, i + BATCH_SIZE);
    const result = await prisma.mushafWord.createMany({
      data: batch.map((w) => ({
        pageNumber: w.pageNumber,
        lineNumber: w.lineNumber,
        lineType: w.lineType || w.charTypeName,
        positionInLine: w.positionInLine,
        charTypeName: w.charTypeName,
        surahNumber: w.surahNumber,
        ayahNumber: w.ayahNumber,
        wordPosition: w.wordPosition,
        textUthmani: w.textUthmani,
        glyphCode: w.glyphCode,
      })),
      skipDuplicates: true,
    });
    imported += result.count;

    if ((i + BATCH_SIZE) % 20000 === 0 || i + BATCH_SIZE >= allWords.length) {
      console.log(`  Progress: ${Math.min(i + BATCH_SIZE, allWords.length)}/${allWords.length} (${imported} inserted)`);
    }
  }

  // Verify
  const total = await prisma.mushafWord.count();
  const pageCount = await prisma.mushafWord.groupBy({
    by: ["pageNumber"],
    _count: true,
  });

  console.log(`\nImported ${imported} mushaf words`);
  console.log(`Total in DB: ${total}`);
  console.log(`Pages covered: ${pageCount.length}`);

  const lineTypeStats = await prisma.mushafWord.groupBy({
    by: ["lineType"],
    _count: true,
  });
  console.log("Line types:", lineTypeStats.map((s) => `${s.lineType}: ${s._count}`).join(", "));

  console.log("Done!");
  await prisma.$disconnect();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
