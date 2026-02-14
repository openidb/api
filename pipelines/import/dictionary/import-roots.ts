/**
 * Import Arabic Word→Root Mappings from Arramooz + Dictionary Headwords
 *
 * Reads the Arramooz SQLite database (nouns + verbs tables) and imports
 * (word, root) pairs with rich metadata into the arabic_roots table.
 * Also mines dictionary headwords as identity root mappings.
 *
 * Data source: https://github.com/linuxscout/arramooz
 *
 * Usage:
 *   bun run pipelines/import/dictionary/import-roots.ts [--dry-run] [--skip-dictionary]
 */

import "../../env";
import { Database } from "bun:sqlite";
import { prisma } from "../../../src/db";
import { normalizeArabic } from "../../../src/utils/arabic-text";
import { resolve } from "path";

const SQLITE_PATH = resolve(import.meta.dir, "data/arramooz.sqlite");
const BATCH_SIZE = 1000;

function parseArgs(): { dryRun: boolean; skipDictionary: boolean } {
  return {
    dryRun: process.argv.includes("--dry-run"),
    skipDictionary: process.argv.includes("--skip-dictionary"),
  };
}

interface NounRow {
  vocalized: string;
  unvocalized: string;
  root: string;
  wazn: string;
  wordtype: string;
  definition: string;
}

interface VerbRow {
  vocalized: string;
  unvocalized: string;
  root: string;
}

interface RootRecord {
  word: string;
  root: string;
  vocalized?: string;
  pattern?: string;
  wordType?: string;
  definition?: string;
  partOfSpeech: string;
  source: string;
}

async function main() {
  const { dryRun, skipDictionary } = parseArgs();

  console.log(`Opening Arramooz SQLite: ${SQLITE_PATH}`);
  const db = new Database(SQLITE_PATH, { readonly: true });

  const records: RootRecord[] = [];
  const seen = new Set<string>(); // "word|root" dedup key

  function addRecord(rec: RootRecord) {
    const key = `${rec.word}|${rec.root}`;
    if (seen.has(key)) return;
    if (!rec.word || !rec.root) return;
    if (rec.word === rec.root && rec.source === "arramooz") return; // skip identity for arramooz
    seen.add(key);
    records.push(rec);
  }

  // ---- Import nouns with full metadata ----
  const nouns = db.query(
    `SELECT vocalized, unvocalized, root, wazn, wordtype, definition
     FROM nouns
     WHERE root IS NOT NULL AND root <> '' AND unvocalized IS NOT NULL AND unvocalized <> ''`,
  ).all() as NounRow[];

  console.log(`  nouns: ${nouns.length} rows with root data`);

  for (const row of nouns) {
    const word = normalizeArabic(row.unvocalized);
    const roots = row.root.split(/[،,]/).map((r) => normalizeArabic(r.trim())).filter(Boolean);
    const vocalized = row.vocalized?.trim() || undefined;
    const pattern = row.wazn?.trim() || undefined;
    const wordType = row.wordtype?.trim() || undefined;
    const def = row.definition?.trim();
    const definition = def && def !== ":لا شرح" ? def : undefined;

    for (const root of roots) {
      addRecord({
        word, root, vocalized, pattern, wordType, definition,
        partOfSpeech: "noun",
        source: "arramooz",
      });
    }
  }

  // ---- Import verbs (vocalized form + root) ----
  const verbs = db.query(
    `SELECT vocalized, unvocalized, root
     FROM verbs
     WHERE root IS NOT NULL AND root <> '' AND unvocalized IS NOT NULL AND unvocalized <> ''`,
  ).all() as VerbRow[];

  console.log(`  verbs: ${verbs.length} rows with root data`);

  for (const row of verbs) {
    const word = normalizeArabic(row.unvocalized);
    const roots = row.root.split(/[،,]/).map((r) => normalizeArabic(r.trim())).filter(Boolean);
    const vocalized = row.vocalized?.trim() || undefined;

    for (const root of roots) {
      addRecord({
        word, root, vocalized,
        partOfSpeech: "verb",
        source: "arramooz",
      });
    }
  }

  db.close();
  console.log(`\nArramooz records: ${records.length}`);

  // ---- Mine dictionary headwords as identity root mappings ----
  if (!skipDictionary) {
    console.log("\nMining dictionary headwords...");
    const headwords = await prisma.dictionaryEntry.findMany({
      select: { rootNormalized: true },
      distinct: ["rootNormalized"],
    });

    let dictCount = 0;
    for (const hw of headwords) {
      const root = hw.rootNormalized;
      if (!root || root.length < 2 || root.length > 5) continue;
      const key = `${root}|${root}`;
      if (seen.has(key)) continue;
      seen.add(key);
      records.push({
        word: root,
        root,
        partOfSpeech: "noun", // headwords are typically noun-like roots
        source: "dictionary",
      });
      dictCount++;
    }
    console.log(`  Dictionary headword roots added: ${dictCount}`);
  }

  console.log(`\nTotal records to import: ${records.length}`);

  if (dryRun) {
    console.log("\n[DRY RUN] Sample entries:");
    for (const r of records.slice(0, 20)) {
      const extra = [r.vocalized, r.pattern, r.wordType].filter(Boolean).join(" | ");
      console.log(`  ${r.word} → ${r.root} [${r.source}/${r.partOfSpeech}]${extra ? ` (${extra})` : ""}`);
    }

    // Stats
    const withVocalized = records.filter((r) => r.vocalized).length;
    const withPattern = records.filter((r) => r.pattern).length;
    const withWordType = records.filter((r) => r.wordType).length;
    const withDefinition = records.filter((r) => r.definition).length;
    console.log(`\nMetadata coverage:`);
    console.log(`  vocalized: ${withVocalized} (${((withVocalized / records.length) * 100).toFixed(1)}%)`);
    console.log(`  pattern:   ${withPattern} (${((withPattern / records.length) * 100).toFixed(1)}%)`);
    console.log(`  wordType:  ${withWordType} (${((withWordType / records.length) * 100).toFixed(1)}%)`);
    console.log(`  definition: ${withDefinition} (${((withDefinition / records.length) * 100).toFixed(1)}%)`);
    console.log("\nDry run complete — no data written.");
    return;
  }

  // Clear existing data and re-insert for clean import
  console.log("\nClearing existing arabic_roots data...");
  await prisma.arabicRoot.deleteMany({});

  // Batch insert
  let inserted = 0;
  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const batch = records.slice(i, i + BATCH_SIZE);
    const result = await prisma.arabicRoot.createMany({
      data: batch.map((r) => ({
        word: r.word,
        root: r.root,
        vocalized: r.vocalized ?? null,
        pattern: r.pattern ?? null,
        wordType: r.wordType ?? null,
        definition: r.definition ?? null,
        partOfSpeech: r.partOfSpeech,
        source: r.source,
      })),
      skipDuplicates: true,
    });
    inserted += result.count;

    if ((i / BATCH_SIZE) % 10 === 0) {
      console.log(`  Batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(records.length / BATCH_SIZE)} — ${inserted} inserted so far`);
    }
  }

  console.log(`\nDone! Inserted ${inserted} word→root mappings.`);
  await prisma.$disconnect();
}

main().catch((err) => {
  console.error("Import failed:", err);
  process.exit(1);
});
