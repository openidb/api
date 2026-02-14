/**
 * Import Arabic Roots & Derivatives from KhorsiCorpus (Taj al-Arus)
 *
 * Downloads and parses a MySQL dump containing 142,000+ word→root records
 * extracted from Taj al-Arus dictionary. CC-BY-SA licensed.
 *
 * Data source: https://sourceforge.net/projects/arabicrootsandderivatives/
 *
 * Usage:
 *   bun run pipelines/import/dictionary/import-taj-derivatives.ts [--dry-run] [--file=path/to/KhorsiCorpus.sql]
 */

import "../../env";
import { prisma } from "../../../src/db";
import { normalizeArabic } from "../../../src/utils/arabic-text";
import { resolve } from "path";
import { existsSync } from "fs";
import { execSync } from "child_process";

const DEFAULT_SQL_PATH = resolve(import.meta.dir, "data/KhorsiCorpus.sql");
const BZ2_PATH = resolve(import.meta.dir, "data/KhorsiCorpus.sql.bz2");
const DOWNLOAD_URL = "https://sourceforge.net/projects/arabicrootsandderivatives/files/latest/download";
const BATCH_SIZE = 1000;

function parseArgs(): { dryRun: boolean; filePath: string } {
  const args = process.argv.slice(2);
  let dryRun = false;
  let filePath = DEFAULT_SQL_PATH;

  for (const arg of args) {
    if (arg === "--dry-run") dryRun = true;
    else if (arg.startsWith("--file=")) filePath = resolve(arg.split("=")[1]);
  }

  return { dryRun, filePath };
}

interface DerivativeRecord {
  word: string;
  root: string;
  vocalized?: string;
}

/**
 * Parse MySQL INSERT statements from the SQL dump.
 * KhorsiCorpus schema: (id, root, word, unvowelword, nonormstem)
 * Each row contains both the root and the derived word directly.
 */
function parseMySQLDump(sql: string): DerivativeRecord[] {
  const records: DerivativeRecord[] = [];

  // Match INSERT INTO statements
  const insertRe = /INSERT\s+INTO\s+[`"]?\w+[`"]?\s*(?:\([^)]+\))?\s*VALUES\s*/gi;
  let match: RegExpExecArray | null;

  while ((match = insertRe.exec(sql)) !== null) {
    const startIdx = match.index + match[0].length;
    const endIdx = sql.indexOf(";", startIdx);
    if (endIdx === -1) continue;

    const valuesStr = sql.slice(startIdx, endIdx);

    // Parse individual value tuples: (val1, val2, ...)
    const tupleRe = /\(([^)]+)\)/g;
    let tupleMatch: RegExpExecArray | null;

    while ((tupleMatch = tupleRe.exec(valuesStr)) !== null) {
      const tuple = tupleMatch[1];
      const fields = parseCSVFields(tuple);

      // Schema: (id, root, word, unvowelword, nonormstem)
      if (fields.length >= 3) {
        const root = unquoteSQL(fields[1]);
        const vocalized = unquoteSQL(fields[2]); // vocalized word form
        const unvoweled = fields.length >= 4 ? unquoteSQL(fields[3]) : undefined;

        if (root && /[\u0600-\u06FF]/.test(root)) {
          // Use unvoweled form as the word, vocalized as metadata
          const word = unvoweled || vocalized;
          if (word && /[\u0600-\u06FF]/.test(word)) {
            records.push({
              word: normalizeArabic(word),
              root: normalizeArabic(root),
              vocalized: vocalized?.trim() || undefined,
            });
          }
        }
      }
    }
  }

  return records;
}

function unquoteSQL(s: string): string {
  const trimmed = s.trim();
  if ((trimmed.startsWith("'") && trimmed.endsWith("'")) ||
      (trimmed.startsWith('"') && trimmed.endsWith('"'))) {
    return trimmed.slice(1, -1).replace(/\\'/g, "'").replace(/\\"/g, '"').replace(/\\\\/g, "\\");
  }
  return trimmed;
}

function parseCSVFields(line: string): string[] {
  const fields: string[] = [];
  let current = "";
  let inQuote = false;
  let quoteChar = "";

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuote) {
      if (ch === "\\" && i + 1 < line.length) {
        current += ch + line[i + 1];
        i++;
      } else if (ch === quoteChar) {
        current += ch;
        inQuote = false;
      } else {
        current += ch;
      }
    } else if (ch === "'" || ch === '"') {
      current += ch;
      inQuote = true;
      quoteChar = ch;
    } else if (ch === ",") {
      fields.push(current);
      current = "";
    } else {
      current += ch;
    }
  }
  fields.push(current);
  return fields;
}

/**
 * Alternative parser: if tables don't match expected names,
 * try to auto-detect from CREATE TABLE statements.
 */
function detectTableSchema(sql: string): { rootTable: string; derivTable: string; rootCol: number; wordCol: number; rootIdCol: number } | null {
  const createRe = /CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`"]?(\w+)[`"]?\s*\(([^;]+)\)/gi;
  let m: RegExpExecArray | null;
  const tables: Record<string, string[]> = {};

  while ((m = createRe.exec(sql)) !== null) {
    const name = m[1].toLowerCase();
    const cols = m[2].split(/\n/).map((l) => l.trim()).filter((l) => l && !l.startsWith("PRIMARY") && !l.startsWith("KEY") && !l.startsWith("INDEX") && !l.startsWith("UNIQUE") && !l.startsWith(")"));
    const colNames = cols.map((c) => {
      const parts = c.replace(/^[`"]/,"").split(/[\s`"]/);
      return parts[0].toLowerCase();
    });
    tables[name] = colNames;
  }

  console.log("  Detected tables:", Object.keys(tables).join(", "));
  for (const [name, cols] of Object.entries(tables)) {
    console.log(`    ${name}: ${cols.join(", ")}`);
  }

  return null;
}

async function main() {
  const { dryRun, filePath } = parseArgs();

  // Check if SQL file exists, if not try to download and decompress
  if (!existsSync(filePath)) {
    if (existsSync(BZ2_PATH)) {
      console.log(`Decompressing ${BZ2_PATH}...`);
      execSync(`bunzip2 -k "${BZ2_PATH}"`);
    } else {
      console.log(`Downloading KhorsiCorpus from SourceForge...`);
      console.log(`  URL: ${DOWNLOAD_URL}`);
      execSync(`curl -L -o "${BZ2_PATH}" "${DOWNLOAD_URL}"`, { stdio: "inherit" });
      console.log(`Decompressing...`);
      execSync(`bunzip2 -k "${BZ2_PATH}"`);
    }
  }

  if (!existsSync(filePath)) {
    console.error(`SQL file not found at ${filePath}`);
    console.error("Download manually from: https://sourceforge.net/projects/arabicrootsandderivatives/");
    process.exit(1);
  }

  console.log(`Reading SQL dump: ${filePath}`);
  const sql = await Bun.file(filePath).text();
  console.log(`  File size: ${(sql.length / 1024 / 1024).toFixed(1)} MB`);

  // Auto-detect schema
  detectTableSchema(sql);

  // Parse the dump
  console.log("\nParsing INSERT statements...");
  const records = parseMySQLDump(sql);
  console.log(`  Parsed ${records.length} derivative records`);

  // Deduplicate
  const seen = new Set<string>();
  const unique: DerivativeRecord[] = [];
  for (const r of records) {
    if (!r.word || !r.root) continue;
    const key = `${r.word}|${r.root}`;
    if (seen.has(key)) continue;
    seen.add(key);
    unique.push(r);
  }
  console.log(`  Unique (word, root) pairs: ${unique.length}`);

  if (dryRun) {
    console.log("\n[DRY RUN] Sample entries:");
    for (const r of unique.slice(0, 30)) {
      console.log(`  ${r.word} → ${r.root}${r.vocalized ? ` (${r.vocalized})` : ""}`);
    }
    const distinctRoots = new Set(unique.map((r) => r.root));
    console.log(`\n  Distinct roots: ${distinctRoots.size}`);
    console.log(`  Total derivatives: ${unique.length}`);
    console.log("\nDry run complete — no data written.");
    return;
  }

  // Batch insert with skipDuplicates
  console.log("\nInserting into arabic_roots table...");
  let inserted = 0;
  for (let i = 0; i < unique.length; i += BATCH_SIZE) {
    const batch = unique.slice(i, i + BATCH_SIZE);
    const result = await prisma.arabicRoot.createMany({
      data: batch.map((r) => ({
        word: r.word,
        root: r.root,
        vocalized: r.vocalized ?? null,
        source: "taj-derivatives",
      })),
      skipDuplicates: true,
    });
    inserted += result.count;

    if ((i / BATCH_SIZE) % 20 === 0) {
      console.log(`  Batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(unique.length / BATCH_SIZE)} — ${inserted} inserted so far`);
    }
  }

  console.log(`\nDone! Inserted ${inserted} Taj al-Arus derivative mappings.`);
  const total = await prisma.arabicRoot.count();
  console.log(`Total arabic_roots records: ${total}`);
  await prisma.$disconnect();
}

main().catch((err) => {
  console.error("Import failed:", err);
  process.exit(1);
});
