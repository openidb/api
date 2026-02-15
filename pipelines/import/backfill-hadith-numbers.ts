/**
 * Backfill number_in_collection for hadithunlocked.com hadiths
 *
 * Reads cached TSV files and maps hId → num, then updates the DB.
 * The `num` field is the display number used in hadithunlocked.com URLs (e.g. "6204a-2"),
 * while `hId` (stored as hadithNumber) is the internal ID.
 *
 * Usage:
 *   bun run pipelines/import/backfill-hadith-numbers.ts
 *   bun run pipelines/import/backfill-hadith-numbers.ts --dry-run
 */

import "../env";
import { prisma } from "../../src/db";
import * as fs from "fs";
import * as path from "path";

const CACHE_DIR = path.join(import.meta.dir, "hadith-unlocked-cache");

const ALIAS_TO_SLUG: Record<string, string> = {
  "hakim": "mustadrak",
  "ibnhibban": "ibn-hibban",
  "tabarani": "mujam-kabir",
  "bayhaqi": "sunan-kubra-bayhaqi",
  "nasai-kubra": "sunan-kubra-nasai",
  "suyuti": "suyuti",
  "ahmad-zuhd": "ahmad-zuhd",
};

function parseTsv(content: string): Array<{ [key: string]: string }> {
  const lines = content.split("\n");
  if (lines.length < 2) return [];
  const headers = lines[0].split("\t");
  const rows: Array<{ [key: string]: string }> = [];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line.trim()) continue;
    const values = line.split("\t");
    const row: { [key: string]: string } = {};
    for (let j = 0; j < headers.length; j++) {
      row[headers[j]] = values[j] ?? "";
    }
    rows.push(row);
  }
  return rows;
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  console.log(`\n=== Backfill number_in_collection ${dryRun ? "(DRY RUN)" : ""} ===\n`);

  let totalUpdated = 0;

  for (const [alias, slug] of Object.entries(ALIAS_TO_SLUG)) {
    const tsvPath = path.join(CACHE_DIR, `${alias}.tsv`);
    if (!fs.existsSync(tsvPath)) {
      console.log(`  SKIP ${alias}: no cached TSV`);
      continue;
    }

    console.log(`Processing ${alias} (${slug})...`);
    const content = fs.readFileSync(tsvPath, "utf-8");
    const rows = parseTsv(content);

    // Build hId → num mapping
    const hIdToNum = new Map<string, string>();
    for (const row of rows) {
      const hId = row.hId?.trim();
      const num = row.num?.trim();
      if (hId && num && num !== "null") {
        hIdToNum.set(hId, num);
      }
    }

    console.log(`  ${hIdToNum.size} hId→num mappings from TSV`);

    if (dryRun) {
      // Show a few samples
      const samples = [...hIdToNum.entries()].slice(0, 5);
      for (const [hId, num] of samples) {
        console.log(`    hId=${hId} → num=${num}`);
      }
      totalUpdated += hIdToNum.size;
      continue;
    }

    // Get book IDs for this collection
    const collection = await prisma.hadithCollection.findUnique({
      where: { slug },
      select: { id: true },
    });
    if (!collection) {
      console.log(`  SKIP ${slug}: collection not found in DB`);
      continue;
    }

    const books = await prisma.hadithBook.findMany({
      where: { collectionId: collection.id },
      select: { id: true },
    });
    const bookIds = books.map(b => b.id);

    // Batch update using raw SQL for performance
    const BATCH_SIZE = 1000;
    const entries = [...hIdToNum.entries()];
    let updated = 0;

    for (let i = 0; i < entries.length; i += BATCH_SIZE) {
      const batch = entries.slice(i, i + BATCH_SIZE);

      // Build a single UPDATE with CASE statement
      const whenClauses = batch
        .map(([hId, num]) => `WHEN hadith_number = '${hId.replace(/'/g, "''")}' THEN '${num.replace(/'/g, "''")}'`)
        .join("\n        ");
      const hIds = batch
        .map(([hId]) => `'${hId.replace(/'/g, "''")}'`)
        .join(", ");
      const bookIdList = bookIds.join(", ");

      const sql = `
        UPDATE hadiths
        SET number_in_collection = CASE
          ${whenClauses}
        END
        WHERE book_id IN (${bookIdList})
          AND hadith_number IN (${hIds})
      `;

      const result = await prisma.$executeRawUnsafe(sql);
      updated += result;

      if ((i + BATCH_SIZE) % 5000 === 0 || i + BATCH_SIZE >= entries.length) {
        console.log(`    ${Math.min(i + BATCH_SIZE, entries.length)}/${entries.length} (${updated} rows updated)`);
      }
    }

    console.log(`  Done: ${updated} rows updated`);
    totalUpdated += updated;
  }

  console.log(`\n=== Total: ${totalUpdated} rows updated ===`);
  await prisma.$disconnect();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
