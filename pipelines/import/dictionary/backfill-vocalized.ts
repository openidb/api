/**
 * Backfill headwordVocalized column for DictionaryEntry and DictionarySubEntry.
 *
 * Reads each row's headword, applies normalizeArabicLight() if it has tashkeel,
 * and writes the result to headwordVocalized.
 *
 * Usage:
 *   bun run pipelines/import/dictionary/backfill-vocalized.ts [--dry-run]
 */

import "../../env";
import { prisma } from "../../../src/db";
import { normalizeArabicLight, hasTashkeel } from "../../../src/utils/arabic-text";

const BATCH_SIZE = 1000;

function parseArgs() {
  return { dryRun: process.argv.includes("--dry-run") };
}

async function backfillTable(table: "dictionaryEntry" | "dictionarySubEntry", dryRun: boolean) {
  const tableName = table === "dictionaryEntry" ? "DictionaryEntry" : "DictionarySubEntry";
  const model = table === "dictionaryEntry" ? prisma.dictionaryEntry : prisma.dictionarySubEntry;

  // Count rows that need backfill (headwordVocalized is empty but headword has tashkeel)
  const total = await (model as any).count();
  console.log(`\n${tableName}: ${total} total rows`);

  let updated = 0;
  let skipped = 0;
  let cursor: number | undefined;

  while (true) {
    const rows = await (model as any).findMany({
      take: BATCH_SIZE,
      ...(cursor ? { skip: 1, cursor: { id: cursor } } : {}),
      select: { id: true, headword: true, headwordVocalized: true },
      orderBy: { id: "asc" },
    });

    if (rows.length === 0) break;
    cursor = rows[rows.length - 1].id;

    const updates: Array<{ id: number; headwordVocalized: string }> = [];

    for (const row of rows) {
      if (row.headwordVocalized && row.headwordVocalized.length > 0) {
        skipped++;
        continue;
      }

      if (hasTashkeel(row.headword)) {
        updates.push({
          id: row.id,
          headwordVocalized: normalizeArabicLight(row.headword),
        });
      } else {
        skipped++;
      }
    }

    if (updates.length > 0 && !dryRun) {
      // Use raw SQL for batch update efficiency
      const cases = updates
        .map((u) => `WHEN ${u.id} THEN '${u.headwordVocalized.replace(/'/g, "''")}'`)
        .join(" ");
      const ids = updates.map((u) => u.id).join(",");
      const sqlTable = table === "dictionaryEntry" ? "dictionary_entries" : "dictionary_sub_entries";
      await prisma.$executeRawUnsafe(
        `UPDATE ${sqlTable} SET headword_vocalized = CASE id ${cases} END WHERE id IN (${ids})`
      );
    }

    updated += updates.length;
    const total_processed = updated + skipped;
    if (total_processed % 10000 === 0 || rows.length < BATCH_SIZE) {
      console.log(`  ${tableName}: ${updated} updated, ${skipped} skipped (${total_processed}/${total})`);
    }
  }

  console.log(`${tableName}: Done. ${updated} updated, ${skipped} skipped.`);
}

async function main() {
  const { dryRun } = parseArgs();

  if (dryRun) console.log("--- DRY RUN ---\n");

  await backfillTable("dictionaryEntry", dryRun);
  await backfillTable("dictionarySubEntry", dryRun);

  console.log("\nBackfill complete.");
}

main()
  .catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
  })
  .finally(() => prisma.$disconnect());
