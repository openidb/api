/**
 * Export Authors for Enrichment
 *
 * Exports authors with biographies to numbered batch JSON files
 * for processing by Claude subagents.
 *
 * Usage:
 *   bun run pipelines/import/export-authors-for-enrichment.ts --batch-size=50
 */

import "../env";
import { prisma } from "../../src/db";
import { mkdirSync, writeFileSync } from "fs";
import { resolve } from "path";

const BATCH_DIR = resolve(import.meta.dir, "author-batches");

function parseArgs(): { batchSize: number } {
  const args = process.argv.slice(2);
  let batchSize = 50;

  for (const arg of args) {
    if (arg.startsWith("--batch-size=")) {
      batchSize = parseInt(arg.slice(13), 10) || 50;
    }
  }

  return { batchSize };
}

async function main() {
  const { batchSize } = parseArgs();

  console.log("Fetching authors with biographies...");

  const authors = await prisma.author.findMany({
    where: { biography: { not: null } },
    select: {
      id: true,
      nameArabic: true,
      biography: true,
    },
    orderBy: { id: "asc" },
  });

  console.log(`Found ${authors.length} authors with biographies.`);

  if (authors.length === 0) {
    console.log("Nothing to export.");
    return;
  }

  mkdirSync(BATCH_DIR, { recursive: true });

  const totalBatches = Math.ceil(authors.length / batchSize);
  console.log(`Writing ${totalBatches} batch files (${batchSize} authors each)...`);

  for (let i = 0; i < totalBatches; i++) {
    const batch = authors.slice(i * batchSize, (i + 1) * batchSize);
    const batchNum = String(i + 1).padStart(3, "0");
    const filePath = resolve(BATCH_DIR, `batch-${batchNum}.json`);
    writeFileSync(filePath, JSON.stringify(batch, null, 2));
    console.log(`  batch-${batchNum}.json: ${batch.length} authors`);
  }

  console.log(`\nDone. ${totalBatches} batch files written to ${BATCH_DIR}`);
}

main()
  .catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
  })
  .finally(() => prisma.$disconnect());
