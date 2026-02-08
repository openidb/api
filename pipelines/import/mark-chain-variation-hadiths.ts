/**
 * Mark Chain Variation Hadiths
 *
 * Applies the conservative multi-criteria detection and marks matching hadiths
 * with isChainVariation=true in the database.
 *
 * Prerequisites:
 * - Run Prisma migration to add is_chain_variation column:
 *   bunx prisma migrate dev --name add_chain_variation_field
 *
 * Usage: bun run scripts/mark-chain-variation-hadiths.ts [--dry-run]
 */

import "../env";
import { prisma } from "../../src/db";

/**
 * Conservative multi-criteria detection for chain variation hadiths.
 * Returns true ONLY if the hadith is clearly just a chain variation with no real content.
 */
function isChainVariationOnly(textPlain: string): boolean {
  const text = textPlain.trim();
  const length = text.length;

  // ===========================================
  // TYPE A: Simple chain variations (very short)
  // Pattern: Isnad only + terminal marker like "نحوه" (similar to it)
  // Example: "حدثنا X عن Y عن Z نحوه"
  // ===========================================
  const endsWithSimpleMarker = /(?:نحوه|بمثله|مثله)\s*\.?\s*$/.test(text);
  if (endsWithSimpleMarker && length < 150) {
    return true;
  }

  // ===========================================
  // TYPE B: Chain variations with reference to another hadith
  // Structure: Isnad + "بنحو حديث X" or "بهذا الحديث نحو حديث X"
  // NOT just the word "نحو" alone (too many false positives)
  // ===========================================

  // Full phrase patterns (REQUIRED - must match one of these)
  const chainVariationPhrases = [
    /بنحو\s+حديث/,           // "بنحو حديث X" - similar to the hadith of X
    /بهذا\s+الحديث\s+نحو/,    // "بهذا الحديث نحو" - with this hadith similar to
    /نحو\s+حديث\s+\S+/,       // "نحو حديث مالك" - similar to hadith of X
    /بمعنى\s+حديث/,           // "بمعنى حديث X" - with meaning of hadith of X
    /بمعناه\s*$/,             // "بمعناه" at end - with its meaning (terminal)
  ];

  const hasChainVariationPhrase = chainVariationPhrases.some(p => p.test(text));
  if (!hasChainVariationPhrase) return false;

  // ===========================================
  // TYPE B.1: With "except he said" (quotes a small difference)
  // Pattern: "بنحو حديثهم غير أنه قال" + quoted text
  // These can be longer (~500 chars) since they include quoted diff
  // ===========================================
  const hasExceptHeSaid = /غير\s*ان[هـ]?\s*قال/.test(text);
  if (hasExceptHeSaid && length < 500) {
    return true;
  }

  // ===========================================
  // TYPE B.2: Just the chain variation phrase, no content
  // Shorter threshold since no quoted material
  // ===========================================
  if (length < 250) {
    return true;
  }

  return false;
}

const BATCH_SIZE = 1000;

async function main() {
  const isDryRun = process.argv.includes("--dry-run");

  console.log("=== Mark Chain Variation Hadiths ===");
  console.log(`Mode: ${isDryRun ? "DRY RUN (no changes)" : "LIVE (will update database)"}\n`);

  // First, reset any existing chain variation flags
  if (!isDryRun) {
    const resetResult = await prisma.hadith.updateMany({
      where: { isChainVariation: true },
      data: { isChainVariation: false },
    });
    console.log(`Reset ${resetResult.count} previously marked hadiths\n`);
  }

  // Fetch all hadiths in batches
  const totalCount = await prisma.hadith.count();
  console.log(`Total hadiths in database: ${totalCount}\n`);

  const chainVariationIds: number[] = [];
  let processed = 0;
  let offset = 0;

  while (offset < totalCount) {
    const hadiths = await prisma.hadith.findMany({
      skip: offset,
      take: BATCH_SIZE,
      select: {
        id: true,
        textPlain: true,
      },
    });

    if (hadiths.length === 0) break;

    for (const hadith of hadiths) {
      if (isChainVariationOnly(hadith.textPlain)) {
        chainVariationIds.push(hadith.id);
      }
    }

    processed += hadiths.length;
    offset += BATCH_SIZE;

    const pct = ((processed / totalCount) * 100).toFixed(1);
    process.stdout.write(`\rProcessed: ${processed}/${totalCount} (${pct}%) - Found: ${chainVariationIds.length}`);
  }

  console.log(`\n\nTotal chain variations found: ${chainVariationIds.length}`);
  console.log(`Percentage: ${((chainVariationIds.length / totalCount) * 100).toFixed(2)}%`);

  if (isDryRun) {
    console.log("\n[DRY RUN] Would mark the following IDs:");
    console.log(`  First 20: ${chainVariationIds.slice(0, 20).join(", ")}`);
    console.log(`  Last 20: ${chainVariationIds.slice(-20).join(", ")}`);
    console.log("\nRun without --dry-run to apply changes.");
  } else {
    // Update in batches
    console.log("\nUpdating database...");
    const updateBatchSize = 500;
    let updated = 0;

    for (let i = 0; i < chainVariationIds.length; i += updateBatchSize) {
      const batch = chainVariationIds.slice(i, i + updateBatchSize);
      await prisma.hadith.updateMany({
        where: { id: { in: batch } },
        data: { isChainVariation: true },
      });
      updated += batch.length;
      process.stdout.write(`\rUpdated: ${updated}/${chainVariationIds.length}`);
    }

    console.log("\n\n=== Update Complete ===");

    // Verify
    const markedCount = await prisma.hadith.count({
      where: { isChainVariation: true },
    });
    console.log(`Verification: ${markedCount} hadiths now marked as chain variations`);
  }

  await prisma.$disconnect();
}

main().catch((err) => {
  console.error("Marking failed:", err);
  process.exit(1);
});
