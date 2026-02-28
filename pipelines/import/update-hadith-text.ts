/**
 * Update hadith text_arabic and footnotes from re-parsed extracted JSON files.
 * This does NOT delete/recreate hadiths — it only updates the text fields
 * for hadiths that were affected by the footnote-splitting bug.
 *
 * Usage:
 *   bun run pipelines/import/update-hadith-text.ts --collection=ahmad
 *   bun run pipelines/import/update-hadith-text.ts --all
 *   bun run pipelines/import/update-hadith-text.ts --all --dry-run
 */

import "../env";
import { prisma } from "../../src/db";
import { readFileSync, readdirSync, existsSync, appendFileSync } from "fs";
import { join } from "path";
import { getConfig, COLLECTIONS } from "./turath-hadith-configs";

interface ExtractedHadith {
  hadithNumber: string;
  matn: string;
  footnotes: string | null;
  pageStart: number;
  pageEnd: number;
}

interface ExtractedChunk {
  hadiths: ExtractedHadith[];
}

/** Strip printed page markers like ⦗٣٢⦘ */
function stripPageMarkers(s: string): string {
  return s
    .replace(/\s*⦗[٠-٩]+⦘\s*/g, " ")
    .replace(/\s{2,}/g, " ")
    .trim();
}

const UPDATED_LOG = "/tmp/updated-hadith-ids.tsv";

async function updateCollection(slug: string, dryRun: boolean): Promise<{ updated: number; skipped: number }> {
  const config = getConfig(slug);
  const cacheDir = config.cacheDir;

  // Read extracted files
  if (!existsSync(cacheDir)) {
    console.log(`  No cache directory for ${slug}, skipping`);
    return { updated: 0, skipped: 0 };
  }

  const files = readdirSync(cacheDir)
    .filter((f) => f.match(/^chunk-\d+\.extracted\.json$/))
    .sort();

  if (files.length === 0) {
    console.log(`  No extracted files found for ${slug}`);
    return { updated: 0, skipped: 0 };
  }

  // Build hadith map from extracted data — prefer the variant with the longest matn
  const extractedMap = new Map<string, ExtractedHadith>();
  for (const file of files) {
    const chunk: ExtractedChunk = JSON.parse(readFileSync(join(cacheDir, file), "utf8"));
    for (const h of chunk.hadiths) {
      const existing = extractedMap.get(h.hadithNumber);
      if (!existing || h.matn.length > existing.matn.length) {
        extractedMap.set(h.hadithNumber, h);
      }
    }
  }

  // Get collection's book IDs
  const collection = await prisma.hadithCollection.findUnique({
    where: { slug },
    select: { books: { select: { id: true } } },
  });

  if (!collection) {
    console.log(`  Collection ${slug} not found in DB`);
    return { updated: 0, skipped: 0 };
  }

  const bookIds = collection.books.map((b) => b.id);

  // Fetch all hadiths for this collection
  const hadiths = await prisma.hadith.findMany({
    where: { bookId: { in: bookIds } },
    select: { id: true, bookId: true, hadithNumber: true, textArabic: true, footnotes: true },
  });

  let updated = 0;
  let skipped = 0;

  // Batch updates
  const BATCH = 200;
  const updates: { id: number; bookId: number; hadithNumber: string; textArabic: string; footnotes: string | null }[] = [];

  for (const hadith of hadiths) {
    const extracted = extractedMap.get(hadith.hadithNumber);
    if (!extracted) {
      skipped++;
      continue;
    }

    const newText = stripPageMarkers(extracted.matn);
    const newFootnotes = extracted.footnotes || null;

    // Only update if text actually changed
    if (newText === hadith.textArabic && newFootnotes === hadith.footnotes) {
      skipped++;
      continue;
    }

    updates.push({ id: hadith.id, bookId: hadith.bookId, hadithNumber: hadith.hadithNumber, textArabic: newText, footnotes: newFootnotes });
  }

  if (dryRun) {
    console.log(`  [DRY RUN] Would update ${updates.length} hadiths, skip ${skipped}`);
    return { updated: updates.length, skipped };
  }

  // Execute updates in batches
  for (let i = 0; i < updates.length; i += BATCH) {
    const batch = updates.slice(i, i + BATCH);
    await prisma.$transaction(
      batch.map((u) =>
        prisma.hadith.update({
          where: { id: u.id },
          data: { textArabic: u.textArabic, footnotes: u.footnotes },
        })
      )
    );
    // Log updated hadith IDs for re-translation targeting
    for (const u of batch) {
      appendFileSync(UPDATED_LOG, `${slug}\t${u.bookId}\t${u.hadithNumber}\n`);
    }
    if ((i + BATCH) % 1000 === 0 || i + BATCH >= updates.length) {
      console.log(`  ${Math.min(i + BATCH, updates.length)}/${updates.length} updated`);
    }
  }

  updated = updates.length;
  return { updated, skipped };
}

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes("--dry-run");
  const all = args.includes("--all");
  const collectionArg = args.find((a) => a.startsWith("--collection="));

  if (!all && !collectionArg) {
    console.error("Usage: --collection=SLUG or --all [--dry-run]");
    process.exit(1);
  }

  const slugs = all
    ? Object.keys(COLLECTIONS).filter((s) => s !== "suyuti")
    : [collectionArg!.slice(13)];

  // Init log file
  if (!dryRun) {
    appendFileSync(UPDATED_LOG, "slug\tbookId\thadithNumber\n");
    console.log(`Updated hadith log: ${UPDATED_LOG}`);
  }

  let grandUpdated = 0;
  let grandSkipped = 0;

  for (const slug of slugs) {
    console.log(`\n=== ${slug} ===`);
    const { updated, skipped } = await updateCollection(slug, dryRun);
    console.log(`  Updated: ${updated}, Skipped: ${skipped}`);
    grandUpdated += updated;
    grandSkipped += skipped;
  }

  console.log(`\nTotal: ${grandUpdated} updated, ${grandSkipped} skipped`);
  await prisma.$disconnect();
}

main().catch((err) => {
  console.error("Fatal:", err);
  prisma.$disconnect();
  process.exit(1);
});
