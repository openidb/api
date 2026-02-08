/**
 * Import Quran Translations Script
 *
 * Fetches translations from fawazahmed0/quran-api via jsDelivr CDN
 * and imports them into the database. Supports importing all available
 * editions (~500+) or filtering by language/edition.
 *
 * Data Source: https://github.com/fawazahmed0/quran-api
 * CDN: https://cdn.jsdelivr.net/gh/fawazahmed0/quran-api@1/editions/{edition-id}.json
 *
 * Usage:
 *   bun run pipelines/import/import-quran-translations.ts --all [--force]
 *   bun run pipelines/import/import-quran-translations.ts --lang=en,fr,ur
 *   bun run pipelines/import/import-quran-translations.ts --edition=eng-mustafakhattaba
 */

import "../env";
import { prisma } from "../../src/db";
import {
  fetchTranslationEditions,
  syncTranslationMetadata,
  type TranslationEdition,
} from "./quran-resources";

// API response type from fawazahmed0/quran-api
interface QuranApiResponse {
  quran: Array<{
    chapter: number;
    verse: number;
    text: string;
  }>;
}

/**
 * Strip HTML tags from translation text (some editions have <sup> tags for footnotes)
 */
function stripHtmlTags(text: string): string {
  return text
    .replace(/<[^>]+>/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Fetch translation from jsDelivr CDN
 */
async function fetchTranslation(cdnUrl: string): Promise<QuranApiResponse | null> {
  try {
    const response = await fetch(cdnUrl);
    if (!response.ok) {
      console.error(`Failed to fetch: ${response.status} ${response.statusText}`);
      return null;
    }
    return (await response.json()) as QuranApiResponse;
  } catch (error) {
    console.error(`Error fetching:`, error);
    return null;
  }
}

/**
 * Import a single translation edition into the database
 */
async function importEdition(
  edition: TranslationEdition,
  force: boolean
): Promise<{ imported: number; skipped: boolean }> {
  console.log(`\n📖 [${edition.id}] ${edition.name} (${edition.language})...`);

  // Check if edition already exists by editionId
  if (!force) {
    const existingCount = await prisma.ayahTranslation.count({
      where: { editionId: edition.id },
    });
    if (existingCount > 0) {
      console.log(`   ⏭️  Already imported (${existingCount} ayahs). Use --force to re-import.`);
      return { imported: 0, skipped: true };
    }
  }

  // Fetch translation from CDN
  const data = await fetchTranslation(edition.cdnUrl);
  if (!data || !data.quran) {
    console.error(`   ❌ Failed to fetch translation data`);
    return { imported: 0, skipped: false };
  }

  // Delete existing translations for this edition if forcing
  if (force) {
    const deleted = await prisma.ayahTranslation.deleteMany({
      where: { editionId: edition.id },
    });
    if (deleted.count > 0) {
      console.log(`   🗑️  Deleted ${deleted.count} existing translations`);
    }
  }

  // Prepare data
  const translations = data.quran.map((ayah) => ({
    surahNumber: ayah.chapter,
    ayahNumber: ayah.verse,
    language: edition.language,
    editionId: edition.id,
    text: stripHtmlTags(ayah.text),
  }));

  // Batch insert
  const BATCH_SIZE = 1000;
  let imported = 0;

  for (let i = 0; i < translations.length; i += BATCH_SIZE) {
    const batch = translations.slice(i, i + BATCH_SIZE);
    await prisma.ayahTranslation.createMany({
      data: batch,
      skipDuplicates: true,
    });
    imported += batch.length;
    process.stdout.write(`   📝 Imported ${imported}/${translations.length} ayahs\r`);
  }

  console.log(`   ✅ Imported ${imported} ayahs`);
  return { imported, skipped: false };
}

async function main() {
  const args = process.argv.slice(2);
  const force = args.includes("--force");
  const all = args.includes("--all");
  const langArg = args.find((a) => a.startsWith("--lang="));
  const editionArg = args.find((a) => a.startsWith("--edition="));

  const selectedLangs = langArg
    ? langArg.split("=")[1].split(",").map((l) => l.trim())
    : null;
  const selectedEdition = editionArg ? editionArg.split("=")[1].trim() : null;

  if (!all && !selectedLangs && !selectedEdition) {
    console.error("Usage:");
    console.error("  --all                Import all available editions");
    console.error("  --lang=en,fr,ur      Import editions for specific languages");
    console.error("  --edition=eng-xxx    Import a single edition by ID");
    console.error("  --force              Re-import even if already exists");
    process.exit(1);
  }

  console.log("🕌 Quran Translations Import Script");
  console.log("=====================================");
  console.log(`Force mode: ${force ? "ON" : "OFF"}`);

  // Fetch all available editions from CDN
  console.log("Fetching available editions from CDN...");
  const allEditions = await fetchTranslationEditions();
  console.log(`Found ${allEditions.length} editions available`);

  // Sync metadata to QuranTranslation table
  console.log("Syncing translation metadata...");
  const synced = await syncTranslationMetadata(allEditions);
  console.log(`Synced ${synced} translation metadata rows`);

  // Filter editions
  let editions: TranslationEdition[];
  if (selectedEdition) {
    editions = allEditions.filter((e) => e.id === selectedEdition);
    if (editions.length === 0) {
      console.error(`Edition "${selectedEdition}" not found. Available editions:`);
      const sample = allEditions.slice(0, 10).map((e) => `  ${e.id} (${e.language})`);
      console.error(sample.join("\n") + "\n  ...");
      process.exit(1);
    }
  } else if (selectedLangs) {
    editions = allEditions.filter((e) => selectedLangs.includes(e.language));
    console.log(`Filtered to ${editions.length} editions for languages: ${selectedLangs.join(", ")}`);
  } else {
    editions = allEditions;
  }

  if (editions.length === 0) {
    console.error("No editions match the given filters.");
    process.exit(1);
  }

  console.log(`\nImporting ${editions.length} editions...`);

  let totalImported = 0;
  let totalSkipped = 0;
  let totalFailed = 0;

  for (let i = 0; i < editions.length; i++) {
    const edition = editions[i];
    console.log(`\n[${i + 1}/${editions.length}]`);
    try {
      const { imported, skipped } = await importEdition(edition, force);
      totalImported += imported;
      if (skipped) totalSkipped++;
    } catch (error) {
      console.error(`   ❌ Error importing ${edition.id}:`, error);
      totalFailed++;
    }
  }

  console.log("\n=====================================");
  console.log(`📊 Summary:`);
  console.log(`   Editions processed: ${editions.length}`);
  console.log(`   Total ayahs imported: ${totalImported}`);
  console.log(`   Editions skipped: ${totalSkipped}`);
  console.log(`   Editions failed: ${totalFailed}`);
  console.log("✅ Done!");
}

main()
  .catch((error) => {
    console.error("Fatal error:", error);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
