/**
 * Import Quran Tafsirs Script (Unified)
 *
 * Fetches all available tafsirs from spa5k/tafsir_api CDN and imports
 * them into the database. Replaces the separate import-tafsir.ts (Jalalayn)
 * and import-ibn-kathir.ts scripts.
 *
 * Data Source: https://github.com/spa5k/tafsir_api
 * CDN: https://cdn.jsdelivr.net/gh/spa5k/tafsir_api@main/tafsir/{slug}/{surah}.json
 *
 * Usage:
 *   bun run pipelines/import/import-tafsirs.ts --all [--force]
 *   bun run pipelines/import/import-tafsirs.ts --lang=en
 *   bun run pipelines/import/import-tafsirs.ts --edition=en-al-jalalayn
 */

import "../env";
import { prisma } from "../../src/db";
import {
  fetchTafsirEditions,
  syncTafsirMetadata,
  TAFSIR_CDN_BASE,
  type TafsirEdition,
} from "./quran-resources";

interface TafsirSurahResponse {
  ayahs: Array<{
    ayah: number;
    surah: number;
    text: string;
  }>;
}

// Legacy source mapping for backward compat with old source column
const LEGACY_SOURCE_MAP: Record<string, string> = {
  "ar-tafsir-ibn-kathir": "ibn_kathir",
  "ar-jalalayn": "jalalayn",
};

/**
 * Fetch tafsir for a single surah from CDN
 */
async function fetchSurahTafsir(
  slug: string,
  surahNumber: number
): Promise<TafsirSurahResponse["ayahs"] | null> {
  const url = `${TAFSIR_CDN_BASE}/${slug}/${surahNumber}.json`;
  try {
    const response = await fetch(url);
    if (!response.ok) {
      if (response.status === 404) return null; // Some editions may not have all surahs
      console.error(`   Failed to fetch surah ${surahNumber}: ${response.status}`);
      return null;
    }
    const data: TafsirSurahResponse = await response.json();
    return data.ayahs;
  } catch (error) {
    console.error(`   Error fetching surah ${surahNumber}:`, error);
    return null;
  }
}

/**
 * Import a single tafsir edition
 */
async function importTafsirEdition(
  edition: TafsirEdition,
  force: boolean
): Promise<{ imported: number; skipped: boolean }> {
  console.log(`\n📖 [${edition.id}] ${edition.name} (${edition.language})...`);

  // Check if already imported
  if (!force) {
    const existingCount = await prisma.ayahTafsir.count({
      where: { editionId: edition.id },
    });
    if (existingCount > 0) {
      console.log(`   ⏭️  Already imported (${existingCount} entries). Use --force to re-import.`);
      return { imported: 0, skipped: true };
    }
  }

  // Delete existing if forcing
  if (force) {
    const deleted = await prisma.ayahTafsir.deleteMany({
      where: { editionId: edition.id },
    });
    if (deleted.count > 0) {
      console.log(`   🗑️  Deleted ${deleted.count} existing entries`);
    }
  }

  const legacySource = LEGACY_SOURCE_MAP[edition.id] || edition.slug;
  let totalImported = 0;

  // Fetch all 114 surahs
  for (let surah = 1; surah <= 114; surah++) {
    const entries = await fetchSurahTafsir(edition.slug, surah);
    if (!entries || entries.length === 0) {
      continue;
    }

    const data = entries.map((entry) => ({
      surahNumber: entry.surah,
      ayahNumber: entry.ayah,
      source: legacySource,
      editionId: edition.id,
      language: edition.language,
      text: entry.text,
    }));

    await prisma.ayahTafsir.createMany({
      data,
      skipDuplicates: true,
    });

    totalImported += data.length;
    process.stdout.write(`   📝 Surah ${surah}/114 — ${totalImported} entries total\r`);
  }

  console.log(`   ✅ Imported ${totalImported} entries`);
  return { imported: totalImported, skipped: false };
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
    console.error("  --all                Import all available tafsir editions");
    console.error("  --lang=en,ar         Import tafsirs for specific languages");
    console.error("  --edition=en-xxx     Import a single tafsir edition by ID");
    console.error("  --force              Re-import even if already exists");
    process.exit(1);
  }

  console.log("🕌 Quran Tafsir Import Script");
  console.log("=====================================");
  console.log(`Force mode: ${force ? "ON" : "OFF"}`);

  // Fetch all available editions
  console.log("Fetching available tafsir editions from CDN...");
  const allEditions = await fetchTafsirEditions();
  console.log(`Found ${allEditions.length} tafsir editions available`);

  // Sync metadata to QuranTafsir table
  console.log("Syncing tafsir metadata...");
  const synced = await syncTafsirMetadata(allEditions);
  console.log(`Synced ${synced} tafsir metadata rows`);

  // Filter editions
  let editions: TafsirEdition[];
  if (selectedEdition) {
    editions = allEditions.filter((e) => e.id === selectedEdition);
    if (editions.length === 0) {
      console.error(`Edition "${selectedEdition}" not found. Available editions:`);
      allEditions.forEach((e) => console.error(`  ${e.id} (${e.language}) — ${e.name}`));
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

  console.log(`\nImporting ${editions.length} tafsir editions...`);

  let totalImported = 0;
  let totalSkipped = 0;
  let totalFailed = 0;

  for (let i = 0; i < editions.length; i++) {
    const edition = editions[i];
    console.log(`\n[${i + 1}/${editions.length}]`);
    try {
      const { imported, skipped } = await importTafsirEdition(edition, force);
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
  console.log(`   Total entries imported: ${totalImported}`);
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
