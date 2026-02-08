/**
 * Import Tafsir Script
 *
 * Fetches Al-Jalalayn tafsir for all Quran ayahs from quran-tafseer.com API
 * and stores them in the AyahTafsir table.
 *
 * API: http://api.quran-tafseer.com/tafseer/{tafsir_id}/{surah}/{ayah}
 * Al-Jalalayn ID: 2
 *
 * Usage: bun run scripts/import-tafsir.ts [--force] [--surah=N] [--delay=100]
 *
 * Options:
 *   --force       Re-import tafsirs even if they already exist
 *   --surah=N     Only import tafsir for a specific surah
 *   --delay=N     Delay between API requests in milliseconds (default: 100)
 */

import "../env";
import { prisma } from "../../src/db";

// Tafsir ID for Al-Jalalayn on quran-tafseer.com
const JALALAYN_TAFSIR_ID = 2;
const TAFSIR_SOURCE = "jalalayn";

// Parse command line arguments
const forceFlag = process.argv.includes("--force");
const surahArg = process.argv.find((arg) => arg.startsWith("--surah="));
const delayArg = process.argv.find((arg) => arg.startsWith("--delay="));
const targetSurah = surahArg ? parseInt(surahArg.split("=")[1], 10) : null;
const DELAY_MS = delayArg ? parseInt(delayArg.split("=")[1], 10) : 100;

interface TafsirResponse {
  tafseer_id: number;
  tafseer_name: string;
  ayah_url: string;
  ayah_number: number;
  text: string;
}

/**
 * Fetch tafsir for a single ayah from the API
 */
async function fetchTafsir(
  surahNumber: number,
  ayahNumber: number
): Promise<string | null> {
  const url = `http://api.quran-tafseer.com/tafseer/${JALALAYN_TAFSIR_ID}/${surahNumber}/${ayahNumber}`;

  try {
    const response = await fetch(url);

    if (!response.ok) {
      console.error(
        `Failed to fetch tafsir for ${surahNumber}:${ayahNumber}: ${response.status}`
      );
      return null;
    }

    const data: TafsirResponse = await response.json();
    return data.text;
  } catch (error) {
    console.error(`Error fetching tafsir for ${surahNumber}:${ayahNumber}:`, error);
    return null;
  }
}

/**
 * Check if tafsir already exists for an ayah
 */
async function tafsirExists(
  surahNumber: number,
  ayahNumber: number
): Promise<boolean> {
  const existing = await prisma.ayahTafsir.findUnique({
    where: {
      surahNumber_ayahNumber_source: {
        surahNumber,
        ayahNumber,
        source: TAFSIR_SOURCE,
      },
    },
  });
  return existing !== null;
}

/**
 * Save tafsir to database
 */
async function saveTafsir(
  surahNumber: number,
  ayahNumber: number,
  text: string
): Promise<void> {
  await prisma.ayahTafsir.upsert({
    where: {
      surahNumber_ayahNumber_source: {
        surahNumber,
        ayahNumber,
        source: TAFSIR_SOURCE,
      },
    },
    update: { text },
    create: {
      surahNumber,
      ayahNumber,
      source: TAFSIR_SOURCE,
      text,
    },
  });
}

/**
 * Get all surahs with their ayah counts
 */
async function getSurahs(): Promise<Array<{ number: number; ayahCount: number; nameArabic: string }>> {
  const surahs = await prisma.surah.findMany({
    select: {
      number: true,
      ayahCount: true,
      nameArabic: true,
    },
    orderBy: { number: "asc" },
  });
  return surahs;
}

/**
 * Import tafsir for a single surah
 */
async function importSurahTafsir(
  surahNumber: number,
  ayahCount: number,
  surahName: string
): Promise<{ imported: number; skipped: number; failed: number }> {
  let imported = 0;
  let skipped = 0;
  let failed = 0;

  console.log(`\nImporting tafsir for Surah ${surahNumber}: ${surahName} (${ayahCount} ayahs)`);

  for (let ayahNumber = 1; ayahNumber <= ayahCount; ayahNumber++) {
    // Check if already exists (unless force flag)
    if (!forceFlag && (await tafsirExists(surahNumber, ayahNumber))) {
      skipped++;
      continue;
    }

    // Fetch tafsir from API
    const tafsirText = await fetchTafsir(surahNumber, ayahNumber);

    if (tafsirText) {
      await saveTafsir(surahNumber, ayahNumber, tafsirText);
      imported++;

      // Progress indicator every 10 ayahs
      if (imported % 10 === 0) {
        process.stdout.write(`  Imported ${imported} ayahs...\r`);
      }
    } else {
      failed++;
    }

    // Rate limiting delay
    await new Promise((resolve) => setTimeout(resolve, DELAY_MS));
  }

  console.log(`  Surah ${surahNumber}: imported=${imported}, skipped=${skipped}, failed=${failed}`);
  return { imported, skipped, failed };
}

async function main() {
  console.log("Tafsir Import Script");
  console.log("=".repeat(60));
  console.log(`Source: Al-Jalalayn (ID: ${JALALAYN_TAFSIR_ID})`);
  console.log(`Mode: ${forceFlag ? "Force reimport" : "Skip existing"}`);
  console.log(`Delay: ${DELAY_MS}ms between requests`);
  if (targetSurah) {
    console.log(`Target surah: ${targetSurah}`);
  }
  console.log("=".repeat(60));

  // Get all surahs
  const surahs = await getSurahs();

  if (surahs.length === 0) {
    console.error("No surahs found in database. Run import-quran.ts first.");
    return;
  }

  console.log(`Found ${surahs.length} surahs in database`);

  // Filter to target surah if specified
  const surahsToProcess = targetSurah
    ? surahs.filter((s) => s.number === targetSurah)
    : surahs;

  if (targetSurah && surahsToProcess.length === 0) {
    console.error(`Surah ${targetSurah} not found in database`);
    return;
  }

  let totalImported = 0;
  let totalSkipped = 0;
  let totalFailed = 0;

  for (const surah of surahsToProcess) {
    const { imported, skipped, failed } = await importSurahTafsir(
      surah.number,
      surah.ayahCount,
      surah.nameArabic
    );
    totalImported += imported;
    totalSkipped += skipped;
    totalFailed += failed;
  }

  console.log("\n" + "=".repeat(60));
  console.log("IMPORT SUMMARY");
  console.log("=".repeat(60));
  console.log(`Total imported: ${totalImported}`);
  console.log(`Total skipped:  ${totalSkipped}`);
  console.log(`Total failed:   ${totalFailed}`);
  console.log("=".repeat(60));

  // Verify by counting records
  const count = await prisma.ayahTafsir.count({
    where: { source: TAFSIR_SOURCE },
  });
  console.log(`\nTotal Al-Jalalayn tafsirs in database: ${count}`);
}

main()
  .catch((e) => {
    console.error("\nTafsir import failed:");
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
