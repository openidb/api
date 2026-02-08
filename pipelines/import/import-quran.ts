/**
 * Import Holy Quran Script
 *
 * Fetches the Quran from Al Quran Cloud API and imports ayahs into the database.
 * Uses Hafs an Asim recitation with Uthmani script.
 *
 * API: https://api.alquran.cloud
 * Edition: quran-uthmani (Uthmani Hafs script with full diacritics)
 *
 * Usage: bun run scripts/import-quran.ts [--force]
 */

import "../env";
import { prisma } from "../../src/db";

const API_BASE = "https://api.alquran.cloud/v1";
const EDITION = "quran-uthmani";

// API Response types
interface SurahMeta {
  number: number;
  name: string;
  englishName: string;
  englishNameTranslation: string;
  revelationType: string;
  numberOfAyahs: number;
}

interface AyahData {
  number: number;
  text: string;
  numberInSurah: number;
  juz: number;
  manzil: number;
  page: number;
  ruku: number;
  hizbQuarter: number;
  sajda: boolean | { id: number; recommended: boolean; obligatory: boolean };
  surah: {
    number: number;
    name: string;
    englishName: string;
    englishNameTranslation: string;
    revelationType: string;
    numberOfAyahs: number;
  };
}

interface QuranResponse {
  code: number;
  status: string;
  data: {
    surahs: Array<{
      number: number;
      name: string;
      englishName: string;
      englishNameTranslation: string;
      revelationType: string;
      numberOfAyahs: number;
      ayahs: AyahData[];
    }>;
    edition: {
      identifier: string;
      language: string;
      name: string;
      englishName: string;
      type: string;
    };
  };
}

interface MetaResponse {
  code: number;
  status: string;
  data: {
    surahs: {
      count: number;
      references: SurahMeta[];
    };
    ayahs: { count: number };
    sajdas: { count: number };
  };
}

/**
 * Remove diacritics from Arabic text for plain text search
 */
function removeDiacritics(text: string): string {
  // Arabic diacritics range: \u064B-\u065F (tashkeel)
  // Also remove tatweel \u0640
  return text
    .replace(/[\u064B-\u065F\u0640]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Fetch surah metadata from API
 */
async function fetchSurahMetadata(): Promise<SurahMeta[]> {
  console.log("Fetching surah metadata...");
  const response = await fetch(`${API_BASE}/meta`);
  if (!response.ok) throw new Error(`API error: ${response.status}`);
  const data: MetaResponse = await response.json();
  return data.data.surahs.references;
}

/**
 * Fetch complete Quran from API
 */
async function fetchQuran(): Promise<QuranResponse["data"]> {
  console.log("Fetching complete Quran...");
  const response = await fetch(`${API_BASE}/quran/${EDITION}`);
  if (!response.ok) throw new Error(`API error: ${response.status}`);
  const data: QuranResponse = await response.json();
  return data.data;
}

async function main() {
  const forceFlag = process.argv.includes("--force");

  console.log("=".repeat(60));
  console.log("QURAN IMPORT");
  console.log("=".repeat(60));
  console.log(`Mode: ${forceFlag ? "Force reimport" : "Skip if exists"}\n`);

  // Check if data already exists
  const existingSurahs = await prisma.surah.count();
  const existingAyahs = await prisma.ayah.count();

  if (existingSurahs > 0 || existingAyahs > 0) {
    if (!forceFlag) {
      console.log(`Found ${existingSurahs} surahs and ${existingAyahs} ayahs in database.`);
      console.log("Use --force to reimport.");
      return;
    }

    console.log("Force mode: Deleting existing data...");
    await prisma.ayah.deleteMany();
    await prisma.surah.deleteMany();
    console.log("  Deleted existing data\n");
  }

  // Fetch metadata for surah info
  const surahMetaList = await fetchSurahMetadata();
  console.log(`Loaded metadata for ${surahMetaList.length} surahs`);

  // Fetch complete Quran
  const quranData = await fetchQuran();
  console.log(`Loaded ${quranData.surahs.length} surahs from API\n`);

  // Create surah records
  console.log("Creating surah records...");
  for (const surahData of quranData.surahs) {
    await prisma.surah.create({
      data: {
        number: surahData.number,
        nameArabic: surahData.name,
        nameEnglish: surahData.englishName,
        revelationType: surahData.revelationType,
        ayahCount: surahData.ayahs.length,
      },
    });
  }
  console.log(`  Created ${quranData.surahs.length} surah records`);

  // Get surah IDs for foreign key references
  const surahs = await prisma.surah.findMany({
    select: { id: true, number: true },
  });
  const surahIdMap = new Map(surahs.map((s) => [s.number, s.id]));

  // Create ayah records
  console.log("\nCreating ayah records...");
  let totalAyahs = 0;

  for (const surahData of quranData.surahs) {
    const surahId = surahIdMap.get(surahData.number);
    if (!surahId) {
      throw new Error(`Surah ${surahData.number} not found in database`);
    }

    // Batch insert ayahs for each surah
    const ayahRecords = surahData.ayahs.map((ayah) => ({
      surahId,
      ayahNumber: ayah.numberInSurah,
      textUthmani: ayah.text,
      textPlain: removeDiacritics(ayah.text),
      juzNumber: ayah.juz,
      pageNumber: ayah.page,
    }));

    await prisma.ayah.createMany({
      data: ayahRecords,
    });

    totalAyahs += ayahRecords.length;

    if (surahData.number % 10 === 0 || surahData.number === 114) {
      console.log(`  Processed surah ${surahData.number}/114 (${totalAyahs} ayahs)`);
    }
  }

  // Verify counts
  const finalSurahCount = await prisma.surah.count();
  const finalAyahCount = await prisma.ayah.count();

  console.log("\n" + "=".repeat(60));
  console.log("IMPORT COMPLETE");
  console.log("=".repeat(60));
  console.log(`Surahs: ${finalSurahCount}`);
  console.log(`Ayahs: ${finalAyahCount}`);
  console.log("=".repeat(60));
  console.log("\nNext steps:");
  console.log("  1. Run: bun run scripts/generate-embeddings.ts");
  console.log("  2. Verify search: bun run dev");
}

main()
  .catch((e) => {
    console.error("\nImport failed:");
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
