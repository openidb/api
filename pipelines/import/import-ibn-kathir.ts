/**
 * Import Ibn Kathir Tafsir Script
 *
 * Fetches Ibn Kathir tafsir from the tafsir_api CDN (spa5k/tafsir_api)
 * and stores entries in the AyahTafsir table.
 *
 * API: https://cdn.jsdelivr.net/gh/spa5k/tafsir_api@main/tafsir/ar-tafsir-ibn-kathir/{surah}.json
 * Returns: { ayahs: [{ ayah, surah, text }] } — one entry per ayah
 *
 * Usage: bun run scripts/import-ibn-kathir.ts --surah=28 --surah=108 --surah=112
 *        bun run scripts/import-ibn-kathir.ts --all [--force]
 */

import "../env";
import { prisma } from "../../src/db";

const TAFSIR_SOURCE = "ibn_kathir";

// Parse arguments
const forceFlag = process.argv.includes("--force");
const allFlag = process.argv.includes("--all");
const surahArgs = process.argv
  .filter((arg) => arg.startsWith("--surah="))
  .map((arg) => parseInt(arg.split("=")[1], 10));

interface IbnKathirEntry {
  ayah: number;
  surah: number;
  text: string;
}

interface IbnKathirResponse {
  ayahs: IbnKathirEntry[];
}

async function fetchSurahTafsir(surahNumber: number): Promise<IbnKathirEntry[]> {
  const url = `https://cdn.jsdelivr.net/gh/spa5k/tafsir_api@main/tafsir/ar-tafsir-ibn-kathir/${surahNumber}.json`;

  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to fetch surah ${surahNumber}: ${response.status}`);
  }

  const data: IbnKathirResponse = await response.json();
  return data.ayahs;
}

async function importSurah(surahNumber: number): Promise<{ imported: number; skipped: number }> {
  let imported = 0;
  let skipped = 0;

  console.log(`\nFetching Ibn Kathir tafsir for surah ${surahNumber}...`);
  const entries = await fetchSurahTafsir(surahNumber);
  console.log(`  Got ${entries.length} entries`);

  for (const entry of entries) {
    if (!forceFlag) {
      const existing = await prisma.ayahTafsir.findUnique({
        where: {
          surahNumber_ayahNumber_source: {
            surahNumber: entry.surah,
            ayahNumber: entry.ayah,
            source: TAFSIR_SOURCE,
          },
        },
      });
      if (existing) {
        skipped++;
        continue;
      }
    }

    await prisma.ayahTafsir.upsert({
      where: {
        surahNumber_ayahNumber_source: {
          surahNumber: entry.surah,
          ayahNumber: entry.ayah,
          source: TAFSIR_SOURCE,
        },
      },
      update: { text: entry.text },
      create: {
        surahNumber: entry.surah,
        ayahNumber: entry.ayah,
        source: TAFSIR_SOURCE,
        text: entry.text,
      },
    });
    imported++;
  }

  console.log(`  Surah ${surahNumber}: imported=${imported}, skipped=${skipped}`);
  return { imported, skipped };
}

async function main() {
  console.log("Ibn Kathir Tafsir Import");
  console.log("=".repeat(60));
  console.log(`Mode: ${forceFlag ? "Force reimport" : "Skip existing"}`);

  let surahs: number[];

  if (allFlag) {
    const allSurahs = await prisma.surah.findMany({
      select: { number: true },
      orderBy: { number: "asc" },
    });
    surahs = allSurahs.map((s) => s.number);
  } else if (surahArgs.length > 0) {
    surahs = surahArgs;
  } else {
    console.error("Usage: --surah=N [--surah=N ...] or --all");
    process.exit(1);
  }

  console.log(`Surahs: ${surahs.join(", ")}`);
  console.log("=".repeat(60));

  let totalImported = 0;
  let totalSkipped = 0;

  for (const surah of surahs) {
    const { imported, skipped } = await importSurah(surah);
    totalImported += imported;
    totalSkipped += skipped;
  }

  console.log("\n" + "=".repeat(60));
  console.log(`Total imported: ${totalImported}`);
  console.log(`Total skipped:  ${totalSkipped}`);

  const count = await prisma.ayahTafsir.count({
    where: { source: TAFSIR_SOURCE },
  });
  console.log(`Total Ibn Kathir tafsirs in database: ${count}`);
}

main()
  .catch((e) => {
    console.error("Import failed:", e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
