/**
 * Dump Combined Ayah + Tafsir Data for Entity Extraction
 *
 * For each pilot surah, reads ayah text (Uthmani) and Ibn Kathir tafsir,
 * and outputs structured JSON files for knowledge graph entity extraction.
 *
 * Grouping strategy:
 * - Default: one group per ayah
 * - Short ayahs that don't stand alone (e.g. "طسم", oaths, fragments)
 *   are merged into the following ayah's group
 * - Threshold: ayahs with ≤5 words are considered too short to stand alone
 *
 * Usage: bun run scripts/knowledge-graph/dump-for-extraction.ts
 */

import "../env";
import { prisma } from "../../src/db";
import { writeFileSync } from "fs";
import { join } from "path";

const PILOT_SURAHS = [28, 108, 112];
const OUTPUT_DIR = join(import.meta.dir, "data");

interface AyahGroup {
  id: string;
  startAyah: number;
  endAyah: number;
  ayahTextArabic: string;
  tafsirText: string;
}

interface SurahDump {
  surah: number;
  surahNameArabic: string;
  surahNameEnglish: string;
  groups: AyahGroup[];
}

async function dumpSurah(surahNumber: number): Promise<SurahDump> {
  // Get surah info
  const surah = await prisma.surah.findUniqueOrThrow({
    where: { number: surahNumber },
  });

  // Get all ayahs for this surah
  const ayahs = await prisma.ayah.findMany({
    where: { surah: { number: surahNumber } },
    orderBy: { ayahNumber: "asc" },
    select: { ayahNumber: true, textUthmani: true },
  });

  // Get Ibn Kathir tafsir entries
  const tafsirs = await prisma.ayahTafsir.findMany({
    where: { surahNumber, source: "ibn_kathir" },
    orderBy: { ayahNumber: "asc" },
    select: { ayahNumber: true, text: true },
  });

  const tafsirMap = new Map(tafsirs.map((t) => [t.ayahNumber, t.text]));

  // Build groups: one per ayah by default, but merge short ayahs
  // (≤5 words) into the next ayah since they don't stand alone
  // (e.g. "طسم", oaths like "والفجر", fragments)
  const SHORT_WORD_THRESHOLD = 5;

  // First pass: identify which ayahs are too short to stand alone
  const isShort = (text: string) => {
    const words = text.trim().split(/\s+/);
    return words.length <= SHORT_WORD_THRESHOLD;
  };

  // Build merged groups: short ayahs attach to the next ayah
  const groups: AyahGroup[] = [];
  let pendingAyahs: typeof ayahs = [];

  for (let i = 0; i < ayahs.length; i++) {
    const ayah = ayahs[i];
    pendingAyahs.push(ayah);

    // If this ayah is short and not the last one, keep accumulating
    if (isShort(ayah.textUthmani) && i < ayahs.length - 1) {
      continue;
    }

    // Flush pending ayahs into a group
    const startAyah = pendingAyahs[0].ayahNumber;
    const endAyah = pendingAyahs[pendingAyahs.length - 1].ayahNumber;

    const ayahText = pendingAyahs
      .map((a) => `${a.textUthmani} ﴿${a.ayahNumber}﴾`)
      .join(" ");
    const tafsirTexts = pendingAyahs
      .map((a) => tafsirMap.get(a.ayahNumber))
      .filter(Boolean)
      .join("\n\n");

    const id =
      startAyah === endAyah
        ? `${surahNumber}:${startAyah}`
        : `${surahNumber}:${startAyah}-${endAyah}`;

    groups.push({
      id,
      startAyah,
      endAyah,
      ayahTextArabic: ayahText,
      tafsirText: tafsirTexts,
    });

    pendingAyahs = [];
  }

  return {
    surah: surahNumber,
    surahNameArabic: surah.nameArabic,
    surahNameEnglish: surah.nameEnglish,
    groups,
  };
}

async function main() {
  console.log("Dumping combined ayah + tafsir data for extraction");
  console.log("=".repeat(60));
  console.log(`Pilot surahs: ${PILOT_SURAHS.join(", ")}`);
  console.log(`Output dir: ${OUTPUT_DIR}`);
  console.log("=".repeat(60));

  for (const surahNumber of PILOT_SURAHS) {
    const dump = await dumpSurah(surahNumber);
    const outPath = join(OUTPUT_DIR, `surah-${surahNumber}.json`);
    writeFileSync(outPath, JSON.stringify(dump, null, 2), "utf-8");
    console.log(
      `Surah ${surahNumber} (${dump.surahNameEnglish}): ${dump.groups.length} groups → ${outPath}`
    );
  }

  console.log("\nDone!");
}

main()
  .catch((e) => {
    console.error("Dump failed:", e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
