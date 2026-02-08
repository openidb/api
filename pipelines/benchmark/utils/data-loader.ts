/**
 * Data loader for benchmark: loads Quran ayahs and Hadith from Prisma.
 */

import { prisma } from "../../../src/db";
import type { QuranAyahData, HadithData } from "../types";

const TAFSIR_SOURCE = "jalalayn";
const TRANSLATION_LANGUAGE = "en";

/**
 * Load all Quran ayahs with tafsir and translation data.
 */
export async function loadQuranAyahs(): Promise<QuranAyahData[]> {
  console.log("Loading Quran ayahs...");

  const ayahs = await prisma.ayah.findMany({
    orderBy: [{ surahId: "asc" }, { ayahNumber: "asc" }],
    select: {
      ayahNumber: true,
      textUthmani: true,
      textPlain: true,
      juzNumber: true,
      pageNumber: true,
      surah: {
        select: {
          number: true,
          nameArabic: true,
          nameEnglish: true,
        },
      },
    },
  });

  // Batch-fetch all tafsirs
  const tafsirs = await prisma.ayahTafsir.findMany({
    where: { source: TAFSIR_SOURCE },
    select: {
      surahNumber: true,
      ayahNumber: true,
      text: true,
    },
  });

  const tafsirMap = new Map<string, string>();
  for (const t of tafsirs) {
    tafsirMap.set(`${t.surahNumber}:${t.ayahNumber}`, t.text);
  }

  // Batch-fetch all English translations
  const translations = await prisma.ayahTranslation.findMany({
    where: { language: TRANSLATION_LANGUAGE },
    select: {
      surahNumber: true,
      ayahNumber: true,
      text: true,
    },
  });

  const translationMap = new Map<string, string>();
  for (const t of translations) {
    translationMap.set(`${t.surahNumber}:${t.ayahNumber}`, t.text);
  }

  const result: QuranAyahData[] = ayahs.map((ayah) => {
    const key = `${ayah.surah.number}:${ayah.ayahNumber}`;
    return {
      surahNumber: ayah.surah.number,
      ayahNumber: ayah.ayahNumber,
      textUthmani: ayah.textUthmani,
      textPlain: ayah.textPlain,
      surahNameArabic: ayah.surah.nameArabic,
      surahNameEnglish: ayah.surah.nameEnglish,
      juzNumber: ayah.juzNumber,
      pageNumber: ayah.pageNumber,
      tafsirText: tafsirMap.get(key) ?? null,
      translationText: translationMap.get(key) ?? null,
    };
  });

  console.log(`Loaded ${result.length} ayahs (${tafsirMap.size} with tafsir, ${translationMap.size} with translation)`);
  return result;
}

/**
 * Load hadith subset: Nawawi 40 + first N from Bukhari + first N from Muslim.
 */
export async function loadHadithSubset(
  bukhariLimit = 200,
  muslimLimit = 200
): Promise<HadithData[]> {
  console.log("Loading hadith subset...");

  const results: HadithData[] = [];

  // Find collection IDs
  const collections = await prisma.hadithCollection.findMany({
    where: {
      slug: { in: ["nawawi40", "bukhari", "muslim"] },
    },
    select: {
      id: true,
      slug: true,
      nameArabic: true,
      nameEnglish: true,
    },
  });

  const collectionMap = new Map(collections.map((c) => [c.slug, c]));

  for (const [slug, limit] of [
    ["nawawi40", 100], // All of Nawawi 40
    ["bukhari", bukhariLimit],
    ["muslim", muslimLimit],
  ] as const) {
    const collection = collectionMap.get(slug);
    if (!collection) {
      console.log(`  Collection '${slug}' not found, skipping`);
      continue;
    }

    const books = await prisma.hadithBook.findMany({
      where: { collectionId: collection.id },
      select: { id: true, nameArabic: true, nameEnglish: true },
    });
    const bookIds = books.map((b) => b.id);
    const bookMap = new Map(books.map((b) => [b.id, b]));

    const hadiths = await prisma.hadith.findMany({
      where: {
        bookId: { in: bookIds },
        isChainVariation: false,
      },
      orderBy: [{ bookId: "asc" }, { hadithNumber: "asc" }],
      take: limit,
      select: {
        bookId: true,
        hadithNumber: true,
        textArabic: true,
        textPlain: true,
        chapterArabic: true,
        chapterEnglish: true,
      },
    });

    // Fetch translations for these hadiths
    const translationPairs = hadiths.map((h) => ({
      bookId: h.bookId,
      hadithNumber: h.hadithNumber,
    }));

    const translations = await prisma.hadithTranslation.findMany({
      where: {
        language: TRANSLATION_LANGUAGE,
        OR: translationPairs,
      },
      select: {
        bookId: true,
        hadithNumber: true,
        text: true,
      },
    });

    const translationMap = new Map<string, string>();
    for (const t of translations) {
      translationMap.set(`${t.bookId}:${t.hadithNumber}`, t.text);
    }

    for (const h of hadiths) {
      const book = bookMap.get(h.bookId)!;
      results.push({
        collectionSlug: slug,
        collectionNameArabic: collection.nameArabic,
        collectionNameEnglish: collection.nameEnglish,
        bookNameArabic: book.nameArabic,
        bookNameEnglish: book.nameEnglish,
        hadithNumber: h.hadithNumber,
        textArabic: h.textArabic,
        textPlain: h.textPlain,
        chapterArabic: h.chapterArabic,
        chapterEnglish: h.chapterEnglish,
        translationText:
          translationMap.get(`${h.bookId}:${h.hadithNumber}`) ?? null,
      });
    }

    console.log(`  ${slug}: loaded ${hadiths.length} hadiths`);
  }

  console.log(`Total hadiths loaded: ${results.length}`);
  return results;
}
