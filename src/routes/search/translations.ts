import { prisma } from "../../db";
import { extractParagraphTexts, findMatchingParagraphIndex } from "./helpers";
import type { AyahResult, HadithResult, RankedResult } from "./types";

export async function fetchAndMergeTranslations(
  params: {
    quranTranslation: string;
    hadithTranslation: string;
    bookContentTranslation: string;
  },
  rankedResults: RankedResult[],
  ayahsRaw: AyahResult[],
  hadiths: HadithResult[]
): Promise<{
  ayahs: AyahResult[];
  hadiths: HadithResult[];
  rankedResults: RankedResult[];
}> {
  const { quranTranslation, hadithTranslation, bookContentTranslation } = params;

  // Detect if quranTranslation is an edition ID (contains '-') or language code
  const isEditionId = quranTranslation !== "none" && quranTranslation.includes("-");
  const quranTransWhere = isEditionId
    ? { editionId: quranTranslation }
    : { language: quranTranslation };

  const [ayahTranslations, hadithTranslationsRaw, bookContentTranslationsRaw] = await Promise.all([
    (quranTranslation !== "none" && ayahsRaw.length > 0)
      ? prisma.ayahTranslation.findMany({
          where: {
            ...quranTransWhere,
            OR: ayahsRaw.map((a) => ({ surahNumber: a.surahNumber, ayahNumber: a.ayahNumber })),
          },
          select: { surahNumber: true, ayahNumber: true, text: true },
        })
      : Promise.resolve([]),
    (hadithTranslation !== "none" && hadiths.length > 0)
      ? prisma.hadithTranslation.findMany({
          where: {
            language: hadithTranslation,
            OR: hadiths.map((h) => ({ bookId: h.bookId, hadithNumber: h.hadithNumber })),
          },
          select: { bookId: true, hadithNumber: true, text: true },
        })
      : Promise.resolve([]),
    (bookContentTranslation !== "none" && rankedResults.length > 0)
      ? prisma.pageTranslation.findMany({
          where: {
            language: bookContentTranslation,
            page: {
              OR: rankedResults.map((r) => ({ bookId: r.bookId, pageNumber: r.pageNumber })),
            },
          },
          select: {
            page: { select: { bookId: true, pageNumber: true, contentHtml: true } },
            paragraphs: true,
          },
        })
      : Promise.resolve([]),
  ]);

  // Merge ayah translations
  let ayahs = ayahsRaw;
  if (ayahTranslations.length > 0) {
    const translationMap = new Map(
      ayahTranslations.map((t) => [`${t.surahNumber}-${t.ayahNumber}`, t.text])
    );
    ayahs = ayahsRaw.map((ayah) => ({
      ...ayah,
      translation: translationMap.get(`${ayah.surahNumber}-${ayah.ayahNumber}`),
    }));
  }

  // Merge hadith translations
  let mergedHadiths = hadiths;
  if (hadithTranslationsRaw.length > 0) {
    const hadithTranslationMap = new Map(
      hadithTranslationsRaw.map((t) => [`${t.bookId}-${t.hadithNumber}`, t.text])
    );
    mergedHadiths = hadiths.map((hadith) => ({
      ...hadith,
      translation: hadithTranslationMap.get(`${hadith.bookId}-${hadith.hadithNumber}`),
    }));
  }

  // Merge book content translations
  let mergedRanked = rankedResults;
  if (bookContentTranslationsRaw.length > 0) {
    type BookTranslationData = {
      paragraphs: Array<{ index: number; translation: string }>;
      contentHtml: string;
    };
    const bookContentTranslationMap = new Map<string, BookTranslationData>();
    for (const t of bookContentTranslationsRaw) {
      const key = `${t.page.bookId}-${t.page.pageNumber}`;
      bookContentTranslationMap.set(key, {
        paragraphs: t.paragraphs as Array<{ index: number; translation: string }>,
        contentHtml: t.page.contentHtml,
      });
    }

    mergedRanked = rankedResults.map((r) => {
      const translationData = bookContentTranslationMap.get(`${r.bookId}-${r.pageNumber}`);
      if (!translationData) return r;

      const { paragraphs: translations, contentHtml } = translationData;
      const pageParagraphs = extractParagraphTexts(contentHtml);
      const matchIndex = findMatchingParagraphIndex(r.textSnippet, pageParagraphs);
      const matchedTranslation = translations.find((t) => t.index === matchIndex);

      return { ...r, contentTranslation: matchedTranslation?.translation || null };
    });
  }

  return { ayahs, hadiths: mergedHadiths, rankedResults: mergedRanked };
}
