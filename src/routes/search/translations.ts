import { prisma } from "../../db";
import { extractParagraphTexts, findMatchingParagraphIndex } from "./helpers";
import type { AyahResult, HadithResult, RankedResult } from "./types";

// Hadith slugs sourced from hadithunlocked.com (mirrors SLUG_TO_HADITHUNLOCKED_ALIAS in source-urls.ts)
const HADITH_UNLOCKED_SLUGS = new Set([
  "mustadrak", "ibn-hibban", "mujam-kabir",
  "sunan-kubra-bayhaqi", "sunan-kubra-nasai",
  "suyuti", "ahmad-zuhd",
]);

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
          select: { surahNumber: true, ayahNumber: true, text: true, editionId: true },
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
            model: true,
          },
        })
      : Promise.resolve([]),
  ]);

  // Look up edition metadata for ayah translations
  let editionMetadataMap = new Map<string, { name: string; source: string }>();
  if (ayahTranslations.length > 0) {
    const editionIds = [...new Set(ayahTranslations.map((t) => t.editionId))];
    const editions = await prisma.quranTranslation.findMany({
      where: { id: { in: editionIds } },
      select: { id: true, name: true, source: true },
    });
    editionMetadataMap = new Map(editions.map((e) => [e.id, { name: e.name, source: e.source }]));
  }

  // Merge ayah translations with edition metadata
  let ayahs = ayahsRaw;
  if (ayahTranslations.length > 0) {
    const translationMap = new Map(
      ayahTranslations.map((t) => [
        `${t.surahNumber}-${t.ayahNumber}`,
        { text: t.text, editionId: t.editionId },
      ])
    );
    ayahs = ayahsRaw.map((ayah) => {
      const match = translationMap.get(`${ayah.surahNumber}-${ayah.ayahNumber}`);
      if (!match) return ayah;
      const edition = editionMetadataMap.get(match.editionId);
      return {
        ...ayah,
        translation: match.text,
        translationEditionId: match.editionId,
        translationName: edition?.name,
        translationSource: edition?.source,
      };
    });
  }

  // Merge hadith translations with source attribution
  let mergedHadiths = hadiths;
  if (hadithTranslationsRaw.length > 0) {
    const hadithTranslationMap = new Map(
      hadithTranslationsRaw.map((t) => [`${t.bookId}-${t.hadithNumber}`, t.text])
    );
    mergedHadiths = hadiths.map((hadith) => ({
      ...hadith,
      translation: hadithTranslationMap.get(`${hadith.bookId}-${hadith.hadithNumber}`),
      translationSource: HADITH_UNLOCKED_SLUGS.has(hadith.collectionSlug)
        ? "hadithunlocked.com"
        : "sunnah.com",
    }));
  }

  // Merge book content translations with model info
  let mergedRanked = rankedResults;
  if (bookContentTranslationsRaw.length > 0) {
    type BookTranslationData = {
      paragraphs: Array<{ index: number; translation: string }>;
      contentHtml: string;
      model: string | null;
    };
    const bookContentTranslationMap = new Map<string, BookTranslationData>();
    for (const t of bookContentTranslationsRaw) {
      const key = `${t.page.bookId}-${t.page.pageNumber}`;
      bookContentTranslationMap.set(key, {
        paragraphs: t.paragraphs as Array<{ index: number; translation: string }>,
        contentHtml: t.page.contentHtml,
        model: t.model,
      });
    }

    mergedRanked = rankedResults.map((r) => {
      const translationData = bookContentTranslationMap.get(`${r.bookId}-${r.pageNumber}`);
      if (!translationData) return r;

      const { paragraphs: translations, contentHtml, model } = translationData;
      const pageParagraphs = extractParagraphTexts(contentHtml);
      const matchIndex = findMatchingParagraphIndex(r.textSnippet, pageParagraphs);
      const matchedTranslation = translations.find((t) => t.index === matchIndex);

      return {
        ...r,
        contentTranslation: matchedTranslation?.translation || null,
        contentTranslationModel: model || null,
      };
    });
  }

  return { ayahs, hadiths: mergedHadiths, rankedResults: mergedRanked };
}
