import { z } from "@hono/zod-openapi";
import { PaginationQuery, SourceSchema } from "./common";

// --- Params ---

export const SurahNumberParam = z.object({
  number: z.coerce.number().int().min(1).max(114).openapi({ example: 1, description: "Surah number (1-114)" }),
});

export const TafsirPathParam = z.object({
  surah: z.coerce.number().int().min(1).max(114).openapi({ example: 1 }),
  ayah: z.coerce.number().int().min(1).max(286).openapi({ example: 1 }),
});

export const TranslationPathParam = z.object({
  surah: z.coerce.number().int().min(1).max(114).openapi({ example: 1 }),
  ayah: z.coerce.number().int().min(1).max(286).openapi({ example: 1 }),
});

// --- Queries ---

export const AyahsQuery = PaginationQuery.merge(z.object({
  limit: z.coerce.number().int().min(1).max(500).default(50).openapi({ example: 50 }),
  surah: z.coerce.number().int().min(1).max(114).optional().openapi({ example: 1 }),
  juz: z.coerce.number().int().min(1).max(30).optional().openapi({ example: 1 }),
  page: z.coerce.number().int().min(1).max(2000).optional(),
}));

export const TafsirListQuery = z.object({
  language: z.string().max(10).optional().openapi({ example: "ar" }),
});

export const TafsirQuery = z.object({
  source: z.string().max(100).optional(),
  editionId: z.string().max(100).optional(),
  language: z.string().max(10).optional(),
});

export const TranslationListQuery = z.object({
  language: z.string().max(10).optional().openapi({ example: "en" }),
});

export const TranslationQuery = z.object({
  language: z.string().max(10).optional(),
  editionId: z.string().max(100).optional(),
});

// --- Responses ---

export const SurahSummary = z.object({
  number: z.number(),
  nameArabic: z.string(),
  nameEnglish: z.string(),
  revelationType: z.string(),
  ayahCount: z.number(),
});

export const SurahListResponse = z.object({
  surahs: z.array(SurahSummary),
  _sources: z.array(SourceSchema),
}).openapi("SurahList");

export const AyahSchema = z.object({
  ayahNumber: z.number(),
  textUthmani: z.string(),
  textPlain: z.string(),
  contentHash: z.string().nullable(),
  juzNumber: z.number().nullable(),
  pageNumber: z.number().nullable(),
  quranComUrl: z.string(),
});

export const SurahDetailResponse = z.object({
  surah: z.object({
    number: z.number(),
    nameArabic: z.string(),
    nameEnglish: z.string(),
    revelationType: z.string(),
    ayahCount: z.number(),
    ayahs: z.array(AyahSchema),
  }),
  _sources: z.array(SourceSchema),
}).openapi("SurahDetail");

export const AyahWithSurah = AyahSchema.extend({
  surah: z.object({
    number: z.number(),
    nameArabic: z.string(),
    nameEnglish: z.string(),
  }),
});

export const AyahListResponse = z.object({
  ayahs: z.array(AyahWithSurah),
  total: z.number(),
  limit: z.number(),
  offset: z.number(),
  _sources: z.array(SourceSchema),
}).openapi("AyahList");

export const TafsirEditionSchema = z.object({
  id: z.string(),
  name: z.string(),
  language: z.string(),
  author: z.string().nullable(),
  source: z.string(),
  direction: z.string(),
});

export const TafsirListResponse = z.object({
  tafsirs: z.array(TafsirEditionSchema),
  count: z.number(),
}).openapi("TafsirEditionList");

export const TafsirEntry = z.object({
  source: z.string().nullable(),
  editionId: z.string(),
  language: z.string(),
  text: z.string(),
  contentHash: z.string().nullable(),
  sourceUrl: z.string(),
});

export const TafsirResponse = z.object({
  surahNumber: z.number(),
  ayahNumber: z.number(),
  tafsirs: z.array(TafsirEntry),
  _sources: z.array(SourceSchema),
}).openapi("TafsirDetail");

export const TranslationEditionSchema = z.object({
  id: z.string(),
  name: z.string(),
  language: z.string(),
  author: z.string().nullable(),
  source: z.string(),
  direction: z.string(),
});

export const TranslationListResponse = z.object({
  translations: z.array(TranslationEditionSchema),
  count: z.number(),
}).openapi("TranslationEditionList");

export const TranslationEntry = z.object({
  language: z.string(),
  editionId: z.string(),
  text: z.string(),
  contentHash: z.string().nullable(),
  sourceUrl: z.string(),
});

export const TranslationResponse = z.object({
  surahNumber: z.number(),
  ayahNumber: z.number(),
  translations: z.array(TranslationEntry),
  _sources: z.array(SourceSchema),
}).openapi("TranslationDetail");
