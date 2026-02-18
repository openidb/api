import { z } from "@hono/zod-openapi";
import { PaginationQuery, SourceSchema } from "./common";

// --- Params ---

export const CollectionSlugParam = z.object({
  slug: z.string().max(50).openapi({ example: "bukhari", description: "Collection slug" }),
});

export const CollectionBookParam = z.object({
  slug: z.string().max(50).openapi({ example: "bukhari" }),
  bookNumber: z.coerce.number().int().min(1).openapi({ example: 1, description: "Book number within the collection" }),
});

export const HadithNumberParam = z.object({
  slug: z.string().max(50).openapi({ example: "bukhari" }),
  number: z.string().max(20).openapi({ example: "1", description: "Hadith number" }),
});

// --- Queries ---

export const HadithBookQuery = PaginationQuery.merge(z.object({
  limit: z.coerce.number().int().min(1).max(200).default(50).openapi({ example: 50 }),
}));

// --- Responses ---

export const CollectionSummary = z.object({
  slug: z.string(),
  nameEnglish: z.string(),
  nameArabic: z.string(),
  booksCount: z.number(),
});

export const CollectionListResponse = z.object({
  collections: z.array(CollectionSummary),
  _sources: z.array(SourceSchema),
}).openapi("HadithCollectionList");

export const CollectionBookSummary = z.object({
  id: z.number(),
  bookNumber: z.number(),
  nameEnglish: z.string().nullable(),
  nameArabic: z.string().nullable(),
  hadithCount: z.number(),
});

export const CollectionDetailResponse = z.object({
  collection: z.object({
    slug: z.string(),
    nameEnglish: z.string(),
    nameArabic: z.string(),
    books: z.array(CollectionBookSummary),
  }),
  _sources: z.array(SourceSchema),
}).openapi("HadithCollectionDetail");

export const HadithSchema = z.object({
  hadithNumber: z.string(),
  textArabic: z.string(),
  contentHash: z.string().nullable(),
  chapterArabic: z.string().nullable(),
  chapterEnglish: z.string().nullable(),
  sourceUrl: z.string().nullable().optional(),
  source: z.string().nullable().optional(),
  // Isnad/matn separation (hadithunlocked.com)
  isnad: z.string().nullable().optional(),
  matn: z.string().nullable().optional(),
  gradeText: z.string().nullable().optional(),
  // Grading & metadata
  grade: z.string().nullable().optional(),
  gradeExplanation: z.string().nullable().optional(),
  graderName: z.string().nullable().optional(),
  sourceBookName: z.string().nullable().optional(),
});

export const HadithBookResponse = z.object({
  book: z.object({
    id: z.number(),
    bookNumber: z.number(),
    nameEnglish: z.string().nullable(),
    nameArabic: z.string().nullable(),
    collection: z.object({
      slug: z.string(),
      nameEnglish: z.string(),
      nameArabic: z.string(),
    }),
  }),
  hadiths: z.array(HadithSchema),
  total: z.number(),
  limit: z.number(),
  offset: z.number(),
  _sources: z.array(SourceSchema),
}).openapi("HadithBookDetail");

export const HadithDetailResponse = z.object({
  hadith: z.object({
    hadithNumber: z.string(),
    textArabic: z.string(),
    textPlain: z.string().nullable(),
    contentHash: z.string().nullable(),
    chapterArabic: z.string().nullable(),
    chapterEnglish: z.string().nullable(),
    sourceUrl: z.string().nullable().optional(),
    source: z.string().nullable().optional(),
    // Isnad/matn separation (hadithunlocked.com)
    isnad: z.string().nullable().optional(),
    matn: z.string().nullable().optional(),
    gradeText: z.string().nullable().optional(),
    // Grading & metadata
    grade: z.string().nullable().optional(),
    gradeExplanation: z.string().nullable().optional(),
    graderName: z.string().nullable().optional(),
    sourceBookName: z.string().nullable().optional(),
    book: z.object({
      bookNumber: z.number(),
      nameEnglish: z.string().nullable(),
      nameArabic: z.string().nullable(),
      collection: z.object({
        slug: z.string(),
        nameEnglish: z.string(),
        nameArabic: z.string(),
      }),
    }),
  }),
  _sources: z.array(SourceSchema),
}).openapi("HadithDetail");
