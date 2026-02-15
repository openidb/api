import { z } from "@hono/zod-openapi";
import { PaginationQuery, SourceSchema } from "./common";

// --- Params ---

export const BookIdParam = z.object({
  id: z.string().max(50).openapi({ example: "123", description: "Book ID" }),
});

export const BookDetailQuery = z.object({
  bookTitleLang: z.string().max(20).optional().openapi({ example: "en", description: "Language code for book title translation" }),
});

export const BookPageParam = z.object({
  id: z.string().max(50).openapi({ example: "123" }),
  page: z.coerce.number().int().min(0).max(999999).openapi({ example: 0, description: "Page number (0-based)" }),
});

// --- Queries ---

export const BookListQuery = PaginationQuery.extend({
  limit: z.coerce.number().int().min(1).max(200).default(50).openapi({ example: 50 }),
  search: z.string().max(500).optional().openapi({ example: "الفقه" }),
  authorId: z.string().max(50).optional(),
  categoryId: z.string().optional().openapi({ example: "5", description: "Category ID(s), comma-separated" }),
  century: z.string().optional().openapi({ example: "3,7", description: "Hijri century(ies), comma-separated" }),
  bookTitleLang: z.string().max(20).optional().openapi({ example: "en", description: "Language code for book title translation" }),
});

export const BookPagesQuery = PaginationQuery.merge(z.object({
  limit: z.coerce.number().int().min(1).max(200).default(50).openapi({ example: 50 }),
}));

// --- Body ---

export const TranslateBody = z.object({
  lang: z.enum(["en", "fr", "id", "ur", "es", "zh", "pt", "ru", "ja", "ko", "it", "bn"]).default("en").openapi({ example: "en", description: "Target language code" }),
  model: z.enum(["gemini-flash", "gpt-oss-120b"]).default("gemini-flash").openapi({ example: "gemini-flash" }),
});

// --- Responses ---

export const BookSummary = z.object({
  id: z.string(),
  titleArabic: z.string(),
  titleLatin: z.string().nullable(),
  titleTranslated: z.string().nullable().optional(),
  filename: z.string(),
  totalVolumes: z.number(),
  totalPages: z.number().nullable(),
  publicationYearHijri: z.string().nullable(),
  publicationYearGregorian: z.string().nullable(),
  referenceUrl: z.string(),
  author: z.object({
    id: z.string(),
    nameArabic: z.string(),
    nameLatin: z.string().nullable(),
    deathDateHijri: z.string().nullable(),
    deathDateGregorian: z.string().nullable(),
  }).nullable(),
  category: z.object({
    id: z.number(),
    nameArabic: z.string(),
    nameEnglish: z.string().nullable(),
  }).nullable(),
});

export const BookListResponse = z.object({
  books: z.array(BookSummary),
  total: z.number(),
  limit: z.number(),
  offset: z.number(),
  _sources: z.array(SourceSchema),
}).openapi("BookList");

export const BookDetailResponse = z.object({
  book: z.object({
    id: z.string(),
    titleArabic: z.string(),
    titleLatin: z.string().nullable(),
    titleTranslated: z.string().nullable().optional(),
    filename: z.string(),
    totalVolumes: z.number(),
    totalPages: z.number().nullable(),
    publicationYearHijri: z.string().nullable(),
    publicationYearGregorian: z.string().nullable(),
    publicationEdition: z.string().nullable(),
    verificationStatus: z.string().nullable(),
    descriptionHtml: z.string().nullable(),
    summary: z.string().nullable(),
    referenceUrl: z.string(),
    author: z.object({
      id: z.string(),
      nameArabic: z.string(),
      nameLatin: z.string().nullable(),
      deathDateHijri: z.string().nullable(),
      deathDateGregorian: z.string().nullable(),
    }).nullable(),
    category: z.object({
      id: z.number(),
      nameArabic: z.string(),
      nameEnglish: z.string().nullable(),
    }).nullable(),
    publisher: z.object({
      name: z.string(),
      location: z.string().nullable(),
    }).nullable(),
    editor: z.object({ name: z.string() }).nullable(),
    keywords: z.array(z.object({ keyword: z.string() })),
  }),
  _sources: z.array(SourceSchema),
}).openapi("BookDetail");

export const PageSchema = z.object({
  pageNumber: z.number(),
  volumeNumber: z.number(),
  urlPageIndex: z.string().nullable(),
  printedPageNumber: z.number().nullable(),
  contentPlain: z.string(),
  contentHtml: z.string(),
  contentHash: z.string().nullable(),
  hasPoetry: z.boolean(),
  hasHadith: z.boolean(),
  hasQuran: z.boolean(),
  sourceUrl: z.string().nullable(),
  referenceUrl: z.string(),
});

export const PageDetailResponse = z.object({
  page: PageSchema,
  _sources: z.array(SourceSchema),
}).openapi("PageDetail");

export const PageListItem = z.object({
  pageNumber: z.number(),
  volumeNumber: z.number(),
  urlPageIndex: z.string().nullable(),
  printedPageNumber: z.number().nullable(),
});

export const PageListResponse = z.object({
  pages: z.array(PageListItem),
  total: z.number(),
  limit: z.number(),
  offset: z.number(),
  _sources: z.array(SourceSchema),
}).openapi("PageList");

export const TranslateResponse = z.object({
  paragraphs: z.array(z.object({
    index: z.number(),
    translation: z.string(),
  })),
  contentHash: z.string().nullable().optional(),
  cached: z.boolean().optional(),
  _sources: z.array(SourceSchema).optional(),
}).openapi("TranslateResponse");
