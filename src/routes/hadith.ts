import { OpenAPIHono, createRoute } from "@hono/zod-openapi";
import { prisma } from "../db";
import { generateHadithSourceUrl, HADITHUNLOCKED_SLUGS, SOURCES } from "../utils/source-urls";
import { ErrorResponse } from "../schemas/common";
import {
  CollectionSlugParam, CollectionBookParam, HadithNumberParam,
  HadithBookQuery,
  CollectionListResponse, CollectionDetailResponse, HadithBookResponse, HadithDetailResponse,
} from "../schemas/hadith";

// --- Route definitions ---

const listCollections = createRoute({
  method: "get",
  path: "/collections",
  tags: ["Hadith"],
  summary: "List all hadith collections",
  responses: {
    200: {
      content: { "application/json": { schema: CollectionListResponse } },
      description: "List of hadith collections",
    },
  },
});

const getCollection = createRoute({
  method: "get",
  path: "/collections/{slug}",
  tags: ["Hadith"],
  summary: "Get collection with books",
  request: { params: CollectionSlugParam },
  responses: {
    200: {
      content: { "application/json": { schema: CollectionDetailResponse } },
      description: "Collection details with books",
    },
    404: {
      content: { "application/json": { schema: ErrorResponse } },
      description: "Collection not found",
    },
  },
});

const getHadithBook = createRoute({
  method: "get",
  path: "/collections/{slug}/books/{bookNumber}",
  tags: ["Hadith"],
  summary: "Get hadiths in a book",
  request: {
    params: CollectionBookParam,
    query: HadithBookQuery,
  },
  responses: {
    200: {
      content: { "application/json": { schema: HadithBookResponse } },
      description: "Book with hadiths",
    },
    404: {
      content: { "application/json": { schema: ErrorResponse } },
      description: "Book not found",
    },
  },
});

const getHadith = createRoute({
  method: "get",
  path: "/collections/{slug}/{number}",
  tags: ["Hadith"],
  summary: "Get hadith by number",
  request: { params: HadithNumberParam },
  responses: {
    200: {
      content: { "application/json": { schema: HadithDetailResponse } },
      description: "Hadith details",
    },
    404: {
      content: { "application/json": { schema: ErrorResponse } },
      description: "Hadith not found",
    },
  },
});

// --- Helpers ---

// Extra hadith fields to include in select queries (hadithunlocked.com + grading metadata)
const EXTRA_HADITH_FIELDS_SELECT = {
  source: true,
  numberInCollection: true,
  isnad: true,
  matn: true,
  gradeText: true,
  grade: true,
  gradeExplanation: true,
  graderName: true,
  sourceBookName: true,
} as const;

// Sunnah.com collection slugs
const SUNNAH_COM_SLUGS = new Set([
  "bukhari", "muslim", "abudawud", "tirmidhi", "nasai", "ibnmajah",
  "ahmad", "malik", "darimi", "riyadussalihin", "adab", "shamail",
  "mishkat", "bulugh", "nawawi40", "qudsi40", "hisn",
]);

// Alias for local use
const HADITH_UNLOCKED_SLUGS = HADITHUNLOCKED_SLUGS;

function isFromSunnah(slug: string): boolean {
  return SUNNAH_COM_SLUGS.has(slug);
}

function getSourcesForSlug(slug: string) {
  if (isFromSunnah(slug)) return [...SOURCES.sunnah];
  if (HADITH_UNLOCKED_SLUGS.has(slug)) return [...SOURCES.hadithUnlocked];
  return [...SOURCES.sunnah, ...SOURCES.hadithUnlocked];
}

function formatHadithForList(h: any, slug: string, bookNumber: number) {
  return {
    hadithNumber: h.hadithNumber,
    textArabic: h.textArabic,
    contentHash: h.contentHash,
    chapterArabic: h.chapterArabic,
    chapterEnglish: h.chapterEnglish,
    sourceUrl: generateHadithSourceUrl(slug, h.hadithNumber, bookNumber, h.numberInCollection),
    source: h.source ?? null,
    isnad: h.isnad ?? null,
    matn: h.matn ?? null,
    gradeText: h.gradeText ?? null,
    grade: h.grade ?? null,
    gradeExplanation: h.gradeExplanation ?? null,
    graderName: h.graderName ?? null,
    sourceBookName: h.sourceBookName ?? null,
  };
}

// --- Handlers ---

export const hadithRoutes = new OpenAPIHono();

hadithRoutes.openapi(listCollections, async (c) => {
  const collections = await prisma.hadithCollection.findMany({
    orderBy: { id: "asc" },
    select: {
      slug: true,
      nameEnglish: true,
      nameArabic: true,
      _count: { select: { books: true } },
    },
  });

  c.header("Cache-Control", "public, max-age=86400, stale-while-revalidate=86400");
  return c.json({
    collections: collections.map((col) => ({
      slug: col.slug,
      nameEnglish: col.nameEnglish,
      nameArabic: col.nameArabic,
      booksCount: col._count.books,
    })),
    _sources: [...SOURCES.sunnah, ...SOURCES.hadithUnlocked],
  }, 200);
});

hadithRoutes.openapi(getCollection, async (c) => {
  const { slug } = c.req.valid("param");

  const collection = await prisma.hadithCollection.findUnique({
    where: { slug },
    include: {
      books: {
        orderBy: { bookNumber: "asc" },
        select: {
          id: true,
          bookNumber: true,
          nameEnglish: true,
          nameArabic: true,
          _count: { select: { hadiths: true } },
        },
      },
    },
  });

  if (!collection) {
    return c.json({ error: "Collection not found" }, 404);
  }

  c.header("Cache-Control", "public, max-age=86400, stale-while-revalidate=86400");
  return c.json({
    collection: {
      slug: collection.slug,
      nameEnglish: collection.nameEnglish,
      nameArabic: collection.nameArabic,
      books: collection.books.map((book) => ({
        id: book.id,
        bookNumber: book.bookNumber,
        nameEnglish: book.nameEnglish,
        nameArabic: book.nameArabic,
        hadithCount: book._count.hadiths,
      })),
    },
    _sources: getSourcesForSlug(slug),
  }, 200);
});

hadithRoutes.openapi(getHadithBook, async (c) => {
  const { slug, bookNumber } = c.req.valid("param");
  const { limit, offset } = c.req.valid("query");

  const book = await prisma.hadithBook.findFirst({
    where: { collection: { slug }, bookNumber },
    select: {
      id: true,
      bookNumber: true,
      nameEnglish: true,
      nameArabic: true,
      collection: { select: { slug: true, nameEnglish: true, nameArabic: true } },
    },
  });

  if (!book) {
    return c.json({ error: "Book not found" }, 404);
  }

  const [hadiths, total] = await Promise.all([
    prisma.hadith.findMany({
      where: { bookId: book.id },
      orderBy: { id: "asc" },
      take: limit,
      skip: offset,
      select: {
        hadithNumber: true,
        textArabic: true,
        contentHash: true,
        chapterArabic: true,
        chapterEnglish: true,
        ...EXTRA_HADITH_FIELDS_SELECT,
      },
    }),
    prisma.hadith.count({ where: { bookId: book.id } }),
  ]);

  return c.json({
    book,
    hadiths: hadiths.map((h) => formatHadithForList(h, slug, bookNumber)),
    total,
    limit,
    offset,
    _sources: getSourcesForSlug(slug),
  }, 200);
});

hadithRoutes.openapi(getHadith, async (c) => {
  const { slug, number: hadithNumber } = c.req.valid("param");

  const hadith = await prisma.hadith.findFirst({
    where: {
      hadithNumber,
      book: { collection: { slug } },
    },
    select: {
      hadithNumber: true,
      textArabic: true,
      textPlain: true,
      contentHash: true,
      chapterArabic: true,
      chapterEnglish: true,
      ...EXTRA_HADITH_FIELDS_SELECT,
      book: {
        select: {
          bookNumber: true,
          nameEnglish: true,
          nameArabic: true,
          collection: { select: { slug: true, nameEnglish: true, nameArabic: true } },
        },
      },
    },
  });

  if (!hadith) {
    return c.json({ error: "Hadith not found" }, 404);
  }

  return c.json({
    hadith: {
      ...hadith,
      sourceUrl: generateHadithSourceUrl(slug, hadith.hadithNumber, hadith.book.bookNumber, hadith.numberInCollection),
    },
    _sources: getSourcesForSlug(slug),
  }, 200);
});
