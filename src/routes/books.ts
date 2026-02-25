import { OpenAPIHono, createRoute } from "@hono/zod-openapi";
import { z } from "@hono/zod-openapi";
import { prisma } from "../db";
import { generateBookReferenceUrl, generatePageReferenceUrl, SOURCES } from "../utils/source-urls";
import { detectPdfStorage } from "../utils/pdf-storage";
import { s3, BUCKET_NAME } from "../s3";
import { GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { ErrorResponse } from "../schemas/common";
import {
  BookIdParam, BookPageParam,
  BookListQuery, BookDetailQuery, BookPagesQuery, PageTranslationQuery,
  BookListResponse, BookDetailResponse, PageDetailResponse, PageListResponse, TranslateResponse,
} from "../schemas/books";
import { searchBooksES, getIndexedBookIds } from "../search/elasticsearch-catalog";

// Cached set of book IDs that have at least one PDF page (5-min TTL)
let pdfBookIdsCache: { ids: Set<string>; expiry: number } | null = null;

export async function getPdfBookIds(): Promise<Set<string>> {
  if (pdfBookIdsCache && Date.now() < pdfBookIdsCache.expiry) {
    return pdfBookIdsCache.ids;
  }
  const rows = await prisma.$queryRawUnsafe<{ book_id: string }[]>(
    `SELECT DISTINCT book_id FROM pages WHERE pdf_url IS NOT NULL AND pdf_url != ''`
  );
  const ids = new Set(rows.map((r) => r.book_id));
  pdfBookIdsCache = { ids, expiry: Date.now() + 5 * 60 * 1000 };
  return ids;
}

// --- Route definitions ---

const listBooks = createRoute({
  method: "get",
  path: "/",
  tags: ["Books"],
  summary: "List books (paginated, searchable)",
  request: { query: BookListQuery },
  responses: {
    200: {
      content: { "application/json": { schema: BookListResponse } },
      description: "Paginated list of books",
    },
  },
});

const getBook = createRoute({
  method: "get",
  path: "/{id}",
  tags: ["Books"],
  summary: "Get book by ID",
  request: { params: BookIdParam, query: BookDetailQuery },
  responses: {
    200: {
      content: { "application/json": { schema: BookDetailResponse } },
      description: "Book details",
    },
    404: {
      content: { "application/json": { schema: ErrorResponse } },
      description: "Book not found",
    },
  },
});

const getPageTranslation = createRoute({
  method: "get",
  path: "/{id}/pages/{page}/translation",
  tags: ["Books"],
  summary: "Get pre-existing page translation",
  request: { params: BookPageParam, query: PageTranslationQuery },
  responses: {
    200: {
      content: { "application/json": { schema: TranslateResponse } },
      description: "Translated paragraphs",
    },
    404: {
      content: { "application/json": { schema: ErrorResponse } },
      description: "Translation not found",
    },
  },
});

const getPage = createRoute({
  method: "get",
  path: "/{id}/pages/{page}",
  tags: ["Books"],
  summary: "Get book page",
  request: { params: BookPageParam },
  responses: {
    200: {
      content: { "application/json": { schema: PageDetailResponse } },
      description: "Page content",
    },
    404: {
      content: { "application/json": { schema: ErrorResponse } },
      description: "Page not found",
    },
  },
});

const listPages = createRoute({
  method: "get",
  path: "/{id}/pages",
  tags: ["Books"],
  summary: "List pages for a book (metadata only)",
  request: {
    params: BookIdParam,
    query: BookPagesQuery,
  },
  responses: {
    200: {
      content: { "application/json": { schema: PageListResponse } },
      description: "Paginated list of pages",
    },
  },
});

const PdfUrlResponse = z.object({
  url: z.string(),
  type: z.enum(["rustfs", "external"]),
  expiresIn: z.number().optional(),
  _sources: z.array(z.object({ name: z.string(), url: z.string(), type: z.string() })),
}).openapi("PdfUrl");

const getPagePdf = createRoute({
  method: "get",
  path: "/{id}/pages/{page}/pdf",
  tags: ["Books"],
  summary: "Get PDF URL for a book page",
  request: { params: BookPageParam },
  responses: {
    200: {
      content: { "application/json": { schema: PdfUrlResponse } },
      description: "PDF URL (presigned for RustFS, direct for external)",
    },
    404: {
      content: { "application/json": { schema: ErrorResponse } },
      description: "Page not found or no PDF available",
    },
  },
});

// --- Handlers ---

export const booksRoutes = new OpenAPIHono();

booksRoutes.openapi(listBooks, async (c) => {
  const { limit, offset, search, authorId, categoryId, century, bookTitleLang, hasPdf, isIndexed, isTranslated } = c.req.valid("query");

  // Resolve century filter to author IDs (shared by both ES and ILIKE paths)
  let centuryAuthorIds: string[] | null = null;
  if (century) {
    const centuries = century.split(",").map(Number).filter((n) => n >= 1 && n <= 15);
    if (centuries.length > 0) {
      const authorRows = await prisma.author.findMany({
        where: { deathCenturyHijri: { in: centuries } },
        select: { id: true },
      });
      centuryAuthorIds = authorRows.map((a) => a.id);
      if (centuryAuthorIds.length === 0) {
        // No authors match these centuries — return empty immediately
        return c.json({ books: [], total: 0, limit, offset, _sources: [...SOURCES.turath] }, 200);
      }
    }
  }

  // Try ES search when search param is present
  let esIds: string[] | null = null;
  if (search) {
    esIds = await searchBooksES(search, 1000);
    // esIds === null means ES unavailable → will fall back to ILIKE below
    // esIds === [] means ES found nothing → return empty
    if (esIds !== null && esIds.length === 0) {
      return c.json({ books: [], total: 0, limit, offset, _sources: [...SOURCES.turath] }, 200);
    }
  }

  // Build raw WHERE clauses
  const conditions: string[] = [];
  const params: unknown[] = [];
  let paramIdx = 1;

  if (esIds !== null && esIds.length > 0) {
    // ES provided ranked IDs — filter to those IDs
    conditions.push(`b.id = ANY($${paramIdx})`);
    params.push(esIds);
    paramIdx++;
  } else if (search) {
    // ES unavailable — fall back to ILIKE
    conditions.push(`(b.title_arabic ILIKE $${paramIdx} OR b.title_latin ILIKE $${paramIdx})`);
    params.push(`%${search}%`);
    paramIdx++;
  }

  if (authorId) {
    if (centuryAuthorIds) {
      // Both authorId and century — intersect
      const intersection = centuryAuthorIds.includes(authorId) ? [authorId] : [];
      if (intersection.length === 0) {
        return c.json({ books: [], total: 0, limit, offset, _sources: [...SOURCES.turath] }, 200);
      }
      conditions.push(`b.author_id = $${paramIdx}`);
      params.push(authorId);
      paramIdx++;
    } else {
      conditions.push(`b.author_id = $${paramIdx}`);
      params.push(authorId);
      paramIdx++;
    }
  } else if (centuryAuthorIds) {
    conditions.push(`b.author_id = ANY($${paramIdx})`);
    params.push(centuryAuthorIds);
    paramIdx++;
  }

  if (categoryId) {
    const ids = categoryId.split(",").map(Number).filter((n) => Number.isFinite(n) && n > 0);
    if (ids.length === 1) {
      conditions.push(`b.category_id = $${paramIdx}`);
      params.push(ids[0]);
      paramIdx++;
    } else if (ids.length > 1) {
      conditions.push(`b.category_id = ANY($${paramIdx})`);
      params.push(ids);
      paramIdx++;
    }
  }

  // Feature filters
  if (hasPdf === "true") {
    const pdfIds = await getPdfBookIds();
    if (pdfIds.size === 0) {
      return c.json({ books: [], total: 0, limit, offset, _sources: [...SOURCES.turath] }, 200);
    }
    conditions.push(`b.id = ANY($${paramIdx})`);
    params.push([...pdfIds]);
    paramIdx++;
  }

  if (isIndexed === "true") {
    const indexedIds = await getIndexedBookIds();
    if (indexedIds !== null) {
      if (indexedIds.size === 0) {
        return c.json({ books: [], total: 0, limit, offset, _sources: [...SOURCES.turath] }, 200);
      }
      conditions.push(`b.id = ANY($${paramIdx})`);
      params.push([...indexedIds]);
      paramIdx++;
    }
    // If ES unavailable (null), skip filter silently
  }

  if (isTranslated === "true" && bookTitleLang && bookTitleLang !== "none" && bookTitleLang !== "transliteration") {
    conditions.push(`b.id IN (
      SELECT p.book_id FROM pages p
      LEFT JOIN page_translations pt ON pt.page_id = p.id AND pt.language = $${paramIdx}
      WHERE p.page_number > 0
      GROUP BY p.book_id
      HAVING COUNT(*) = COUNT(pt.id)
    )`);
    params.push(bookTitleLang);
    paramIdx++;
  }

  const whereSQL = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

  // When ES provides ranked IDs, preserve ES relevance order; otherwise sort by numeric ID
  const useESOrder = esIds !== null && esIds.length > 0;
  const orderSQL = useESOrder
    ? `ORDER BY array_position($${paramIdx}::text[], b.id)`
    : `ORDER BY CAST(b.id AS INTEGER)`;
  const orderParams = useESOrder ? [esIds] : [];

  const [idRows, countRows] = await Promise.all([
    prisma.$queryRawUnsafe<{ id: string }[]>(
      `SELECT b.id FROM books b ${whereSQL} ${orderSQL} LIMIT $${paramIdx + orderParams.length} OFFSET $${paramIdx + orderParams.length + 1}`,
      ...params, ...orderParams, limit, offset,
    ),
    prisma.$queryRawUnsafe<{ count: bigint }[]>(
      `SELECT COUNT(*)::bigint AS count FROM books b ${whereSQL}`,
      ...params,
    ),
  ]);

  const orderedIds = idRows.map((r) => r.id);
  const total = Number(countRows[0]?.count ?? 0);

  const books = orderedIds.length > 0
    ? await prisma.book.findMany({
        where: { id: { in: orderedIds } },
        select: {
          id: true,
          titleArabic: true,
          titleLatin: true,
          filename: true,
          totalVolumes: true,
          totalPages: true,
          publicationYearHijri: true,
          publicationYearGregorian: true,
          author: {
            select: { id: true, nameArabic: true, nameLatin: true, deathDateHijri: true, deathDateGregorian: true },
          },
          category: {
            select: { id: true, nameArabic: true, nameEnglish: true },
          },
          ...(bookTitleLang && bookTitleLang !== "none" && bookTitleLang !== "transliteration"
            ? {
                titleTranslations: {
                  where: { language: bookTitleLang },
                  select: { title: true },
                  take: 1,
                },
              }
            : {}),
        },
      })
    : [];

  // Re-sort to match the raw SQL numeric order
  const idOrder = new Map(orderedIds.map((id, i) => [id, i]));
  books.sort((a, b) => (idOrder.get(a.id) ?? 0) - (idOrder.get(b.id) ?? 0));

  c.header("Cache-Control", "public, max-age=3600, stale-while-revalidate=86400");
  return c.json({
    books: books.map((b) => {
      const { titleTranslations, ...rest } = b as typeof b & {
        titleTranslations?: { title: string }[];
      };
      return {
        ...rest,
        titleTranslated: titleTranslations?.[0]?.title || null,
        displayDate: rest.author?.deathDateHijri || rest.publicationYearHijri || null,
        displayDateType: rest.author?.deathDateHijri ? "death" : rest.publicationYearHijri ? "publication" : null,
        referenceUrl: generateBookReferenceUrl(rest.id),
      };
    }),
    total,
    limit,
    offset,
    _sources: [...SOURCES.turath],
  }, 200);
});

booksRoutes.openapi(getBook, async (c) => {
  const { id } = c.req.valid("param");
  const { bookTitleLang } = c.req.valid("query");

  const [book, lastPage, volumeStarts, translatedLangs] = await Promise.all([
    prisma.book.findUnique({
      where: { id },
      select: {
        id: true,
        titleArabic: true,
        titleLatin: true,
        filename: true,
        totalVolumes: true,
        totalPages: true,
        publicationYearHijri: true,
        publicationYearGregorian: true,
        publicationEdition: true,
        verificationStatus: true,
        descriptionHtml: true,
        summary: true,
        tableOfContents: true,
        author: {
          select: {
            id: true,
            nameArabic: true,
            nameLatin: true,
            deathDateHijri: true,
            deathDateGregorian: true,
          },
        },
        category: {
          select: { id: true, nameArabic: true, nameEnglish: true },
        },
        publisher: {
          select: { name: true, location: true },
        },
        editor: {
          select: { name: true },
        },
        keywords: {
          select: { keyword: true },
        },
        ...(bookTitleLang && bookTitleLang !== "none" && bookTitleLang !== "transliteration"
          ? {
              titleTranslations: {
                where: { language: bookTitleLang },
                select: { title: true },
                take: 1,
              },
            }
          : {}),
      },
    }),
    prisma.page.findFirst({
      where: { bookId: id, printedPageNumber: { not: null } },
      orderBy: { printedPageNumber: "desc" },
      select: { printedPageNumber: true },
    }),
    prisma.page.groupBy({
      by: ["volumeNumber"],
      where: { bookId: id },
      _min: { pageNumber: true },
      orderBy: { volumeNumber: "asc" },
    }),
    prisma.$queryRawUnsafe<{ language: string }[]>(
      `SELECT pt.language FROM page_translations pt
       JOIN pages p ON pt.page_id = p.id
       WHERE p.book_id = $1 AND p.page_number > 0
       GROUP BY pt.language
       HAVING COUNT(DISTINCT p.id) = (SELECT COUNT(*) FROM pages WHERE book_id = $1 AND page_number > 0)`,
      id,
    ),
  ]);

  if (!book) {
    return c.json({ error: "Book not found" }, 404);
  }

  const { titleTranslations, ...rest } = book as typeof book & {
    titleTranslations?: { title: string }[];
  };

  c.header("Cache-Control", "public, max-age=86400, stale-while-revalidate=86400");
  return c.json({
    book: {
      ...rest,
      titleTranslated: titleTranslations?.[0]?.title || null,
      maxPrintedPage: lastPage?.printedPageNumber ?? book.totalPages,
      volumeStartPages: Object.fromEntries(
        volumeStarts
          .filter((v) => v.volumeNumber > 0)
          .map((v) => [String(v.volumeNumber), v._min.pageNumber!])
      ),
      displayDate: rest.author?.deathDateHijri || rest.publicationYearHijri || null,
      displayDateType: rest.author?.deathDateHijri ? "death" : rest.publicationYearHijri ? "publication" : null,
      referenceUrl: generateBookReferenceUrl(rest.id),
      ...(translatedLangs.length > 0 ? { translatedLanguages: translatedLangs.map((r) => r.language) } : {}),
    },
    _sources: [...SOURCES.turath],
  }, 200);
});

booksRoutes.openapi(getPageTranslation, async (c) => {
  const { id: bookId, page: pageNumber } = c.req.valid("param");
  const { lang } = c.req.valid("query");

  const page = await prisma.page.findUnique({
    where: { bookId_pageNumber: { bookId, pageNumber } },
    select: { id: true },
  });

  if (!page) {
    return c.json({ error: "Page not found" }, 404);
  }

  const translation = await prisma.pageTranslation.findUnique({
    where: { pageId_language: { pageId: page.id, language: lang } },
    select: { paragraphs: true, contentHash: true },
  });

  if (!translation) {
    return c.json({ error: "Translation not found" }, 404);
  }

  c.header("Cache-Control", "public, max-age=86400, stale-while-revalidate=86400, immutable");
  return c.json({
    paragraphs: translation.paragraphs as { index: number; translation: string }[],
    contentHash: translation.contentHash,
  }, 200);
});

booksRoutes.openapi(getPage, async (c) => {
  const { id: bookId, page: pageNumber } = c.req.valid("param");

  const page = await prisma.page.findUnique({
    where: { bookId_pageNumber: { bookId, pageNumber } },
    select: {
      pageNumber: true,
      volumeNumber: true,
      urlPageIndex: true,
      printedPageNumber: true,
      contentPlain: true,
      contentHtml: true,
      contentHash: true,
      hasPoetry: true,
      hasHadith: true,
      hasQuran: true,
      sourceUrl: true,
      pdfUrl: true,
    },
  });

  if (!page) {
    return c.json({ error: "Page not found" }, 404);
  }

  c.header("Cache-Control", "public, max-age=86400, stale-while-revalidate=86400, immutable");
  return c.json({
    page: {
      ...page,
      referenceUrl: generatePageReferenceUrl(bookId, pageNumber),
    },
    _sources: [...SOURCES.turath],
  }, 200);
});

booksRoutes.openapi(listPages, async (c) => {
  const { id: bookId } = c.req.valid("param");
  const { limit, offset } = c.req.valid("query");

  const [pages, total] = await Promise.all([
    prisma.page.findMany({
      where: { bookId },
      orderBy: { pageNumber: "asc" },
      take: limit,
      skip: offset,
      select: {
        pageNumber: true,
        volumeNumber: true,
        urlPageIndex: true,
        printedPageNumber: true,
      },
    }),
    prisma.page.count({ where: { bookId } }),
  ]);

  return c.json({
    pages,
    total,
    limit,
    offset,
    _sources: [...SOURCES.turath],
  }, 200);
});

booksRoutes.openapi(getPagePdf, async (c) => {
  const { id: bookId, page: pageNumber } = c.req.valid("param");

  const page = await prisma.page.findUnique({
    where: { bookId_pageNumber: { bookId, pageNumber } },
    select: { pdfUrl: true },
  });

  if (!page) {
    return c.json({ error: "Page not found" }, 404);
  }

  let storage = detectPdfStorage(page.pdfUrl);

  // Overview page (page 0) is synthetic and has no PDF — fall back to page 1
  if (storage.type === "none" && pageNumber === 0) {
    const firstPage = await prisma.page.findUnique({
      where: { bookId_pageNumber: { bookId, pageNumber: 1 } },
      select: { pdfUrl: true },
    });
    if (firstPage) {
      storage = detectPdfStorage(firstPage.pdfUrl);
    }
  }

  if (storage.type === "none") {
    return c.json({ error: "No PDF available for this page" }, 404);
  }

  if (storage.type === "external") {
    return c.redirect(storage.url, 302);
  }

  // Stream PDF from RustFS through the API (presigned URLs use internal hostnames)
  const command = new GetObjectCommand({ Bucket: BUCKET_NAME, Key: storage.key });
  const presignedUrl = await getSignedUrl(s3, command, { expiresIn: 3600 });
  const pdfRes = await fetch(presignedUrl);

  if (!pdfRes.ok || !pdfRes.body) {
    return c.json({ error: "PDF not found in storage" }, 404);
  }

  c.header("Content-Type", "application/pdf");
  c.header("Cache-Control", "public, max-age=86400, immutable");
  const contentLength = pdfRes.headers.get("content-length");
  if (contentLength) c.header("Content-Length", contentLength);

  return new Response(pdfRes.body as any, { status: 200 });
});
