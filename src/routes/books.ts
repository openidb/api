import { OpenAPIHono, createRoute } from "@hono/zod-openapi";
import { z } from "@hono/zod-openapi";
import { prisma } from "../db";
import { callOpenRouter } from "../lib/openrouter";
import { generateBookReferenceUrl, generatePageReferenceUrl, SOURCES } from "../utils/source-urls";
import { hashPageTranslation } from "../utils/content-hash";
import { detectPdfStorage } from "../utils/pdf-storage";
import { s3, BUCKET_NAME } from "../s3";
import { GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { ErrorResponse } from "../schemas/common";
import {
  BookIdParam, BookPageParam,
  BookListQuery, BookDetailQuery, BookPagesQuery, TranslateBody,
  BookListResponse, BookDetailResponse, PageDetailResponse, PageListResponse, TranslateResponse,
} from "../schemas/books";
import { searchBooksES } from "../search/elasticsearch-catalog";

const LANGUAGE_NAMES: Record<string, string> = {
  en: "English", fr: "French", id: "Indonesian", ur: "Urdu",
  es: "Spanish", zh: "Chinese", pt: "Portuguese", ru: "Russian",
  ja: "Japanese", ko: "Korean", it: "Italian", bn: "Bengali",
  ha: "Hausa", sw: "Swahili", nl: "Dutch", de: "German",
  tr: "Turkish", fa: "Persian", hi: "Hindi", ms: "Malay",
  pa: "Punjabi", ku: "Kurdish", ps: "Pashto", so: "Somali",
  uz: "Uzbek", yo: "Yoruba", ta: "Tamil",
};

const MODEL_MAP: Record<string, string> = {
  "gemini-flash": "google/gemini-3-flash-preview",
  "gpt-oss-120b": "openai/gpt-oss-120b",
};

const ARABIC_REGEX = /[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]/;

function isMeaningfulContent(text: string): boolean {
  if (text.length < 2) return false;
  if (!ARABIC_REGEX.test(text)) return false;
  if (/^[\s\d\-–—_.*•·,،؛:;!?'"()[\]{}«»<>\/\\|@#$%^&+=~`]+$/.test(text)) return false;
  return true;
}

function stripHtmlEntities(text: string): string {
  return text
    .replace(/<[^>]+>/g, "")
    .replace(/&nbsp;/g, " ").replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<").replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"').replace(/&#39;/g, "'")
    .replace(/\s+/g, " ").trim();
}

function extractParagraphs(html: string): { index: number; text: string }[] {
  const paragraphs: { index: number; text: string }[] = [];

  // Try <p> tag extraction first
  const pRegex = /<p[^>]*>([\s\S]*?)<\/p>/gi;
  let match;
  let index = 0;
  while ((match = pRegex.exec(html)) !== null) {
    const text = stripHtmlEntities(match[1]);
    if (isMeaningfulContent(text)) paragraphs.push({ index, text });
    index++;
  }
  if (paragraphs.length > 0) return paragraphs;

  // Fallback: newline splitting (Turath raw text format)
  // Join multi-line title spans into single lines (matches frontend formatContentHtml)
  html = html.replace(
    /<span\s+data-type=['"]title['"][^>]*>[\s\S]*?<\/span>/g,
    (m) => m.replace(/\n/g, " ")
  );
  const lines = html.split(/\n/);
  index = 0;
  for (const line of lines) {
    const text = stripHtmlEntities(line);
    if (isMeaningfulContent(text)) {
      paragraphs.push({ index, text });
    }
    index++;
  }
  return paragraphs;
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

const translatePage = createRoute({
  method: "post",
  path: "/{id}/pages/{page}/translate",
  tags: ["Books"],
  summary: "Translate page paragraphs",
  request: {
    params: BookPageParam,
    body: {
      content: { "application/json": { schema: TranslateBody } },
    },
  },
  responses: {
    200: {
      content: { "application/json": { schema: TranslateResponse } },
      description: "Translated paragraphs",
    },
    400: {
      content: { "application/json": { schema: ErrorResponse } },
      description: "Invalid request",
    },
    404: {
      content: { "application/json": { schema: ErrorResponse } },
      description: "Page not found",
    },
    502: {
      content: { "application/json": { schema: ErrorResponse } },
      description: "Translation service error",
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
  const { limit, offset, search, authorId, categoryId, century, bookTitleLang } = c.req.valid("query");

  // Resolve century filter to author IDs (shared by both ES and ILIKE paths)
  let centuryAuthorIds: string[] | null = null;
  if (century) {
    const centuries = century.split(",").map(Number).filter((n) => n >= 1 && n <= 15);
    if (centuries.length > 0) {
      const authorRows = await prisma.$queryRaw<{ id: string }[]>`
        SELECT id FROM authors
        WHERE death_date_hijri ~ '^[0-9]+$'
          AND CEIL(CAST(death_date_hijri AS DOUBLE PRECISION) / 100)::int = ANY(${centuries})
      `;
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

  const [book, lastPage] = await Promise.all([
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
  ]);

  if (!book) {
    return c.json({ error: "Book not found" }, 404);
  }

  const { titleTranslations, ...rest } = book as typeof book & {
    titleTranslations?: { title: string }[];
  };

  return c.json({
    book: {
      ...rest,
      titleTranslated: titleTranslations?.[0]?.title || null,
      maxPrintedPage: lastPage?.printedPageNumber ?? book.totalPages,
      displayDate: rest.author?.deathDateHijri || rest.publicationYearHijri || null,
      displayDateType: rest.author?.deathDateHijri ? "death" : rest.publicationYearHijri ? "publication" : null,
      referenceUrl: generateBookReferenceUrl(rest.id),
    },
    _sources: [...SOURCES.turath],
  }, 200);
});

booksRoutes.openapi(translatePage, async (c) => {
  const { id: bookId, page: pageNumber } = c.req.valid("param");
  const { lang, model: modelKey } = c.req.valid("json");

  const model = MODEL_MAP[modelKey] || MODEL_MAP["gemini-flash"];

  const targetLanguage = LANGUAGE_NAMES[lang];
  if (!targetLanguage) {
    return c.json({ error: "Unsupported language" }, 400);
  }

  const page = await prisma.page.findUnique({
    where: { bookId_pageNumber: { bookId, pageNumber } },
    select: { id: true, contentHtml: true, bookId: true, pageNumber: true },
  });

  if (!page) {
    return c.json({ error: "Page not found" }, 404);
  }

  // Check cache
  const existing = await prisma.pageTranslation.findUnique({
    where: { pageId_language: { pageId: page.id, language: lang } },
    select: { paragraphs: true, contentHash: true, model: true },
  });
  if (existing) {
    const cachedModel = MODEL_MAP[existing.model ?? ""] ?? existing.model ?? "unknown";
    return c.json({
      paragraphs: existing.paragraphs as any,
      contentHash: existing.contentHash,
      cached: true,
      _sources: [{ name: `LLM Translation (${cachedModel})`, url: "https://openrouter.ai", type: "llm" }],
    }, 200);
  }

  const paragraphs = extractParagraphs(page.contentHtml);
  if (paragraphs.length === 0) {
    return c.json({ paragraphs: [] }, 200);
  }

  const numberedParagraphs = paragraphs.map((p) => `[${p.index}] ${p.text}`).join("\n\n");

  const prompt = `Translate the following Arabic paragraphs to ${targetLanguage}.
Each paragraph is numbered with [N]. Return a JSON array where each element has "index" (the paragraph number) and "translation" (the translated text).
Only translate the text content - do not include the original Arabic or the [N] markers in the translation.
Preserve the meaning and tone of the original text.

IMPORTANT — Use the FULL PAGE as context when translating each paragraph. All paragraphs come from the same page of an Islamic scholarly text. If a word or phrase is ambiguous, use the surrounding paragraphs to disambiguate and determine the correct meaning. Always prefer the interpretation that is consistent with the rest of the page.

IMPORTANT — Preserve Islamic terminology in their conventional English/transliterated forms:
- Surah names: keep the standard transliteration (e.g. al-Baqarah, al-Qasas, al-Anfal, Yasin) — do NOT translate surah names into their literal meanings
- "الله" → "Allah" (not "God")
- "محمد" → "Muhammad" or "the Prophet Muhammad"
- "القرآن" → "Quran" (not "the holy book")
- "الرسول" → "the Messenger" or "the Prophet"
- "صلى الله عليه وسلم" → "peace be upon him" or "ﷺ"
- Other Islamic terms like: Salah, Zakat, Hajj, Iman, Taqwa, Sunnah, Hadith, Fiqh, Tafsir, Ijma, Qiyas, etc.

Arabic paragraphs:
${numberedParagraphs}

Respond with ONLY a valid JSON array, no other text. Example format:
[{"index": 0, "translation": "First paragraph translation"}, {"index": 1, "translation": "Second paragraph translation"}]`;

  const result = await callOpenRouter({ model, messages: [{ role: "user", content: prompt }], temperature: 0.3, timeoutMs: 30000 });
  if (!result) {
    return c.json({ error: "Translation service unavailable" }, 502);
  }

  let translations: { index: number; translation: string }[] = [];
  try {
    let cleaned = result.content.trim();
    if (cleaned.startsWith("```json")) cleaned = cleaned.slice(7);
    else if (cleaned.startsWith("```")) cleaned = cleaned.slice(3);
    if (cleaned.endsWith("```")) cleaned = cleaned.slice(0, -3);
    const parsed = JSON.parse(cleaned.trim());
    if (!Array.isArray(parsed)) {
      return c.json({ error: "Failed to parse translation" }, 400);
    }
    for (const item of parsed.slice(0, paragraphs.length)) {
      if (
        typeof item?.index === "number" && Number.isFinite(item.index) &&
        typeof item?.translation === "string"
      ) {
        translations.push({ index: item.index, translation: item.translation.slice(0, 5000) });
      }
    }
    if (translations.length === 0) {
      return c.json({ error: "Failed to parse translation" }, 400);
    }
  } catch {
    return c.json({ error: "Failed to parse translation" }, 400);
  }

  const contentHash = hashPageTranslation(page.bookId, page.pageNumber, lang, translations);
  await prisma.pageTranslation.create({
    data: { pageId: page.id, language: lang, model: modelKey, paragraphs: translations, contentHash },
  });

  return c.json({
    paragraphs: translations,
    contentHash,
    cached: false,
    _sources: [{ name: `LLM Translation (${model})`, url: "https://openrouter.ai", type: "llm" }],
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
    return c.json({ url: storage.url, type: "external" as const, _sources: [...SOURCES.turath] }, 200);
  }

  const expiresIn = 3600;
  const command = new GetObjectCommand({ Bucket: BUCKET_NAME, Key: storage.key });
  const url = await getSignedUrl(s3, command, { expiresIn });

  return c.json({ url, type: "rustfs" as const, expiresIn, _sources: [...SOURCES.turath] }, 200);
});
