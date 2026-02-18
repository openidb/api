import { OpenAPIHono, createRoute } from "@hono/zod-openapi";
import { prisma } from "../db";
import { SOURCES } from "../utils/source-urls";
import { ErrorResponse } from "../schemas/common";
import {
  AuthorListQuery, AuthorIdParam, AuthorDetailQuery,
  AuthorListResponse, AuthorDetailResponse,
} from "../schemas/authors";
import { searchAuthorsES } from "../search/elasticsearch-catalog";

const listAuthors = createRoute({
  method: "get",
  path: "/",
  tags: ["Authors"],
  summary: "List authors (paginated, searchable)",
  request: { query: AuthorListQuery },
  responses: {
    200: {
      content: { "application/json": { schema: AuthorListResponse } },
      description: "Paginated list of authors",
    },
  },
});

const getAuthor = createRoute({
  method: "get",
  path: "/{id}",
  tags: ["Authors"],
  summary: "Get author by ID",
  request: { params: AuthorIdParam, query: AuthorDetailQuery },
  responses: {
    200: {
      content: { "application/json": { schema: AuthorDetailResponse } },
      description: "Author details with books",
    },
    404: {
      content: { "application/json": { schema: ErrorResponse } },
      description: "Author not found",
    },
  },
});

export const authorsRoutes = new OpenAPIHono();

authorsRoutes.openapi(listAuthors, async (c) => {
  const { limit, offset, search, century } = c.req.valid("query");

  // Try ES search when search param is present
  let esIds: string[] | null = null;
  if (search) {
    esIds = await searchAuthorsES(search, 1000);
    // esIds === null means ES unavailable → will fall back to ILIKE below
    // esIds === [] means ES found nothing → return empty
    if (esIds !== null && esIds.length === 0) {
      return c.json({ authors: [], total: 0, limit, offset, _sources: [...SOURCES.turath] }, 200);
    }
  }

  // Build raw WHERE clauses for numeric ID sorting (id is a string column)
  const conditions: string[] = [];
  const params: unknown[] = [];
  let paramIdx = 1;

  if (esIds !== null && esIds.length > 0) {
    // ES provided ranked IDs — filter to those IDs
    conditions.push(`a.id = ANY($${paramIdx})`);
    params.push(esIds);
    paramIdx++;
  } else if (search) {
    // ES unavailable — fall back to ILIKE
    conditions.push(`(a.name_arabic ILIKE $${paramIdx} OR a.name_latin ILIKE $${paramIdx})`);
    params.push(`%${search}%`);
    paramIdx++;
  }

  if (century) {
    const centuries = century.split(",").map(Number).filter((n) => n >= 1 && n <= 15);
    if (centuries.length > 0) {
      conditions.push(`a.death_century_hijri = ANY($${paramIdx}::int[])`);
      params.push(centuries);
      paramIdx++;
    }
  }

  const whereSQL = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

  // When ES provides ranked IDs, preserve ES relevance order; otherwise sort by numeric ID
  const useESOrder = esIds !== null && esIds.length > 0;
  const orderSQL = useESOrder
    ? `ORDER BY array_position($${paramIdx}::text[], a.id)`
    : `ORDER BY CAST(a.id AS INTEGER)`;
  const orderParams = useESOrder ? [esIds] : [];

  const [idRows, countRows] = await Promise.all([
    prisma.$queryRawUnsafe<{ id: string }[]>(
      `SELECT a.id FROM authors a ${whereSQL} ${orderSQL} LIMIT $${paramIdx + orderParams.length} OFFSET $${paramIdx + orderParams.length + 1}`,
      ...params, ...orderParams, limit, offset,
    ),
    prisma.$queryRawUnsafe<{ count: bigint }[]>(
      `SELECT COUNT(*)::bigint AS count FROM authors a ${whereSQL}`,
      ...params,
    ),
  ]);

  const orderedIds = idRows.map((r) => r.id);
  const total = Number(countRows[0]?.count ?? 0);

  const authors = orderedIds.length > 0
    ? await prisma.author.findMany({
        where: { id: { in: orderedIds } },
        select: {
          id: true,
          nameArabic: true,
          nameLatin: true,
          deathDateHijri: true,
          deathDateGregorian: true,
          _count: { select: { books: true } },
        },
      })
    : [];

  // Re-sort to match raw SQL numeric order
  const idOrder = new Map(orderedIds.map((id, i) => [id, i]));
  authors.sort((a, b) => (idOrder.get(a.id) ?? 0) - (idOrder.get(b.id) ?? 0));

  c.header("Cache-Control", "public, max-age=3600, stale-while-revalidate=86400");
  return c.json({
    authors: authors.map((a) => ({
      id: a.id,
      nameArabic: a.nameArabic,
      nameLatin: a.nameLatin,
      deathDateHijri: a.deathDateHijri,
      deathDateGregorian: a.deathDateGregorian,
      booksCount: a._count.books,
    })),
    total,
    limit,
    offset,
    _sources: [...SOURCES.turath],
  }, 200);
});

authorsRoutes.openapi(getAuthor, async (c) => {
  const { id } = c.req.valid("param");
  const { bookTitleLang } = c.req.valid("query");

  const author = await prisma.author.findUnique({
    where: { id },
    select: {
      id: true,
      nameArabic: true,
      nameLatin: true,
      kunya: true,
      nasab: true,
      nisba: true,
      laqab: true,
      birthDateHijri: true,
      deathDateHijri: true,
      birthDateGregorian: true,
      deathDateGregorian: true,
      biography: true,
      biographySource: true,
      books: {
        orderBy: { titleArabic: "asc" },
        select: {
          id: true,
          titleArabic: true,
          titleLatin: true,
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
      },
    },
  });

  if (!author) {
    return c.json({ error: "Author not found" }, 404);
  }

  c.header("Cache-Control", "public, max-age=86400, stale-while-revalidate=86400");
  return c.json({
    author: {
      ...author,
      books: author.books.map((b) => ({
        id: b.id,
        titleArabic: b.titleArabic,
        titleLatin: b.titleLatin,
        ...("titleTranslations" in b && Array.isArray(b.titleTranslations) && b.titleTranslations.length > 0
          ? { titleTranslated: (b.titleTranslations as { title: string }[])[0].title }
          : {}),
      })),
    },
    _sources: [...SOURCES.turath],
  }, 200);
});
