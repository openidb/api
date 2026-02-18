import { OpenAPIHono, createRoute } from "@hono/zod-openapi";
import { z } from "@hono/zod-openapi";
import { prisma } from "../db";
import { SOURCES } from "../utils/source-urls";
import { SourceSchema } from "../schemas/common";

// --- 5-minute in-memory cache ---
const CACHE_TTL_MS = 5 * 60 * 1000;
let centuryCache: { data: unknown; expiry: number } | null = null;
let authorCenturyCache: { data: unknown; expiry: number } | null = null;

const CenturyItem = z.object({
  century: z.number(),
  booksCount: z.number(),
});

const CenturyListResponse = z.object({
  centuries: z.array(CenturyItem),
  _sources: z.array(SourceSchema),
}).openapi("CenturyList");

const AuthorCenturyItem = z.object({
  century: z.number(),
  authorsCount: z.number(),
});

const AuthorCenturyListResponse = z.object({
  centuries: z.array(AuthorCenturyItem),
  _sources: z.array(SourceSchema),
}).openapi("AuthorCenturyList");

const CenturyListQuery = z.object({
  categoryId: z.string().optional().openapi({ example: "1,5", description: "Filter counts by category ID (comma-separated)" }),
});

const listCenturies = createRoute({
  method: "get",
  path: "/",
  tags: ["Books"],
  summary: "Get centuries with book counts",
  request: { query: CenturyListQuery },
  responses: {
    200: {
      content: { "application/json": { schema: CenturyListResponse } },
      description: "List of centuries with book counts",
    },
  },
});

export const centuriesRoutes = new OpenAPIHono();

centuriesRoutes.openapi(listCenturies, async (c) => {
  const { categoryId } = c.req.valid("query");
  const categoryFilter = categoryId
    ? categoryId.split(",").map(Number).filter((n) => !isNaN(n))
    : [];

  // Unfiltered path — use cache
  if (categoryFilter.length === 0) {
    if (centuryCache && Date.now() < centuryCache.expiry) {
      return c.json(centuryCache.data as any, 200);
    }

    const rows = await prisma.$queryRaw<{ century: number; books_count: bigint }[]>`
      SELECT
        a.death_century_hijri AS century,
        COUNT(b.id)::bigint AS books_count
      FROM authors a
      JOIN books b ON b.author_id = a.id
      WHERE a.death_century_hijri IS NOT NULL
      GROUP BY a.death_century_hijri
      ORDER BY a.death_century_hijri
    `;

    const centuries = rows.map((r) => ({
      century: Number(r.century),
      booksCount: Number(r.books_count),
    }));

    const result = {
      centuries,
      _sources: [...SOURCES.turath],
    };

    centuryCache = { data: result, expiry: Date.now() + CACHE_TTL_MS };
    c.header("Cache-Control", "public, max-age=86400, stale-while-revalidate=86400");
    return c.json(result, 200);
  }

  // Filtered path — get filtered counts, then merge with full list
  const [filteredRows, fullResult] = await Promise.all([
    prisma.$queryRaw<{ century: number; books_count: bigint }[]>`
      SELECT
        a.death_century_hijri AS century,
        COUNT(b.id)::bigint AS books_count
      FROM authors a
      JOIN books b ON b.author_id = a.id
      WHERE a.death_century_hijri IS NOT NULL
        AND b.category_id = ANY(${categoryFilter}::int[])
      GROUP BY a.death_century_hijri
      ORDER BY a.death_century_hijri
    `,
    // Get full unfiltered list (from cache or fresh)
    (async () => {
      if (centuryCache && Date.now() < centuryCache.expiry) {
        return centuryCache.data as { centuries: { century: number; booksCount: number }[] };
      }
      const rows = await prisma.$queryRaw<{ century: number; books_count: bigint }[]>`
        SELECT
          a.death_century_hijri AS century,
          COUNT(b.id)::bigint AS books_count
        FROM authors a
        JOIN books b ON b.author_id = a.id
        WHERE a.death_century_hijri IS NOT NULL
        GROUP BY a.death_century_hijri
        ORDER BY a.death_century_hijri
      `;
      const centuries = rows.map((r) => ({
        century: Number(r.century),
        booksCount: Number(r.books_count),
      }));
      const result = { centuries, _sources: [...SOURCES.turath] };
      centuryCache = { data: result, expiry: Date.now() + CACHE_TTL_MS };
      return result;
    })(),
  ]);

  const filteredMap = new Map(
    filteredRows.map((r) => [Number(r.century), Number(r.books_count)])
  );

  const centuries = fullResult.centuries.map((c) => ({
    century: c.century,
    booksCount: filteredMap.get(c.century) ?? 0,
  }));

  c.header("Cache-Control", "public, max-age=86400, stale-while-revalidate=86400");
  return c.json({
    centuries,
    _sources: [...SOURCES.turath],
  }, 200);
});

const listAuthorCenturies = createRoute({
  method: "get",
  path: "/authors",
  tags: ["Authors"],
  summary: "Get centuries with author counts",
  responses: {
    200: {
      content: { "application/json": { schema: AuthorCenturyListResponse } },
      description: "List of centuries with author counts",
    },
  },
});

centuriesRoutes.openapi(listAuthorCenturies, async (c) => {
  if (authorCenturyCache && Date.now() < authorCenturyCache.expiry) {
    return c.json(authorCenturyCache.data as any, 200);
  }

  const rows = await prisma.$queryRaw<{ century: number; authors_count: bigint }[]>`
    SELECT
      death_century_hijri AS century,
      COUNT(*)::bigint AS authors_count
    FROM authors
    WHERE death_century_hijri IS NOT NULL
    GROUP BY death_century_hijri
    ORDER BY death_century_hijri
  `;

  const centuries = rows.map((r) => ({
    century: Number(r.century),
    authorsCount: Number(r.authors_count),
  }));

  const result = {
    centuries,
    _sources: [...SOURCES.turath],
  };

  authorCenturyCache = { data: result, expiry: Date.now() + CACHE_TTL_MS };
  c.header("Cache-Control", "public, max-age=86400, stale-while-revalidate=86400");
  return c.json(result, 200);
});
