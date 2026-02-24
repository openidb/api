import { OpenAPIHono, createRoute } from "@hono/zod-openapi";
import { z } from "@hono/zod-openapi";
import { prisma } from "../db";
import { SOURCES } from "../utils/source-urls";
import { SourceSchema } from "../schemas/common";
import { getIndexedBookIds } from "../search/elasticsearch-catalog";
import { getPdfBookIds } from "./books";

const CACHE_TTL_MS = 5 * 60 * 1000;
const featuresCache = new Map<string, { data: unknown; expiry: number }>();

const BookFeaturesQuery = z.object({
  lang: z.string().max(20).optional().openapi({ example: "en", description: "Language code for isTranslated count" }),
  categoryId: z.string().optional().openapi({ example: "5", description: "Category ID(s), comma-separated" }),
  century: z.string().optional().openapi({ example: "3,7", description: "Hijri century(ies), comma-separated" }),
});

const BookFeaturesResponse = z.object({
  features: z.object({
    hasPdf: z.number(),
    isIndexed: z.number(),
    isTranslated: z.number(),
  }),
  _sources: z.array(SourceSchema),
}).openapi("BookFeatures");

const listFeatures = createRoute({
  method: "get",
  path: "/",
  tags: ["Books"],
  summary: "Get feature filter counts for books",
  request: { query: BookFeaturesQuery },
  responses: {
    200: {
      content: { "application/json": { schema: BookFeaturesResponse } },
      description: "Counts of books matching each feature filter",
    },
  },
});

export const bookFeaturesRoutes = new OpenAPIHono();

bookFeaturesRoutes.openapi(listFeatures, async (c) => {
  const { lang, categoryId, century } = c.req.valid("query");

  const cacheKey = `${lang || ""}:${categoryId || ""}:${century || ""}`;
  const cached = featuresCache.get(cacheKey);
  if (cached && Date.now() < cached.expiry) {
    c.header("Cache-Control", "public, max-age=300, stale-while-revalidate=600");
    return c.json(cached.data as any, 200);
  }

  // Build shared book-level filter conditions
  const bookConditions: string[] = [];
  const bookParams: unknown[] = [];
  let paramIdx = 1;

  if (categoryId) {
    const ids = categoryId.split(",").map(Number).filter((n) => Number.isFinite(n) && n > 0);
    if (ids.length === 1) {
      bookConditions.push(`b.category_id = $${paramIdx}`);
      bookParams.push(ids[0]);
      paramIdx++;
    } else if (ids.length > 1) {
      bookConditions.push(`b.category_id = ANY($${paramIdx})`);
      bookParams.push(ids);
      paramIdx++;
    }
  }

  if (century) {
    const centuries = century.split(",").map(Number).filter((n) => n >= 1 && n <= 15);
    if (centuries.length > 0) {
      bookConditions.push(`b.author_id IN (SELECT a.id FROM authors a WHERE a.death_century_hijri = ANY($${paramIdx}))`);
      bookParams.push(centuries);
      paramIdx++;
    }
  }

  const bookWhereSQL = bookConditions.length > 0 ? `WHERE ${bookConditions.join(" AND ")}` : "";

  // Run all 3 counts in parallel
  const [pdfCount, indexedCount, translatedCount] = await Promise.all([
    // hasPdf count — use cached set of PDF book IDs
    (async () => {
      const pdfIds = await getPdfBookIds();
      if (pdfIds.size === 0) return 0;

      if (bookConditions.length === 0) {
        return pdfIds.size;
      }

      const pdfParam = paramIdx;
      const rows = await prisma.$queryRawUnsafe<{ count: bigint }[]>(
        `SELECT COUNT(*)::bigint AS count FROM books b ${bookWhereSQL} AND b.id = ANY($${pdfParam})`,
        ...bookParams, [...pdfIds],
      );
      return Number(rows[0]?.count ?? 0);
    })(),

    // isIndexed count
    (async () => {
      const indexedIds = await getIndexedBookIds();
      if (indexedIds === null || indexedIds.size === 0) return 0;

      if (bookConditions.length === 0) {
        return indexedIds.size;
      }

      // Filter indexed IDs by book-level conditions
      const idxParam = paramIdx; // capture for this closure
      const rows = await prisma.$queryRawUnsafe<{ count: bigint }[]>(
        `SELECT COUNT(*)::bigint AS count FROM books b ${bookWhereSQL} AND b.id = ANY($${idxParam})`,
        ...bookParams, [...indexedIds],
      );
      return Number(rows[0]?.count ?? 0);
    })(),

    // isTranslated count
    (async () => {
      if (!lang || lang === "none" || lang === "transliteration") return 0;

      // Build a standalone query with fresh param indices
      const tConditions: string[] = [];
      const tParams: unknown[] = [];
      let tIdx = 1;

      if (categoryId) {
        const ids = categoryId.split(",").map(Number).filter((n) => Number.isFinite(n) && n > 0);
        if (ids.length === 1) {
          tConditions.push(`b2.category_id = $${tIdx}`);
          tParams.push(ids[0]);
          tIdx++;
        } else if (ids.length > 1) {
          tConditions.push(`b2.category_id = ANY($${tIdx})`);
          tParams.push(ids);
          tIdx++;
        }
      }

      if (century) {
        const centuries = century.split(",").map(Number).filter((n) => n >= 1 && n <= 15);
        if (centuries.length > 0) {
          tConditions.push(`b2.author_id IN (SELECT a.id FROM authors a WHERE a.death_century_hijri = ANY($${tIdx}))`);
          tParams.push(centuries);
          tIdx++;
        }
      }

      const bookFilter = tConditions.length > 0
        ? `AND p.book_id IN (SELECT b2.id FROM books b2 WHERE ${tConditions.join(" AND ")})`
        : "";

      const rows = await prisma.$queryRawUnsafe<{ count: bigint }[]>(
        `SELECT COUNT(*)::bigint AS count FROM (
          SELECT p.book_id
          FROM pages p
          LEFT JOIN page_translations pt ON pt.page_id = p.id AND pt.language = $${tIdx}
          WHERE p.page_number > 0 ${bookFilter}
          GROUP BY p.book_id
          HAVING COUNT(*) = COUNT(pt.id)
        ) t`,
        ...tParams, lang,
      );
      return Number(rows[0]?.count ?? 0);
    })(),
  ]);

  const result = {
    features: {
      hasPdf: pdfCount,
      isIndexed: indexedCount,
      isTranslated: translatedCount,
    },
    _sources: [...SOURCES.turath],
  };

  featuresCache.set(cacheKey, { data: result, expiry: Date.now() + CACHE_TTL_MS });

  c.header("Cache-Control", "public, max-age=300, stale-while-revalidate=600");
  return c.json(result, 200);
});
