import { OpenAPIHono, createRoute } from "@hono/zod-openapi";
import { prisma } from "../db";
import { SOURCES } from "../utils/source-urls";
import { ErrorResponse } from "../schemas/common";
import {
  CategoryListQuery, CategoryIdParam, CategoryDetailQuery,
  CategoryListResponse, CategoryDetailResponse,
} from "../schemas/categories";

// --- Category list cache (5-minute TTL) ---
import { TTLCache } from "../lib/ttl-cache";
const categoryCache = new TTLCache<unknown>({ maxSize: 50, ttlMs: 5 * 60 * 1000, evictionCount: 10, label: "Category" });

const listCategories = createRoute({
  method: "get",
  path: "/",
  tags: ["Categories"],
  summary: "Get category list (tree or flat)",
  request: { query: CategoryListQuery },
  responses: {
    200: {
      content: { "application/json": { schema: CategoryListResponse } },
      description: "Category list",
    },
  },
});

const getCategory = createRoute({
  method: "get",
  path: "/{id}",
  tags: ["Categories"],
  summary: "Get category with books",
  request: {
    params: CategoryIdParam,
    query: CategoryDetailQuery,
  },
  responses: {
    200: {
      content: { "application/json": { schema: CategoryDetailResponse } },
      description: "Category details with books",
    },
    404: {
      content: { "application/json": { schema: ErrorResponse } },
      description: "Category not found",
    },
  },
});

export const categoriesRoutes = new OpenAPIHono();

categoriesRoutes.openapi(listCategories, async (c) => {
  const { flat, century } = c.req.valid("query");
  const centuryFilter = century
    ? century.split(",").map(Number).filter((n) => !isNaN(n))
    : [];

  // When century filter is active, skip cache and run filtered SQL
  if (centuryFilter.length > 0 && flat === "true") {
    const rows = await prisma.$queryRaw<
      { id: number; code: string | null; name_arabic: string; name_english: string | null; parent_id: number | null; books_count: number }[]
    >`
      SELECT c.id, c.code, c.name_arabic, c.name_english, c.parent_id,
             COALESCE(bc.cnt, 0)::int AS books_count
      FROM categories c
      LEFT JOIN (
        SELECT b.category_id, COUNT(*)::int AS cnt
        FROM books b
        JOIN authors a ON a.id = b.author_id
        WHERE a.death_century_hijri = ANY(${centuryFilter}::int[])
        GROUP BY b.category_id
      ) bc ON bc.category_id = c.id
      ORDER BY c.name_arabic
    `;

    const result = {
      categories: rows.map((r) => ({
        id: r.id,
        code: r.code,
        nameArabic: r.name_arabic,
        nameEnglish: r.name_english,
        parentId: r.parent_id,
        booksCount: Number(r.books_count),
      })),
      _sources: [...SOURCES.turath],
    };
    c.header("Cache-Control", "public, max-age=86400, stale-while-revalidate=86400");
    return c.json(result, 200);
  }

  const cacheKey = flat === "true" ? "flat" : "tree";

  const cached = categoryCache.get(cacheKey) as { categories: unknown; _sources: unknown } | null;
  if (cached) return c.json(cached, 200);

  const categories = await prisma.category.findMany({
    orderBy: { nameArabic: "asc" },
    select: {
      id: true,
      code: true,
      nameArabic: true,
      nameEnglish: true,
      parentId: true,
      _count: { select: { books: true } },
    },
  });

  if (flat === "true") {
    const result = {
      categories: categories.map((cat) => ({
        id: cat.id,
        code: cat.code,
        nameArabic: cat.nameArabic,
        nameEnglish: cat.nameEnglish,
        parentId: cat.parentId,
        booksCount: cat._count.books,
      })),
      _sources: [...SOURCES.turath],
    };
    categoryCache.set(cacheKey, result);
    c.header("Cache-Control", "public, max-age=86400, stale-while-revalidate=86400");
    return c.json(result, 200);
  }

  // Build tree structure
  type CategoryNode = {
    id: number;
    code: string | null;
    nameArabic: string;
    nameEnglish: string | null;
    booksCount: number;
    children: CategoryNode[];
  };

  const nodeMap = new Map<number, CategoryNode>();
  const roots: CategoryNode[] = [];

  for (const cat of categories) {
    nodeMap.set(cat.id, {
      id: cat.id,
      code: cat.code,
      nameArabic: cat.nameArabic,
      nameEnglish: cat.nameEnglish,
      booksCount: cat._count.books,
      children: [],
    });
  }

  for (const cat of categories) {
    const node = nodeMap.get(cat.id)!;
    if (cat.parentId && nodeMap.has(cat.parentId)) {
      nodeMap.get(cat.parentId)!.children.push(node);
    } else {
      roots.push(node);
    }
  }

  const result = {
    categories: roots,
    _sources: [...SOURCES.turath],
  };
  categoryCache.set(cacheKey, result);
  c.header("Cache-Control", "public, max-age=86400, stale-while-revalidate=86400");
  return c.json(result, 200);
});

categoriesRoutes.openapi(getCategory, async (c) => {
  const { id } = c.req.valid("param");
  const { limit, offset } = c.req.valid("query");

  const category = await prisma.category.findUnique({
    where: { id },
    select: {
      id: true,
      code: true,
      nameArabic: true,
      nameEnglish: true,
      parent: { select: { id: true, nameArabic: true } },
      children: { select: { id: true, nameArabic: true, nameEnglish: true } },
    },
  });

  if (!category) {
    return c.json({ error: "Category not found" }, 404);
  }

  const [books, total] = await Promise.all([
    prisma.book.findMany({
      where: { categoryId: id },
      orderBy: { titleArabic: "asc" },
      take: limit,
      skip: offset,
      select: {
        id: true,
        titleArabic: true,
        titleLatin: true,
        author: { select: { nameArabic: true, nameLatin: true } },
      },
    }),
    prisma.book.count({ where: { categoryId: id } }),
  ]);

  c.header("Cache-Control", "public, max-age=86400, stale-while-revalidate=86400");
  return c.json({
    category,
    books,
    total,
    limit,
    offset,
    _sources: [...SOURCES.turath],
  }, 200);
});
