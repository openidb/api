import { Hono } from "hono";
import { prisma } from "../db";

export const categoriesRoutes = new Hono();

// GET / — get category tree
categoriesRoutes.get("/", async (c) => {
  const flat = c.req.query("flat") === "true";

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

  if (flat) {
    return c.json({
      categories: categories.map((cat) => ({
        id: cat.id,
        code: cat.code,
        nameArabic: cat.nameArabic,
        nameEnglish: cat.nameEnglish,
        parentId: cat.parentId,
        booksCount: cat._count.books,
      })),
    });
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

  return c.json({ categories: roots });
});

// GET /:id — get category with books
categoriesRoutes.get("/:id", async (c) => {
  const id = parseInt(c.req.param("id"), 10);
  if (isNaN(id)) {
    return c.json({ error: "Invalid category ID" }, 400);
  }

  const limitParam = c.req.query("limit");
  const offsetParam = c.req.query("offset");
  const limit = Math.min(Math.max(parseInt(limitParam || "20", 10), 1), 100);
  const offset = Math.max(parseInt(offsetParam || "0", 10), 0);

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

  return c.json({ category, books, total, limit, offset });
});
