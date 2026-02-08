import { Hono } from "hono";
import { prisma } from "../db";

export const authorsRoutes = new Hono();

// GET / — list authors (paginated, searchable)
authorsRoutes.get("/", async (c) => {
  const search = c.req.query("search");
  const limitParam = c.req.query("limit");
  const offsetParam = c.req.query("offset");

  const limit = Math.min(Math.max(parseInt(limitParam || "20", 10), 1), 100);
  const offset = Math.max(parseInt(offsetParam || "0", 10), 0);

  const where: Record<string, unknown> = {};
  if (search) {
    where.OR = [
      { nameArabic: { contains: search, mode: "insensitive" } },
      { nameLatin: { contains: search, mode: "insensitive" } },
    ];
  }

  const [authors, total] = await Promise.all([
    prisma.author.findMany({
      where,
      orderBy: { nameArabic: "asc" },
      take: limit,
      skip: offset,
      select: {
        id: true,
        nameArabic: true,
        nameLatin: true,
        deathDateHijri: true,
        deathDateGregorian: true,
        _count: { select: { books: true } },
      },
    }),
    prisma.author.count({ where }),
  ]);

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
  });
});

// GET /:id — get author by id
authorsRoutes.get("/:id", async (c) => {
  const id = c.req.param("id");

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
        },
      },
    },
  });

  if (!author) {
    return c.json({ error: "Author not found" }, 404);
  }

  return c.json({ author });
});
