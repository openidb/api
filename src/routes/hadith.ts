import { Hono } from "hono";
import { prisma } from "../db";
import { generateSunnahComUrl } from "../utils/source-urls";

export const hadithRoutes = new Hono();

// GET /collections — list all collections
hadithRoutes.get("/collections", async (c) => {
  const collections = await prisma.hadithCollection.findMany({
    orderBy: { id: "asc" },
    select: {
      slug: true,
      nameEnglish: true,
      nameArabic: true,
      _count: { select: { books: true } },
    },
  });

  return c.json({
    collections: collections.map((col) => ({
      slug: col.slug,
      nameEnglish: col.nameEnglish,
      nameArabic: col.nameArabic,
      booksCount: col._count.books,
    })),
    _sources: [{ name: "sunnah.com", url: "https://sunnah.com", type: "scrape" }],
  });
});

// GET /collections/:slug — get collection with books
hadithRoutes.get("/collections/:slug", async (c) => {
  const slug = c.req.param("slug");

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
  });
});

// GET /collections/:slug/books/:bookNumber — get hadiths in a book
hadithRoutes.get("/collections/:slug/books/:bookNumber", async (c) => {
  const slug = c.req.param("slug");
  const bookNumber = parseInt(c.req.param("bookNumber"), 10);
  const limitParam = c.req.query("limit");
  const offsetParam = c.req.query("offset");

  const limit = Math.min(Math.max(parseInt(limitParam || "50", 10), 1), 200);
  const offset = Math.max(parseInt(offsetParam || "0", 10), 0);

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
        chapterArabic: true,
        chapterEnglish: true,
      },
    }),
    prisma.hadith.count({ where: { bookId: book.id } }),
  ]);

  return c.json({
    book,
    hadiths: hadiths.map((h) => ({
      ...h,
      sunnahComUrl: generateSunnahComUrl(slug, h.hadithNumber, bookNumber),
    })),
    total,
    limit,
    offset,
  });
});

// GET /collections/:slug/:number — get hadith by number
hadithRoutes.get("/collections/:slug/:number", async (c) => {
  const slug = c.req.param("slug");
  const hadithNumber = c.req.param("number");

  const hadith = await prisma.hadith.findFirst({
    where: {
      hadithNumber,
      book: { collection: { slug } },
    },
    select: {
      hadithNumber: true,
      textArabic: true,
      textPlain: true,
      chapterArabic: true,
      chapterEnglish: true,
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
      sunnahComUrl: generateSunnahComUrl(slug, hadith.hadithNumber, hadith.book.bookNumber),
    },
  });
});
