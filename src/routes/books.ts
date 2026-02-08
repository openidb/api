import { Hono } from "hono";
import { prisma } from "../db";
import { generateShamelaBookUrl, generateShamelaPageUrl } from "../utils/source-urls";
import { parsePagination } from "../utils/pagination";

export const booksRoutes = new Hono();

// GET / — list books (paginated, searchable)
booksRoutes.get("/", async (c) => {
  const search = c.req.query("search");
  const authorId = c.req.query("authorId");
  const categoryId = c.req.query("categoryId");
  const { limit, offset } = parsePagination(c.req.query("limit"), c.req.query("offset"));

  const where: Record<string, unknown> = {};
  if (search) {
    where.OR = [
      { titleArabic: { contains: search, mode: "insensitive" } },
      { titleLatin: { contains: search, mode: "insensitive" } },
    ];
  }
  if (authorId) where.authorId = authorId;
  if (categoryId) where.categoryId = parseInt(categoryId, 10);

  const [books, total] = await Promise.all([
    prisma.book.findMany({
      where,
      orderBy: { titleArabic: "asc" },
      take: limit,
      skip: offset,
      select: {
        id: true,
        titleArabic: true,
        titleLatin: true,
        filename: true,
        totalVolumes: true,
        totalPages: true,
        publicationYearHijri: true,
        author: {
          select: { id: true, nameArabic: true, nameLatin: true },
        },
        category: {
          select: { id: true, nameArabic: true, nameEnglish: true },
        },
      },
    }),
    prisma.book.count({ where }),
  ]);

  return c.json({
    books: books.map((b) => ({
      ...b,
      shamelaUrl: generateShamelaBookUrl(b.id),
    })),
    total,
    limit,
    offset,
    _sources: [{ name: "Maktaba Shamela", url: "https://shamela.ws", type: "backup" }],
  });
});

// GET /:id — get book by id
booksRoutes.get("/:id", async (c) => {
  const id = c.req.param("id");

  const book = await prisma.book.findUnique({
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
      author: {
        select: {
          id: true,
          nameArabic: true,
          nameLatin: true,
          deathDateHijri: true,
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
    },
  });

  if (!book) {
    return c.json({ error: "Book not found" }, 404);
  }

  return c.json({
    book: {
      ...book,
      shamelaUrl: generateShamelaBookUrl(book.id),
    },
    _sources: [{ name: "Maktaba Shamela", url: "https://shamela.ws", type: "backup" }],
  });
});

// GET /:id/pages/:page — get book page
booksRoutes.get("/:id/pages/:page", async (c) => {
  const bookId = c.req.param("id");
  const pageNumber = parseInt(c.req.param("page"), 10);

  if (isNaN(pageNumber)) {
    return c.json({ error: "Invalid page number" }, 400);
  }

  const page = await prisma.page.findUnique({
    where: { bookId_pageNumber: { bookId, pageNumber } },
    select: {
      pageNumber: true,
      volumeNumber: true,
      urlPageIndex: true,
      printedPageNumber: true,
      contentPlain: true,
      contentHtml: true,
      hasPoetry: true,
      hasHadith: true,
      hasQuran: true,
      sourceUrl: true,
    },
  });

  if (!page) {
    return c.json({ error: "Page not found" }, 404);
  }

  return c.json({
    page: {
      ...page,
      shamelaUrl: generateShamelaPageUrl(bookId, pageNumber),
    },
    _sources: [{ name: "Maktaba Shamela", url: "https://shamela.ws", type: "backup" }],
  });
});

// GET /:id/pages — list pages for a book (metadata only)
booksRoutes.get("/:id/pages", async (c) => {
  const bookId = c.req.param("id");
  const { limit, offset } = parsePagination(c.req.query("limit"), c.req.query("offset"), 50, 200);

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
    _sources: [{ name: "Maktaba Shamela", url: "https://shamela.ws", type: "backup" }],
  });
});
