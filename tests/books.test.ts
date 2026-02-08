import { describe, test, expect, beforeAll } from "bun:test";
import { get, expectOk, expectPagination, expectSources } from "./helpers/api";

let knownBookId: string;

beforeAll(async () => {
  const data = expectOk(await get("/api/books", { limit: 1 }));
  knownBookId = data.books[0].id;
});

describe("GET /api/books", () => {
  test("returns books with pagination and sources", async () => {
    const data = expectOk(await get("/api/books"));
    expect(data.books.length).toBeGreaterThan(0);
    expectPagination(data);
    expectSources(data);

    const book = data.books[0];
    expect(typeof book.id).toBe("string");
    expect(typeof book.titleArabic).toBe("string");
    expect(typeof book.titleLatin).toBe("string");
    expect(typeof book.filename).toBe("string");
    expect(typeof book.totalVolumes).toBe("number");
    expect(book.author).toBeDefined();
    expect(typeof book.author.id).toBe("string");
    expect(typeof book.shamelaUrl).toBe("string");
  });

  test("?search=تفسير returns results", async () => {
    const data = expectOk(await get("/api/books", { search: "تفسير" }));
    expect(data.books.length).toBeGreaterThan(0);
  });

  test("?limit=3 limits results", async () => {
    const data = expectOk(await get("/api/books", { limit: 3 }));
    expect(data.books.length).toBeLessThanOrEqual(3);
  });

  test("pagination totals are consistent", async () => {
    const data0 = expectOk(await get("/api/books", { limit: 1, offset: 0 }));
    const data1 = expectOk(await get("/api/books", { limit: 1, offset: 1 }));
    expect(data0.total).toBe(data1.total);
  });
});

describe("GET /api/books/:id", () => {
  test("known book returns full details", async () => {
    const data = expectOk(await get(`/api/books/${knownBookId}`));
    const book = data.book;
    expect(book.id).toBe(knownBookId);
    expect(typeof book.titleArabic).toBe("string");
    expect(typeof book.titleLatin).toBe("string");
    expect(book.author).toBeDefined();
    expect(book.category).toBeDefined();
    expect(typeof book.shamelaUrl).toBe("string");
    expect(Array.isArray(book.keywords)).toBe(true);
    expectSources(data);
  });

  test("nonexistent book returns 404", async () => {
    const res = await get("/api/books/nonexistent-book-id");
    expect(res.status).toBe(404);
  });
});

describe("GET /api/books/:id/pages", () => {
  test("returns pages with pagination", async () => {
    const data = expectOk(await get(`/api/books/${knownBookId}/pages`));
    expect(data.pages.length).toBeGreaterThan(0);
    expectPagination(data);
    expectSources(data);

    const page = data.pages[0];
    expect(typeof page.pageNumber).toBe("number");
    expect(typeof page.volumeNumber).toBe("number");
  });

  test("?limit=2 limits results", async () => {
    const data = expectOk(
      await get(`/api/books/${knownBookId}/pages`, { limit: 2 })
    );
    expect(data.pages.length).toBeLessThanOrEqual(2);
  });
});

describe("GET /api/books/:id/pages/:page", () => {
  test("page 1 returns full content", async () => {
    const data = expectOk(
      await get(`/api/books/${knownBookId}/pages/1`)
    );
    const page = data.page;
    expect(typeof page.pageNumber).toBe("number");
    expect(typeof page.volumeNumber).toBe("number");
    expect(typeof page.contentPlain).toBe("string");
    expect(typeof page.contentHtml).toBe("string");
    expect(typeof page.hasPoetry).toBe("boolean");
    expect(typeof page.hasHadith).toBe("boolean");
    expect(typeof page.hasQuran).toBe("boolean");
    expect(typeof page.shamelaUrl).toBe("string");
    expectSources(data);
  });

  test("page 999999 returns 404", async () => {
    const res = await get(`/api/books/${knownBookId}/pages/999999`);
    expect(res.status).toBe(404);
  });

  test("page 'abc' returns 400", async () => {
    const res = await get(`/api/books/${knownBookId}/pages/abc`);
    expect(res.status).toBe(400);
  });
});
