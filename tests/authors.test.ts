import { describe, test, expect, beforeAll } from "bun:test";
import { get, expectOk, expectPagination, expectSources } from "./helpers/api";

let knownAuthorId: string;

beforeAll(async () => {
  const data = expectOk(await get("/api/books/authors", { limit: 1 }));
  knownAuthorId = data.authors[0].id;
});

describe("GET /api/books/authors", () => {
  test("returns authors with pagination and sources", async () => {
    const data = expectOk(await get("/api/books/authors"));
    expect(data.authors.length).toBeGreaterThan(0);
    expectPagination(data);
    expectSources(data);

    const author = data.authors[0];
    expect(typeof author.id).toBe("string");
    expect(typeof author.nameArabic).toBe("string");
    expect(typeof author.nameLatin).toBe("string");
    expect(typeof author.booksCount).toBe("number");
  });

  test("?search=ابن returns results", async () => {
    const data = expectOk(
      await get("/api/books/authors", { search: "ابن" })
    );
    expect(data.authors.length).toBeGreaterThan(0);
  });
});

describe("GET /api/books/authors/:id", () => {
  test("known author returns full details with books", async () => {
    const data = expectOk(await get(`/api/books/authors/${knownAuthorId}`));
    const author = data.author;
    expect(author.id).toBe(knownAuthorId);
    expect(typeof author.nameArabic).toBe("string");
    expect(typeof author.nameLatin).toBe("string");
    expect(Array.isArray(author.books)).toBe(true);
    expectSources(data);

    if (author.books.length > 0) {
      const book = author.books[0];
      expect(typeof book.id).toBe("string");
      expect(typeof book.titleArabic).toBe("string");
      expect(typeof book.titleLatin).toBe("string");
    }
  });

  test("nonexistent author returns 404", async () => {
    const res = await get("/api/books/authors/nonexistent-author-id");
    expect(res.status).toBe(404);
  });
});
