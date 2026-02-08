import { describe, test, expect } from "bun:test";
import { get, expectOk, expectPagination, expectSources } from "./helpers/api";

describe("GET /api/hadith/collections", () => {
  test("returns collections including bukhari", async () => {
    const data = expectOk(await get("/api/hadith/collections"));
    expect(data.collections.length).toBeGreaterThanOrEqual(17);
    expectSources(data);

    const col = data.collections[0];
    expect(typeof col.slug).toBe("string");
    expect(typeof col.nameEnglish).toBe("string");
    expect(typeof col.nameArabic).toBe("string");
    expect(typeof col.booksCount).toBe("number");

    const bukhari = data.collections.find(
      (c: any) => c.slug === "bukhari"
    );
    expect(bukhari).toBeDefined();
  });
});

describe("GET /api/hadith/collections/:slug", () => {
  test("bukhari returns collection with books", async () => {
    const data = expectOk(await get("/api/hadith/collections/bukhari"));
    expect(data.collection.slug).toBe("bukhari");
    expect(data.collection.books.length).toBeGreaterThan(0);
    expectSources(data);

    const book = data.collection.books[0];
    expect(typeof book.id).toBe("number");
    expect(typeof book.bookNumber).toBe("number");
    expect(typeof book.nameEnglish).toBe("string");
    expect(typeof book.nameArabic).toBe("string");
    expect(typeof book.hadithCount).toBe("number");
  });

  test("nonexistent collection returns 404", async () => {
    const res = await get("/api/hadith/collections/nonexistent");
    expect(res.status).toBe(404);
  });
});

describe("GET /api/hadith/collections/:slug/books/:bookNumber", () => {
  test("bukhari book 1 returns hadiths with pagination", async () => {
    const data = expectOk(
      await get("/api/hadith/collections/bukhari/books/1")
    );
    expect(data.hadiths.length).toBeGreaterThan(0);
    expectPagination(data);
    expectSources(data);

    const h = data.hadiths[0];
    expect(typeof h.hadithNumber).toBe("string");
    expect(typeof h.textArabic).toBe("string");
    expect(typeof h.sunnahUrl).toBe("string");
  });

  test("?limit=5 limits results", async () => {
    const data = expectOk(
      await get("/api/hadith/collections/bukhari/books/1", { limit: 5 })
    );
    expect(data.hadiths.length).toBeLessThanOrEqual(5);
  });

  test("nonexistent book returns 404", async () => {
    const res = await get("/api/hadith/collections/bukhari/books/99999");
    expect(res.status).toBe(404);
  });
});

describe("GET /api/hadith/collections/:slug/:number", () => {
  test("bukhari hadith 1 returns full hadith", async () => {
    const data = expectOk(await get("/api/hadith/collections/bukhari/1"));
    expect(data.hadith).toBeDefined();
    expectSources(data);

    const h = data.hadith;
    expect(typeof h.hadithNumber).toBe("string");
    expect(typeof h.textArabic).toBe("string");
    expect(h.book).toBeDefined();
    expect(typeof h.book.bookNumber).toBe("number");
    expect(typeof h.book.nameEnglish).toBe("string");
    expect(typeof h.book.nameArabic).toBe("string");
    expect(h.book.collection).toBeDefined();
    expect(typeof h.sunnahUrl).toBe("string");
  });

  test("nonexistent hadith returns 404", async () => {
    const res = await get("/api/hadith/collections/bukhari/999999");
    expect(res.status).toBe(404);
  });
});
