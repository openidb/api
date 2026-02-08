import { describe, test, expect, beforeAll } from "bun:test";
import { get, expectOk, expectPagination, expectSources } from "./helpers/api";

let knownCategoryId: number;

beforeAll(async () => {
  const data = expectOk(await get("/api/books/categories"));
  knownCategoryId = data.categories[0].id;
});

describe("GET /api/books/categories", () => {
  test("default returns tree structure with children", async () => {
    const data = expectOk(await get("/api/books/categories"));
    expect(data.categories.length).toBeGreaterThan(0);
    expectSources(data);

    const root = data.categories[0];
    expect(typeof root.id).toBe("number");
    expect(typeof root.nameArabic).toBe("string");
    expect(typeof root.booksCount).toBe("number");
    expect(Array.isArray(root.children)).toBe(true);
  });

  test("?flat=true returns flat list without children", async () => {
    const flat = expectOk(
      await get("/api/books/categories", { flat: "true" })
    );
    expect(flat.categories.length).toBeGreaterThan(0);
    expectSources(flat);

    const cat = flat.categories[0];
    expect(typeof cat.id).toBe("number");
    expect(typeof cat.nameArabic).toBe("string");
    expect(cat.children).toBeUndefined();

    // Flat count should be >= tree root count
    const tree = expectOk(await get("/api/books/categories"));
    expect(flat.categories.length).toBeGreaterThanOrEqual(
      tree.categories.length
    );
  });
});

describe("GET /api/books/categories/:id", () => {
  test("known category returns details with books", async () => {
    const data = expectOk(
      await get(`/api/books/categories/${knownCategoryId}`)
    );
    expect(data.category).toBeDefined();
    expect(data.category.id).toBe(knownCategoryId);
    expect(typeof data.category.nameArabic).toBe("string");
    expect(Array.isArray(data.category.children)).toBe(true);
    expect(Array.isArray(data.books)).toBe(true);
    expectPagination(data);
    expectSources(data);
  });

  test("nonexistent category returns 404", async () => {
    const res = await get("/api/books/categories/999999");
    expect(res.status).toBe(404);
  });

  test("invalid category id returns 400", async () => {
    const res = await get("/api/books/categories/abc");
    expect(res.status).toBe(400);
  });
});
