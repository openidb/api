import { describe, test, expect } from "bun:test";
import { get, expectOk } from "./helpers/api";

describe("GET /api/search - error cases", () => {
  test("no q param returns 400", async () => {
    const res = await get("/api/search");
    expect(res.status).toBe(400);
  });

  test("empty q returns 400", async () => {
    const res = await get("/api/search", { q: "" });
    expect(res.status).toBe(400);
  });

  test("q > 500 chars returns 400", async () => {
    const res = await get("/api/search", { q: "ا".repeat(501) });
    expect(res.status).toBe(400);
  });

  test("invalid mode returns 400", async () => {
    const res = await get("/api/search", { q: "test", mode: "invalid" });
    expect(res.status).toBe(400);
  });
});

describe("GET /api/search - success cases", { timeout: 30000 }, () => {
  test("basic Arabic query returns hybrid results", async () => {
    const data = expectOk(
      await get("/api/search", { q: "بسم الله الرحمن الرحيم" })
    );
    expect(data.query).toBe("بسم الله الرحمن الرحيم");
    expect(data.mode).toBe("hybrid");
    expect(typeof data.count).toBe("number");
    expect(Array.isArray(data.results)).toBe(true);
    expect(Array.isArray(data.ayahs)).toBe(true);
    expect(Array.isArray(data.hadiths)).toBe(true);
    expect(Array.isArray(data.authors)).toBe(true);
  });

  test("mode=keyword returns keyword results", async () => {
    const data = expectOk(
      await get("/api/search", { q: "بسم الله", mode: "keyword" })
    );
    expect(data.mode).toBe("keyword");
  });

  test("mode=semantic returns semantic results", async () => {
    const data = expectOk(
      await get("/api/search", { q: "الصلاة", mode: "semantic" })
    );
    expect(data.mode).toBe("semantic");
  });

  test("?limit=3 limits results", async () => {
    const data = expectOk(
      await get("/api/search", { q: "الله", limit: 3 })
    );
    expect(data.results.length).toBeLessThanOrEqual(3);
  });

  test("includeQuran=false returns 200", async () => {
    const data = expectOk(
      await get("/api/search", { q: "الصلاة", includeQuran: "false" })
    );
    expect(data).toBeDefined();
  });

  test("includeBooks=false returns 200", async () => {
    const data = expectOk(
      await get("/api/search", { q: "الصلاة", includeBooks: "false" })
    );
    expect(data).toBeDefined();
  });
});
