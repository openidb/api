import { describe, test, expect } from "bun:test";
import { get, expectOk } from "./helpers/api";

describe("GET /api/stats", () => {
  test("returns all counts as positive numbers", async () => {
    const res = await get("/api/stats");
    const data = expectOk(res);
    expect(data.bookCount).toBeGreaterThan(0);
    expect(data.authorCount).toBeGreaterThan(0);
    expect(data.hadithCount).toBeGreaterThan(0);
    expect(data.categoryCount).toBeGreaterThan(0);
  });
});
