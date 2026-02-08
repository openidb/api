import { describe, test, expect } from "bun:test";
import { get, expectOk } from "./helpers/api";

describe("GET /api/health", () => {
  test("returns status ok", async () => {
    const res = await get("/api/health");
    const data = expectOk(res);
    expect(data.status).toBe("ok");
  });
});
