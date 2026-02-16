import type { MiddlewareHandler } from "hono";

/**
 * Request timeout middleware.
 * Aborts the request if it exceeds the specified timeout.
 * First-wins: if a more specific route already set a timeout, the general one is skipped.
 */
export function timeout(ms: number): MiddlewareHandler {
  return async (c, next) => {
    // Skip if a more specific timeout middleware already claimed this request
    if (c.get("_timeout_ms")) {
      return next();
    }
    c.set("_timeout_ms", ms);

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), ms);

    try {
      // Race the handler against the timeout
      await Promise.race([
        next(),
        new Promise((_, reject) => {
          controller.signal.addEventListener("abort", () =>
            reject(new Error("Request timeout"))
          );
        }),
      ]);
    } catch (err) {
      if (err instanceof Error && err.message === "Request timeout") {
        return c.json({ error: "Request timeout" }, 504);
      }
      throw err;
    } finally {
      clearTimeout(timer);
    }
  };
}
