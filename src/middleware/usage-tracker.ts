import type { MiddlewareHandler } from "hono";
import { logApiRequest } from "../analytics/log-request";

/**
 * Extract client IP from request headers / runtime.
 * Same logic as rate-limit.ts but inlined to avoid coupling.
 */
function extractClientIp(c: Parameters<MiddlewareHandler>[0]): string | undefined {
  const trustProxy = process.env.TRUSTED_PROXY === "true";

  if (trustProxy) {
    const forwarded = c.req.header("x-forwarded-for")?.split(",")[0]?.trim();
    if (forwarded) return forwarded;

    const realIp = c.req.header("x-real-ip");
    if (realIp) return realIp;
  }

  try {
    const addr = (c.env as Record<string, unknown>)?.remoteAddr ??
      ((c.req.raw as unknown as Record<string, unknown>)?.["__bun_addr"] as string | undefined);
    if (typeof addr === "string" && addr) return addr;
  } catch { /* ignore */ }

  return undefined;
}

/**
 * API usage tracking middleware.
 * Logs every request to the api_requests table (fire-and-forget).
 * Placed after the request completes so we capture status and duration.
 */
export const usageTracker: MiddlewareHandler = async (c, next) => {
  const start = Date.now();
  await next();
  const duration = Date.now() - start;

  logApiRequest(
    c.req.method,
    c.req.path,
    c.req.routePath,
    c.res.status,
    duration,
    extractClientIp(c),
    c.req.header("user-agent"),
  );
};
