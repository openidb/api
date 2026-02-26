import { prisma } from "../db";

/**
 * Fire-and-forget API request logging.
 * Never blocks the response — errors are logged and swallowed.
 */
export function logApiRequest(
  method: string,
  path: string,
  routePath: string | undefined,
  statusCode: number,
  durationMs: number,
  clientIp: string | undefined,
  userAgent: string | undefined,
): void {
  prisma.apiRequest
    .create({
      data: {
        method,
        path: path.slice(0, 500),
        routePath: routePath?.slice(0, 500) ?? null,
        statusCode,
        durationMs: Math.round(durationMs),
        clientIp: clientIp?.slice(0, 45) ?? null,
        userAgent: userAgent?.slice(0, 500) ?? null,
      },
    })
    .catch((err) => console.error("[analytics:request]", err.message));
}
