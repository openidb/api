import { prisma } from "../db";

/**
 * Fire-and-forget search event logging.
 * Never blocks the search response — errors are logged and swallowed.
 */
export function logSearchEvent(
  searchEventId: string,
  sessionId: string | undefined,
  query: string,
  mode: string,
  isRefine: boolean,
  results: { type: string; docId: string; score: number; rank: number }[],
  totalTimeMs: number,
): void {
  prisma.searchEvent
    .create({
      data: {
        id: searchEventId,
        sessionId: sessionId || null,
        query,
        mode,
        isRefine,
        resultCount: results.length,
        totalTimeMs: Math.round(totalTimeMs),
        topResults: results.slice(0, 20).map((r) => ({
          t: r.type,
          d: r.docId,
          s: Math.round(r.score * 1000) / 1000,
          r: r.rank,
        })),
      },
    })
    .catch((err) => console.error("[analytics]", err.message));
}
