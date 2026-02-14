import { prisma } from "../db";

/**
 * Fire-and-forget book event logging.
 * Never blocks the response — errors are logged and swallowed.
 */
export function logBookEvent(
  sessionId: string | undefined,
  bookId: string,
  action: "open" | "page_view" | "pdf_open" | "word_lookup",
  pageNumber?: number | null,
  durationMs?: number | null,
  word?: string | null,
): void {
  prisma.bookEvent
    .create({
      data: {
        sessionId: sessionId || null,
        bookId,
        action,
        pageNumber: pageNumber ?? null,
        durationMs: durationMs != null ? Math.round(durationMs) : null,
        word: word?.slice(0, 100) ?? null,
      },
    })
    .catch((err) => console.error("[analytics:book]", err.message));
}
