import { OpenAPIHono, createRoute } from "@hono/zod-openapi";
import { z } from "zod";
import { logBookEvent } from "../analytics/log-book";

const bookEventBody = z.object({
  sessionId: z.string().uuid().optional(),
  bookId: z.string().min(1),
  action: z.enum(["open", "page_view", "pdf_open", "word_lookup"]),
  pageNumber: z.number().int().min(0).optional(),
  durationMs: z.number().int().min(0).max(3_600_000).optional(),
  word: z.string().max(100).optional(),
});

const singleEvent = createRoute({
  method: "post",
  path: "/events",
  tags: ["Books"],
  summary: "Track a book reader interaction",
  request: {
    body: {
      content: { "application/json": { schema: bookEventBody } },
    },
  },
  responses: {
    204: { description: "Event recorded" },
    400: {
      content: { "application/json": { schema: z.object({ error: z.string() }) } },
      description: "Invalid request",
    },
  },
});

const batchEvent = createRoute({
  method: "post",
  path: "/events/batch",
  tags: ["Books"],
  summary: "Track multiple book reader interactions (sendBeacon)",
  request: {
    body: {
      content: {
        "application/json": {
          schema: z.object({ events: z.array(bookEventBody).min(1).max(50) }),
        },
      },
    },
  },
  responses: {
    204: { description: "Events recorded" },
    400: {
      content: { "application/json": { schema: z.object({ error: z.string() }) } },
      description: "Invalid request",
    },
  },
});

export const bookEventRoutes = new OpenAPIHono();

bookEventRoutes.openapi(singleEvent, async (c) => {
  const body = c.req.valid("json");
  logBookEvent(body.sessionId, body.bookId, body.action, body.pageNumber, body.durationMs, body.word);
  return c.body(null, 204);
});

bookEventRoutes.openapi(batchEvent, async (c) => {
  const { events } = c.req.valid("json");
  for (const ev of events) {
    logBookEvent(ev.sessionId, ev.bookId, ev.action, ev.pageNumber, ev.durationMs, ev.word);
  }
  return c.body(null, 204);
});
