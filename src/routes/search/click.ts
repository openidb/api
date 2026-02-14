import { OpenAPIHono, createRoute } from "@hono/zod-openapi";
import { z } from "zod";
import { prisma } from "../../db";

const clickBody = z.object({
  searchEventId: z.string().uuid(),
  resultType: z.enum(["book", "quran", "hadith"]),
  docId: z.string().min(1),
  rank: z.number().int().min(1),
});

const click = createRoute({
  method: "post",
  path: "/click",
  tags: ["Search"],
  summary: "Track a click on a search result",
  request: {
    body: {
      content: { "application/json": { schema: clickBody } },
    },
  },
  responses: {
    204: { description: "Click recorded" },
    400: {
      content: { "application/json": { schema: z.object({ error: z.string() }) } },
      description: "Invalid request",
    },
  },
});

export const clickRoutes = new OpenAPIHono();

clickRoutes.openapi(click, async (c) => {
  const body = c.req.valid("json");

  prisma.searchClick
    .create({
      data: {
        searchEventId: body.searchEventId,
        resultType: body.resultType,
        docId: body.docId,
        rank: body.rank,
      },
    })
    .catch((err) => console.error("[analytics:click]", err.message));

  return c.body(null, 204);
});
