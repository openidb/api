import { z } from "@hono/zod-openapi";
import { SourceSchema } from "./common";

export const StatsResponse = z.object({
  bookCount: z.number(),
  authorCount: z.number(),
  hadithCount: z.number(),
  categoryCount: z.number(),
  _sources: z.array(SourceSchema),
}).openapi("Stats");
