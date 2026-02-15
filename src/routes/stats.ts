import { OpenAPIHono, createRoute } from "@hono/zod-openapi";
import { prisma } from "../db";
import { StatsResponse } from "../schemas/stats";
import { SOURCES } from "../utils/source-urls";

const getStats = createRoute({
  method: "get",
  path: "/",
  tags: ["Stats"],
  summary: "Get database statistics",
  responses: {
    200: {
      content: { "application/json": { schema: StatsResponse } },
      description: "Database statistics",
    },
  },
});

export const statsRoutes = new OpenAPIHono();

statsRoutes.openapi(getStats, async (c) => {
  const safeCount = (p: Promise<number>) => p.catch(() => 0);
  const [bookCount, authorCount, hadithCount, categoryCount] = await Promise.all([
    safeCount(prisma.book.count()),
    safeCount(prisma.author.count()),
    safeCount(prisma.hadith.count()),
    safeCount(prisma.category.count()),
  ]);

  c.header("Cache-Control", "public, max-age=3600");
  return c.json({
    bookCount, authorCount, hadithCount, categoryCount,
    _sources: [...SOURCES.turath, ...SOURCES.quranCloud, ...SOURCES.sunnah, ...SOURCES.hadithUnlocked],
  }, 200);
});
