import { OpenAPIHono, createRoute } from "@hono/zod-openapi";
import { prisma } from "../db";
import { UsageQuerySchema, UsageResponse } from "../schemas/usage";

const getUsage = createRoute({
  method: "get",
  path: "/",
  tags: ["Usage"],
  summary: "Get API usage statistics for dashboards",
  request: { query: UsageQuerySchema },
  responses: {
    200: {
      content: { "application/json": { schema: UsageResponse } },
      description: "API usage statistics",
    },
  },
});

export const usageRoutes = new OpenAPIHono();

usageRoutes.openapi(getUsage, async (c) => {
  const { days } = c.req.valid("query");

  const since = new Date();
  since.setDate(since.getDate() - days);

  // Run all aggregation queries in parallel
  const [totals, topEndpoints, hourlyTraffic, statusBreakdown] = await Promise.all([
    // Overall totals
    prisma.$queryRaw<
      { requests: bigint; avg_duration: number; error_count: bigint; unique_clients: bigint }[]
    >`
      SELECT
        COUNT(*)::bigint AS requests,
        COALESCE(AVG(duration_ms), 0) AS avg_duration,
        COUNT(*) FILTER (WHERE status_code >= 400)::bigint AS error_count,
        COUNT(DISTINCT client_ip)::bigint AS unique_clients
      FROM api_requests
      WHERE created_at >= ${since}
    `,

    // Top endpoints by request count (using route pattern for grouping)
    prisma.$queryRaw<
      {
        path: string;
        method: string;
        total_requests: bigint;
        avg_duration: number;
        p95_duration: number;
        error_count: bigint;
      }[]
    >`
      SELECT
        COALESCE(route_path, path) AS path,
        method,
        COUNT(*)::bigint AS total_requests,
        AVG(duration_ms) AS avg_duration,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_duration,
        COUNT(*) FILTER (WHERE status_code >= 400)::bigint AS error_count
      FROM api_requests
      WHERE created_at >= ${since}
      GROUP BY COALESCE(route_path, path), method
      ORDER BY total_requests DESC
      LIMIT 20
    `,

    // Hourly traffic buckets
    prisma.$queryRaw<{ hour: Date; requests: bigint }[]>`
      SELECT
        date_trunc('hour', created_at) AS hour,
        COUNT(*)::bigint AS requests
      FROM api_requests
      WHERE created_at >= ${since}
      GROUP BY date_trunc('hour', created_at)
      ORDER BY hour
    `,

    // Status code breakdown
    prisma.$queryRaw<{ status_code: number; count: bigint }[]>`
      SELECT
        status_code,
        COUNT(*)::bigint AS count
      FROM api_requests
      WHERE created_at >= ${since}
      GROUP BY status_code
      ORDER BY count DESC
    `,
  ]);

  const t = totals[0] ?? { requests: 0n, avg_duration: 0, error_count: 0n, unique_clients: 0n };
  const totalReqs = Number(t.requests);

  return c.json(
    {
      period: {
        from: since.toISOString(),
        to: new Date().toISOString(),
        days,
      },
      totals: {
        requests: totalReqs,
        avgDurationMs: Math.round(Number(t.avg_duration)),
        errorCount: Number(t.error_count),
        errorRate: totalReqs > 0 ? Math.round((Number(t.error_count) / totalReqs) * 10000) / 10000 : 0,
        uniqueClients: Number(t.unique_clients),
      },
      topEndpoints: topEndpoints.map((e) => {
        const reqs = Number(e.total_requests);
        const errors = Number(e.error_count);
        return {
          path: e.path,
          method: e.method,
          totalRequests: reqs,
          avgDurationMs: Math.round(Number(e.avg_duration)),
          p95DurationMs: Math.round(Number(e.p95_duration)),
          errorCount: errors,
          errorRate: reqs > 0 ? Math.round((errors / reqs) * 10000) / 10000 : 0,
        };
      }),
      hourlyTraffic: hourlyTraffic.map((h) => ({
        hour: h.hour instanceof Date ? h.hour.toISOString() : String(h.hour),
        requests: Number(h.requests),
      })),
      statusBreakdown: statusBreakdown.map((s) => ({
        statusCode: s.status_code,
        count: Number(s.count),
      })),
    },
    200,
  );
});
