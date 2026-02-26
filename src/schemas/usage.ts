import { z } from "@hono/zod-openapi";

export const UsageQuerySchema = z.object({
  days: z.coerce.number().int().min(1).max(90).default(7).openapi({
    description: "Number of days to look back (1–90)",
    example: 7,
  }),
});

const EndpointStatsSchema = z.object({
  path: z.string(),
  method: z.string(),
  totalRequests: z.number(),
  avgDurationMs: z.number(),
  p95DurationMs: z.number(),
  errorCount: z.number(),
  errorRate: z.number(),
});

const HourlyBucketSchema = z.object({
  hour: z.string(),
  requests: z.number(),
});

const StatusBreakdownSchema = z.object({
  statusCode: z.number(),
  count: z.number(),
});

export const UsageResponse = z.object({
  period: z.object({
    from: z.string(),
    to: z.string(),
    days: z.number(),
  }),
  totals: z.object({
    requests: z.number(),
    avgDurationMs: z.number(),
    errorCount: z.number(),
    errorRate: z.number(),
    uniqueClients: z.number(),
  }),
  topEndpoints: z.array(EndpointStatsSchema),
  hourlyTraffic: z.array(HourlyBucketSchema),
  statusBreakdown: z.array(StatusBreakdownSchema),
}).openapi("ApiUsage");
