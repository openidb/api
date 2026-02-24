import { OpenAPIHono } from "@hono/zod-openapi";
import { cors } from "hono/cors";
import { compress } from "hono/compress";
import { bodyLimit } from "hono/body-limit";
import { HTTPException } from "hono/http-exception";
import { apiReference } from "@scalar/hono-api-reference";
import { quranRoutes } from "./routes/quran";
import { hadithRoutes } from "./routes/hadith";
import { booksRoutes } from "./routes/books";
import { authorsRoutes } from "./routes/authors";
import { categoriesRoutes } from "./routes/categories";
import { centuriesRoutes } from "./routes/centuries";
import { searchRoutes } from "./routes/search";
import { transcribeRoutes } from "./routes/transcribe";
import { statsRoutes } from "./routes/stats";
import { dictionaryRoutes } from "./routes/dictionary";
import { bookEventRoutes } from "./routes/book-events";
import { bookFeaturesRoutes } from "./routes/book-features";
import { apiRateLimit, searchRateLimit, expensiveRateLimit } from "./middleware/rate-limit";
import { timeout } from "./middleware/timeout";
import { requestLogger } from "./middleware/request-logger";
import { internalAuth } from "./middleware/internal-auth";
import { prisma } from "./db";
import { qdrant } from "./qdrant";
import { checkS3Health } from "./utils/s3-bucket";

const app = new OpenAPIHono({
  defaultHook: (result, c) => {
    if (!result.success) {
      return c.json({
        error: "Validation error",
        details: result.error.issues.map((i) => ({
          path: i.path.join("."),
          message: i.message,
        })),
      }, 400);
    }
  },
});

const allowedOrigins = (process.env.ALLOWED_ORIGINS || "http://localhost:3000")
  .split(",")
  .map((o) => o.trim());

// Middleware stack: CORS → compression → timeout → request logging → rate limits → routes
app.use(
  "/api/*",
  cors({
    origin: allowedOrigins,
    allowMethods: ["GET", "POST", "OPTIONS"],
    allowHeaders: ["Content-Type", "Authorization", "x-search-event-id", "x-session-id"],
    maxAge: 86400,
  })
);

// Response compression (skip audio — MP3 is already compressed, and gzip breaks Content-Length)
app.use("/api/*", async (c, next) => {
  if (c.req.path.includes("/audio/")) return next();
  return compress()(c, next);
});

// Request body size limits (before parsing/validation)
app.use("/api/transcribe", bodyLimit({ maxSize: 26 * 1024 * 1024 })); // 26MB for audio upload
app.use("/api/transcribe/*", bodyLimit({ maxSize: 26 * 1024 * 1024 }));
app.use("/api/*", bodyLimit({ maxSize: 1024 * 1024 })); // 1MB default for JSON

// Request timeout — specific routes first (first-wins), general fallback last
app.use("/api/books/:id/pages/:page/translate", timeout(60_000));
app.use("/api/search/translate-hadiths", timeout(90_000));
app.use("/api/*", timeout(30_000));

// Request logging
app.use("/api/*", requestLogger);

// General rate limit for all API endpoints (120 req/min)
app.use("/api/*", apiRateLimit);

// Rate limiting for expensive endpoints
app.use("/api/search/*", searchRateLimit);
app.use("/api/search", searchRateLimit);
app.use("/api/transcribe/*", internalAuth);
app.use("/api/transcribe", internalAuth);
app.use("/api/transcribe/*", expensiveRateLimit);
app.use("/api/transcribe", expensiveRateLimit);
app.use("/api/books/:id/pages/:page/translate", internalAuth);
app.use("/api/books/:id/pages/:page/translate", expensiveRateLimit);
app.use("/api/search/translate-hadiths", internalAuth);
app.use("/api/search/translate-hadiths", expensiveRateLimit);

app.route("/api/search", searchRoutes);
app.route("/api/quran", quranRoutes);
app.route("/api/hadith", hadithRoutes);
app.route("/api/books", bookEventRoutes);
app.route("/api/books/authors", authorsRoutes);
app.route("/api/books/categories", categoriesRoutes);
app.route("/api/books/centuries", centuriesRoutes);
app.route("/api/books/features", bookFeaturesRoutes);
app.route("/api/books", booksRoutes);
app.route("/api/transcribe", transcribeRoutes);
app.route("/api/stats", statsRoutes);
app.route("/api/dictionary", dictionaryRoutes);

// Health check — pings Postgres, Qdrant, Elasticsearch, and S3 (RustFS)
app.get("/api/health", async (c) => {
  const checks: Record<string, "ok" | "error"> = {};
  const errors: string[] = [];

  // Postgres
  try {
    await prisma.$queryRawUnsafe("SELECT 1");
    checks.postgres = "ok";
  } catch (err) {
    checks.postgres = "error";
    errors.push(`postgres: ${(err as Error).message}`);
  }

  // Qdrant
  try {
    await Promise.race([
      qdrant.getCollections(),
      new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), 3000)),
    ]);
    checks.qdrant = "ok";
  } catch (err) {
    checks.qdrant = "error";
    errors.push(`qdrant: ${(err as Error).message}`);
  }

  // Elasticsearch
  const esUrl = process.env.ELASTICSEARCH_URL || "http://localhost:9200";
  const esPassword = process.env.ELASTIC_PASSWORD || "";
  const esAuth = btoa(`elastic:${esPassword}`);
  try {
    const res = await fetch(esUrl, {
      signal: AbortSignal.timeout(3000),
      headers: { Authorization: `Basic ${esAuth}` },
    });
    checks.elasticsearch = res.ok ? "ok" : "error";
    if (!res.ok) errors.push(`elasticsearch: HTTP ${res.status}`);
  } catch (err) {
    checks.elasticsearch = "error";
    errors.push(`elasticsearch: ${(err as Error).message}`);
  }

  // S3 (RustFS) — non-blocking, doesn't affect overall status
  try {
    checks.s3 = await checkS3Health();
    if (checks.s3 === "error") errors.push("s3: health check failed");
  } catch (err) {
    checks.s3 = "error";
    errors.push(`s3: ${(err as Error).message}`);
  }

  // S3 is non-blocking: exclude from allOk calculation
  const allOk = Object.entries(checks)
    .filter(([key]) => key !== "s3")
    .every(([, v]) => v === "ok");
  const status = allOk ? "ok" : "degraded";

  // In production, only expose aggregate status without service topology
  if (process.env.NODE_ENV === "production") {
    return c.json({ status }, allOk ? 200 : 503);
  }

  return c.json(
    { status, services: checks, ...(errors.length > 0 && { errors }) },
    allOk ? 200 : 503
  );
});

// OpenAPI spec
app.doc("/api/openapi.json", {
  openapi: "3.1.0",
  info: {
    title: "OpenIDB API",
    version: "1.0.0",
    description: "API for Islamic texts: Quran, Hadith, and classical Arabic books",
  },
  servers: [{ url: "http://localhost:4000" }],
});

// Interactive docs UI
app.get("/api/docs", apiReference({
  url: "/api/openapi.json",
  theme: "default",
}));

// Global error handler
app.onError((err, c) => {
  console.error(`[${c.req.method}] ${c.req.path}:`, err);
  if (err instanceof HTTPException) {
    return c.json({ error: err.message }, err.status);
  }
  const message = process.env.NODE_ENV === "production"
    ? "Internal server error"
    : err.message;
  return c.json({ error: message }, 500);
});

app.notFound((c) => c.json({ error: "Not found" }, 404));

export default { port: 4000, fetch: app.fetch };
