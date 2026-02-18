import { OpenAPIHono, createRoute } from "@hono/zod-openapi";
import { startTimer } from "../../utils/timing";
import { QDRANT_QURAN_COLLECTION } from "../../qdrant";
import { searchAuthors } from "./engines";
import { executeStandardSearch } from "./standard-search";
import { executeRefineSearch } from "./refine-search";
import { fetchAndMergeTranslations } from "./translations";
import { startGraphSearch, resolveGraphContext, fetchBookDetails, formatSearchResults, buildDebugStats, resolveHadithSourceUrls } from "./response";
import type { AyahSearchMeta, SearchMode, RerankerType, EmbeddingModel } from "./types";
import type { SearchParams } from "./params";
import { ErrorResponse } from "../../schemas/common";
import { SearchQuery, SearchResponse } from "../../schemas/search";
import { logSearchEvent } from "../../analytics/log-search";
import { clickRoutes } from "./click";
import { hadithTranslateRoutes } from "./hadith-translate";
import { SOURCES } from "../../utils/source-urls";
import { TTLCache } from "../../lib/ttl-cache";

const searchResponseCache = new TTLCache<object>({
  maxSize: 2000,
  ttlMs: 60 * 60 * 1000, // 1 hour — data is static
  evictionCount: 200,
  label: "SearchResponse",
});

const search = createRoute({
  method: "get",
  path: "/",
  tags: ["Search"],
  summary: "Search across Quran, Hadith, and books",
  request: { query: SearchQuery },
  responses: {
    200: {
      content: { "application/json": { schema: SearchResponse } },
      description: "Search results",
    },
    400: {
      content: { "application/json": { schema: ErrorResponse } },
      description: "Invalid search parameters",
    },
    503: {
      content: { "application/json": { schema: ErrorResponse } },
      description: "Search index not initialized",
    },
  },
});

export const searchRoutes = new OpenAPIHono();

// Mount click tracking sub-route: POST /api/search/click
searchRoutes.route("/", clickRoutes);

// Mount hadith translation sub-route: POST /api/search/translate-hadiths
searchRoutes.route("/", hadithTranslateRoutes);

searchRoutes.openapi(search, async (c) => {
  const validated = c.req.valid("query");

  // Convert validated query to SearchParams
  const params: SearchParams = {
    query: validated.q,
    limit: validated.limit,
    bookId: validated.bookId || null,
    mode: validated.mode as SearchMode,
    includeQuran: validated.includeQuran !== "false",
    includeHadith: validated.includeHadith !== "false",
    includeBooks: validated.includeBooks !== "false",
    reranker: validated.reranker as RerankerType,
    similarityCutoff: validated.similarityCutoff,
    bookLimit: validated.bookLimit,
    fuzzyEnabled: validated.fuzzy !== "false",
    quranTranslation: validated.quranTranslation,
    hadithTranslation: validated.hadithTranslation,
    bookTitleLang: validated.bookTitleLang,
    bookContentTranslation: validated.bookContentTranslation,
    refine: validated.refine === "true",
    refineSimilarityCutoff: validated.refineSimilarityCutoff,
    refineOriginalWeight: validated.refineOriginalWeight,
    refineExpandedWeight: validated.refineExpandedWeight,
    refineBookPerQuery: validated.refineBookPerQuery,
    refineAyahPerQuery: validated.refineAyahPerQuery,
    refineHadithPerQuery: validated.refineHadithPerQuery,
    refineBookRerank: validated.refineBookRerank,
    refineAyahRerank: validated.refineAyahRerank,
    refineHadithRerank: validated.refineHadithRerank,
    queryExpansionModel: validated.queryExpansionModel,
    includeGraph: validated.includeGraph !== "false",
    embeddingModel: (validated.embeddingModel || "gemini") as EmbeddingModel,
    hadithCollections: validated.hadithCollections
      ? validated.hadithCollections.split(",").filter(Boolean)
      : [],
  };

  // Check search response cache (excludes debug stats which have timing data)
  const cacheKey = JSON.stringify({
    q: params.query, mode: params.mode, limit: params.limit, offset: 0,
    includeBooks: params.includeBooks, includeQuran: params.includeQuran, includeHadith: params.includeHadith,
    hadithCollections: params.hadithCollections, bookId: params.bookId, reranker: params.reranker,
    refine: params.refine, embeddingModel: params.embeddingModel, quranTranslation: params.quranTranslation,
    bookTitleLang: params.bookTitleLang, bookContentTranslation: params.bookContentTranslation,
    hadithTranslation: params.hadithTranslation,
  });
  const cached = searchResponseCache.get(cacheKey);
  if (cached) return c.json(cached, 200);

  try {
    const _timing = {
      start: Date.now(),
      embedding: 0,
      semantic: { books: 0, ayahs: 0, hadiths: 0 },
      keyword: { books: 0, ayahs: 0, hadiths: 0 },
      merge: 0,
      authorSearch: 0,
      rerank: 0,
      translations: 0,
      bookMetadata: 0,
      graph: 0,
    };

    // Start graph search and author search early
    const graphPromise = startGraphSearch(params.query, params.includeGraph, _timing);
    const authorsPromise = params.bookId ? Promise.resolve([]) : searchAuthors(params.query, 5);

    let rankedResults;
    let ayahsRaw;
    let hadiths;
    let expandedQueries: { query: string; reason: string }[] = [];
    let totalAboveCutoff = 0;
    let rerankerTimedOut = false;
    let refineStats;
    let ayahSearchMeta: AyahSearchMeta = {
      collection: QDRANT_QURAN_COLLECTION,
      usedFallback: false,
      embeddingTechnique: "metadata-translation",
    };

    // Execute search
    if (params.refine && params.mode === "hybrid" && !params.bookId) {
      const result = await executeRefineSearch(params);
      rankedResults = result.rankedResults;
      ayahsRaw = result.ayahsRaw;
      hadiths = result.hadiths;
      expandedQueries = result.expandedQueries;
      rerankerTimedOut = result.rerankerTimedOut;
      refineStats = result.refineStats;
    } else {
      const result = await executeStandardSearch(params);
      rankedResults = result.rankedResults;
      ayahsRaw = result.ayahsRaw;
      hadiths = result.hadiths;
      ayahSearchMeta = result.ayahSearchMeta;
      totalAboveCutoff = result.totalAboveCutoff;
      _timing.embedding = result.timing.embedding;
      _timing.semantic = result.timing.semantic;
      _timing.keyword = result.timing.keyword;
      _timing.merge = result.timing.merge;
    }

    // Wait for graph + author search
    const graphResult = await graphPromise;
    const authorTimer = startTimer();
    const authors = await authorsPromise;
    _timing.authorSearch = authorTimer();

    // Fetch translations
    const translationsTimer = startTimer();
    const translated = await fetchAndMergeTranslations(params, rankedResults, ayahsRaw, hadiths);
    rankedResults = translated.rankedResults;
    hadiths = translated.hadiths;
    _timing.translations = translationsTimer();

    // Parallelize independent response pipeline operations
    const [resolvedHadiths, graphCtx, bookDetails] = await Promise.all([
      resolveHadithSourceUrls(hadiths),
      resolveGraphContext(graphResult, params.includeGraph, ayahsRaw),
      fetchBookDetails(rankedResults.slice(0, params.limit), params.bookTitleLang),
    ]);

    hadiths = resolvedHadiths;
    rankedResults = bookDetails.results;
    ayahsRaw = graphCtx.ayahsRaw;
    _timing.bookMetadata = bookDetails.timing.bookMetadata;
    const graphContext = graphCtx.graphContext;

    const results = formatSearchResults(rankedResults, bookDetails.books, params.mode);
    const ayahs = translated.ayahs;

    // Build debug stats (skip in production — fires 4 COUNT(*) queries)
    const debugStats = process.env.NODE_ENV !== "production"
      ? await buildDebugStats(
          params, results, ayahs, hadiths, rankedResults,
          ayahSearchMeta, totalAboveCutoff, rerankerTimedOut,
          _timing, refineStats,
        )
      : undefined;

    // Fire-and-forget analytics logging (only when frontend sends event ID)
    const searchEventId = c.req.header("x-search-event-id");
    if (searchEventId) {
      const allResults = [
        ...results.map((r, i) => ({
          type: "book" as const,
          docId: `${r.bookId}:${r.pageNumber}`,
          score: r.score,
          rank: r.rank ?? i + 1,
        })),
        ...ayahs.map((a, i) => ({
          type: "quran" as const,
          docId: `${a.surahNumber}:${a.ayahNumber}`,
          score: a.score,
          rank: a.rank ?? i + 1,
        })),
        ...hadiths.map((h, i) => ({
          type: "hadith" as const,
          docId: `${h.collectionSlug}:${h.hadithNumber}`,
          score: h.score,
          rank: h.rank ?? i + 1,
        })),
      ];
      logSearchEvent(
        searchEventId,
        c.req.header("x-session-id"),
        params.query,
        params.mode,
        params.refine,
        allResults,
        Date.now() - _timing.start,
      );
    }

    const _sources = [
      ...(params.includeBooks ? SOURCES.turath : []),
      ...(params.includeQuran ? SOURCES.quranCloud : []),
      ...(params.includeHadith ? [...SOURCES.sunnah, ...SOURCES.hadithUnlocked] : []),
    ];

    const responseBody = {
      query: params.query,
      mode: params.mode,
      count: results.length,
      results,
      authors,
      ayahs,
      hadiths,
      ...(process.env.NODE_ENV !== "production" && { debugStats }),
      ...(graphContext && { graphContext }),
      ...(params.refine && {
        refined: true,
        expandedQueries,
      }),
      _sources,
    };

    // Cache without debug stats (they contain timing data)
    const { debugStats: _ds, ...cacheable } = responseBody as typeof responseBody & { debugStats?: unknown };
    searchResponseCache.set(cacheKey, cacheable);

    return c.json(responseBody, 200);
  } catch (error) {
    console.error("Search error:", error);

    if (error instanceof Error) {
      if (error.message.includes("Collection not found")) {
        return c.json(
          { error: "Search index not initialized" },
          503
        );
      }
    }

    return c.json(
      { error: "Search failed" },
      400
    );
  }
});
