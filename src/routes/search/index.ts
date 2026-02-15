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
  };

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

    // Resolve hadithunlocked.com source URLs (needs DB lookup for display numbers)
    hadiths = await resolveHadithSourceUrls(hadiths);

    // Limit final results
    rankedResults = rankedResults.slice(0, params.limit);

    // Graph context resolution
    const { graphContext, ayahsRaw: boostedAyahs } = await resolveGraphContext(graphResult, params.includeGraph, ayahsRaw);
    ayahsRaw = boostedAyahs;

    // Fetch book details & format results
    const bookDetails = await fetchBookDetails(rankedResults, params.bookTitleLang);
    rankedResults = bookDetails.results;
    _timing.bookMetadata = bookDetails.timing.bookMetadata;

    const results = formatSearchResults(rankedResults, bookDetails.books, params.mode);
    const ayahs = translated.ayahs;

    // Build debug stats
    const debugStats = await buildDebugStats(
      params, results, ayahs, hadiths, rankedResults,
      ayahSearchMeta, totalAboveCutoff, rerankerTimedOut,
      _timing, refineStats,
    );

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

    return c.json({
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
    }, 200);
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
