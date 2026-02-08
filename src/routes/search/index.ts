import { Hono } from "hono";
import { startTimer } from "../../utils/timing";
import { QDRANT_QURAN_COLLECTION } from "../../qdrant";
import { searchAuthors } from "./engines";
import { parseSearchParams } from "./params";
import { executeStandardSearch } from "./standard-search";
import { executeRefineSearch } from "./refine-search";
import { fetchAndMergeTranslations } from "./translations";
import { startGraphSearch, resolveGraphContext, fetchBookDetails, formatSearchResults, buildDebugStats } from "./response";
import type { AyahSearchMeta } from "./types";

export const searchRoutes = new Hono();

searchRoutes.get("/", async (c) => {
  const parsed = parseSearchParams(c);
  if ("error" in parsed) {
    return c.json({ error: parsed.error }, parsed.status as 400);
  }
  const params = parsed;

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
    });
  } catch (error) {
    console.error("Search error:", error);

    if (error instanceof Error) {
      if (error.message.includes("Collection not found")) {
        return c.json(
          { error: "Search index not initialized", message: "Run the embedding generation script first" },
          503
        );
      }
    }

    return c.json(
      {
        error: "Search failed",
        ...(process.env.NODE_ENV !== "production" && { message: String(error) }),
      },
      500
    );
  }
});
