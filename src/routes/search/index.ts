/**
 * Hybrid Search API Endpoint (Hono)
 *
 * GET /api/search?q={query}&limit={20}&mode={hybrid|semantic|keyword}&bookId={optional}
 *     &includeQuran={true}&includeHadith={true}&includeBooks={true}
 *     &reranker={gpt-oss-20b|gpt-oss-120b|gemini-flash|none}&similarityCutoff={0.6}
 *     &bookLimit={10}
 */

import { Hono } from "hono";
import {
  QDRANT_COLLECTION,
  QDRANT_QURAN_COLLECTION,
  QDRANT_HADITH_COLLECTION,
  QDRANT_COLLECTION_BGE,
  QDRANT_QURAN_COLLECTION_BGE,
  QDRANT_HADITH_COLLECTION_BGE,
  GEMINI_DIMENSIONS,
  BGE_DIMENSIONS,
} from "../../qdrant";
import {
  generateEmbedding,
  normalizeArabicText,
  getEmbeddingModelName,
  type EmbeddingModel,
} from "../../embeddings";
import { prisma } from "../../db";
import { normalizeBM25Score } from "../../search/bm25";
import {
  keywordSearchES,
  keywordSearchHadithsES,
  keywordSearchAyahsES,
} from "../../search/elasticsearch-search";
import {
  searchEntities,
  resolveSources,
  resolveGraphMentions,
  type GraphSearchResult,
  type GraphContext,
  type GraphContextEntity,
  type ResolvedSource,
} from "../../graph/search";

import type {
  RerankerType,
  SearchMode,
  SearchResult,
  AyahResult,
  HadithResult,
  RankedResult,
  AyahRankedResult,
  HadithRankedResult,
  AyahSearchMeta,
  SearchDebugStats,
  TopResultBreakdown,
  ExpandedQueryStats,
} from "./types";
import { MIN_CHARS_FOR_SEMANTIC, RRF_K, SEMANTIC_WEIGHT, KEYWORD_WEIGHT } from "./config";
import { hasQuotedPhrases, getSearchStrategy } from "./query-utils";
import { generateShamelaPageUrl } from "../../utils/source-urls";
import { calculateRRFScore, mergeWithRRF, mergeWithRRFGeneric, getMatchType, mergeAndDeduplicateBooks, mergeAndDeduplicateAyahs, mergeAndDeduplicateHadiths } from "./fusion";
import { rerankUnifiedRefine } from "./rerankers";
import { semanticSearch, searchAuthors, searchAyahsSemantic, searchAyahsHybrid, searchHadithsSemantic, searchHadithsHybrid } from "./engines";
import { expandQueryWithCacheInfo, getQueryExpansionModelId } from "./refine";
import { getDatabaseStats, extractParagraphTexts, findMatchingParagraphIndex, getBookMetadataForReranking } from "./helpers";

/**
 * Merge results by search mode, eliminating repeated if/else per content type
 */
function mergeByMode<T>(opts: {
  include: boolean;
  mode: string;
  keywordResults: T[];
  semanticResults: T[];
  limit: number;
  merge: () => T[];
  normalizeKeyword?: (items: T[]) => T[];
}): T[] {
  if (!opts.include) return [];
  if (opts.mode === "keyword") return (opts.normalizeKeyword ?? ((x) => x))(opts.keywordResults).slice(0, opts.limit);
  if (opts.mode === "semantic") return opts.semanticResults.slice(0, opts.limit);
  return opts.merge().slice(0, opts.limit);
}

export const searchRoutes = new Hono();

searchRoutes.get("/", async (c) => {
  const query = c.req.query("q");
  const limitParam = c.req.query("limit");
  const bookIdParam = c.req.query("bookId");
  const modeParam = c.req.query("mode") as SearchMode | undefined;

  // Content type toggles
  const includeQuran = c.req.query("includeQuran") !== "false";
  const includeHadith = c.req.query("includeHadith") !== "false";
  const includeBooks = c.req.query("includeBooks") !== "false";

  // Reranker configuration
  const rerankerParam = c.req.query("reranker") as RerankerType | undefined;
  const reranker: RerankerType = rerankerParam && ["gpt-oss-20b", "gpt-oss-120b", "gemini-flash", "none"].includes(rerankerParam)
    ? rerankerParam
    : "none";
  const similarityCutoff = parseFloat(c.req.query("similarityCutoff") || "0.6");
  const bookLimit = Math.min(Math.max(parseInt(c.req.query("bookLimit") || "10", 10), 5), 50);

  // Fuzzy search
  const fuzzyEnabled = c.req.query("fuzzy") !== "false";

  // Translation parameters
  const quranTranslation = c.req.query("quranTranslation") || "none";
  const hadithTranslation = c.req.query("hadithTranslation") || "none";
  const bookTitleLang = c.req.query("bookTitleLang");
  const bookContentTranslation = c.req.query("bookContentTranslation") || "none";

  // Refine search parameters
  const refine = c.req.query("refine") === "true";
  const refineSimilarityCutoff = refine ? parseFloat(c.req.query("refineSimilarityCutoff") || "0.25") : 0.25;
  const refineOriginalWeight = Math.min(Math.max(parseFloat(c.req.query("refineOriginalWeight") || "1.0"), 0.5), 1.0);
  const refineExpandedWeight = Math.min(Math.max(parseFloat(c.req.query("refineExpandedWeight") || "0.7"), 0.3), 1.0);
  const refineBookPerQuery = Math.min(Math.max(parseInt(c.req.query("refineBookPerQuery") || "30", 10), 10), 60);
  const refineAyahPerQuery = Math.min(Math.max(parseInt(c.req.query("refineAyahPerQuery") || "30", 10), 10), 60);
  const refineHadithPerQuery = Math.min(Math.max(parseInt(c.req.query("refineHadithPerQuery") || "30", 10), 10), 60);
  const refineBookRerank = Math.min(Math.max(parseInt(c.req.query("refineBookRerank") || "20", 10), 5), 40);
  const refineAyahRerank = Math.min(Math.max(parseInt(c.req.query("refineAyahRerank") || "12", 10), 5), 25);
  const refineHadithRerank = Math.min(Math.max(parseInt(c.req.query("refineHadithRerank") || "15", 10), 5), 25);
  const queryExpansionModel = c.req.query("queryExpansionModel") || "gemini-flash";

  // Graph search
  const includeGraph = c.req.query("includeGraph") !== "false";

  // Embedding model selection
  const embeddingModelParam = c.req.query("embeddingModel") as EmbeddingModel | undefined;
  const embeddingModel: EmbeddingModel = embeddingModelParam === "bge-m3" ? "bge-m3" : "gemini";

  // Get collections based on embedding model
  const pageCollection = embeddingModel === "bge-m3" ? QDRANT_COLLECTION_BGE : QDRANT_COLLECTION;
  const quranCollection = embeddingModel === "bge-m3" ? QDRANT_QURAN_COLLECTION_BGE : QDRANT_QURAN_COLLECTION;
  const hadithCollection = embeddingModel === "bge-m3" ? QDRANT_HADITH_COLLECTION_BGE : QDRANT_HADITH_COLLECTION;
  const embeddingDimensions = embeddingModel === "bge-m3" ? BGE_DIMENSIONS : GEMINI_DIMENSIONS;

  // Validate query
  if (!query || query.trim().length === 0) {
    return c.json({ error: "Query parameter 'q' is required" }, 400);
  }

  if (query.length > 500) {
    return c.json({ error: "Query too long (max 500 characters)" }, 400);
  }

  // Parse parameters
  const limit = Math.min(Math.max(parseInt(limitParam || "20", 10), 1), 100);
  const bookId = bookIdParam || null;
  const mode: SearchMode = modeParam || "hybrid";

  if (!["hybrid", "semantic", "keyword"].includes(mode)) {
    return c.json({ error: "Invalid mode. Must be 'hybrid', 'semantic', or 'keyword'" }, 400);
  }

  const searchOptions = { reranker, similarityCutoff };
  const fuzzyOptions = { fuzzyFallback: fuzzyEnabled };

  try {
    let rankedResults: RankedResult[];
    let expandedQueries: { query: string; reason: string }[] = [];
    let ayahsRaw: AyahResult[] = [];
    let hadiths: HadithResult[] = [];

    let ayahSearchMeta: AyahSearchMeta = {
      collection: QDRANT_QURAN_COLLECTION,
      usedFallback: false,
      embeddingTechnique: "metadata-translation",
    };

    let refineQueryStats: ExpandedQueryStats[] = [];
    let totalAboveCutoff = 0;
    let rerankerTimedOut = false;

    const _refineTiming = { queryExpansion: 0, parallelSearches: 0, merge: 0, rerank: 0 };
    let _refineCandidates = {
      totalBeforeMerge: 0,
      afterMerge: { books: 0, ayahs: 0, hadiths: 0 },
      sentToReranker: 0,
    };
    let _refineQueryExpansionCached = false;

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

    // Request-scoped cache for book metadata
    const bookMetadataCache = new Map<string, { id: string; titleArabic: string; author: { nameArabic: string } }>();

    const databaseStats = await getDatabaseStats();

    const authorsPromise = bookId ? Promise.resolve([]) : searchAuthors(query, 5);
    const hybridOptions = { ...searchOptions, ...fuzzyOptions };
    const refineSearchOptions = { ...searchOptions, similarityCutoff: refineSimilarityCutoff };
    const refineHybridOptions = { ...refineSearchOptions, ...fuzzyOptions };

    const searchStrategy = getSearchStrategy(query);
    const shouldSkipKeyword = searchStrategy === 'semantic_only' || mode === "semantic";

    // Start graph search early
    const _graphStart = Date.now();
    const emptyGraphResult: GraphSearchResult = { entities: [], allSourceRefs: [], timingMs: 0 };
    const graphPromise: Promise<GraphSearchResult> = includeGraph
      ? searchEntities(normalizeArabicText(query))
          .then(res => { _timing.graph = Date.now() - _graphStart; return res; })
          .catch(() => emptyGraphResult)
      : Promise.resolve(emptyGraphResult);

    // ========================================================================
    // REFINE SEARCH
    // ========================================================================
    if (refine && mode === "hybrid" && !bookId) {
      const _refineStart = Date.now();

      // Step 1: Expand the query
      const _expansionStart = Date.now();
      const { queries: expandedRaw, cached: expansionCached } = await expandQueryWithCacheInfo(query, queryExpansionModel);
      _refineTiming.queryExpansion = Date.now() - _expansionStart;
      _refineQueryExpansionCached = expansionCached;

      const expanded = expandedRaw.map((exp, idx) => ({
        ...exp,
        weight: idx === 0 ? refineOriginalWeight : refineExpandedWeight,
      }));
      expandedQueries = expanded.map(e => ({ query: e.query, reason: e.reason }));

      // Step 2: Execute parallel searches for all expanded queries
      const _searchesStart = Date.now();
      const perQueryTimings: number[] = [];

      const querySearches = expanded.map(async (exp, queryIndex) => {
        const _queryStart = Date.now();
        const q = exp.query;
        const weight = exp.weight;

        const normalizedQ = normalizeArabicText(q);
        const shouldSkipSemantic = normalizedQ.replace(/\s/g, '').length < MIN_CHARS_FOR_SEMANTIC || hasQuotedPhrases(q);
        const qEmbedding = shouldSkipSemantic ? undefined : await generateEmbedding(normalizedQ, embeddingModel);

        const [bookSemantic, bookKeyword] = await Promise.all([
          semanticSearch(q, refineBookPerQuery, null, refineSimilarityCutoff, qEmbedding, pageCollection, embeddingModel).catch(() => []),
          shouldSkipKeyword
            ? Promise.resolve([] as RankedResult[])
            : keywordSearchES(q, refineBookPerQuery, null, fuzzyOptions).catch(() => []),
        ]);

        const mergedBooks = mergeWithRRF(bookSemantic, bookKeyword, q);

        let ayahResults: AyahRankedResult[] = [];
        let hadithResults: HadithRankedResult[] = [];

        if (shouldSkipKeyword) {
          const defaultMeta: AyahSearchMeta = { collection: quranCollection, usedFallback: false, embeddingTechnique: "metadata-translation" };
          ayahResults = includeQuran
            ? (await searchAyahsSemantic(q, refineAyahPerQuery, refineSimilarityCutoff, qEmbedding, quranCollection, embeddingModel).catch(() => ({ results: [], meta: defaultMeta }))).results
            : [];
          hadithResults = includeHadith
            ? await searchHadithsSemantic(q, refineHadithPerQuery, refineSimilarityCutoff, qEmbedding, hadithCollection, embeddingModel).catch(() => [])
            : [];
        } else {
          const refineHybridOptionsWithEmbedding = {
            ...refineHybridOptions,
            reranker: "none" as RerankerType,
            precomputedEmbedding: qEmbedding,
            quranCollection,
            hadithCollection,
            embeddingModel,
          };
          ayahResults = includeQuran
            ? await searchAyahsHybrid(q, refineAyahPerQuery, refineHybridOptionsWithEmbedding).catch(() => [])
            : [];
          hadithResults = includeHadith
            ? await searchHadithsHybrid(q, refineHadithPerQuery, refineHybridOptionsWithEmbedding).catch(() => [])
            : [];
        }

        perQueryTimings[queryIndex] = Date.now() - _queryStart;

        return {
          books: { results: mergedBooks, weight },
          ayahs: { results: ayahResults as AyahRankedResult[], weight },
          hadiths: { results: hadithResults as HadithRankedResult[], weight },
        };
      });

      const allResults = await Promise.all(querySearches);
      _refineTiming.parallelSearches = Date.now() - _searchesStart;

      refineQueryStats = expanded.map((exp, idx) => ({
        query: exp.query,
        weight: exp.weight,
        docsRetrieved: allResults[idx].books.results.length +
                       allResults[idx].ayahs.results.length +
                       allResults[idx].hadiths.results.length,
        books: allResults[idx].books.results.length,
        ayahs: allResults[idx].ayahs.results.length,
        hadiths: allResults[idx].hadiths.results.length,
        searchTimeMs: perQueryTimings[idx],
      }));

      _refineCandidates.totalBeforeMerge = refineQueryStats.reduce((sum, q) => sum + q.docsRetrieved, 0);

      // Step 3: Merge and deduplicate
      const _mergeStart = Date.now();
      const mergedBooks = includeBooks ? mergeAndDeduplicateBooks(allResults.map(r => r.books)) : [];
      const mergedAyahs = includeQuran ? mergeAndDeduplicateAyahs(allResults.map(r => r.ayahs)) : [];
      const mergedHadiths = includeHadith ? mergeAndDeduplicateHadiths(allResults.map(r => r.hadiths)) : [];
      _refineTiming.merge = Date.now() - _mergeStart;

      _refineCandidates.afterMerge = {
        books: mergedBooks.length,
        ayahs: mergedAyahs.length,
        hadiths: mergedHadiths.length,
      };

      // Step 4: Unified cross-type reranking
      const _rerankStart = Date.now();
      const preRerankBookIds = [...new Set(mergedBooks.slice(0, 30).map((r) => r.bookId))];
      const preRerankBookMap = await getBookMetadataForReranking(preRerankBookIds, bookMetadataCache);

      const rerankLimits = { books: refineBookRerank, ayahs: refineAyahRerank, hadiths: refineHadithRerank };
      _refineCandidates.sentToReranker = Math.min(mergedBooks.length, rerankLimits.books) +
                                          Math.min(mergedAyahs.length, rerankLimits.ayahs) +
                                          Math.min(mergedHadiths.length, rerankLimits.hadiths);

      const unifiedResult = await rerankUnifiedRefine(
        query, mergedAyahs, mergedHadiths, mergedBooks, preRerankBookMap, rerankLimits, reranker
      );
      _refineTiming.rerank = Date.now() - _rerankStart;

      rerankerTimedOut = unifiedResult.timedOut;
      rankedResults = unifiedResult.books;
      ayahsRaw = unifiedResult.ayahs;
      hadiths = unifiedResult.hadiths;

    } else {
      // ========================================================================
      // STANDARD SEARCH
      // ========================================================================
      const normalizedQuery = normalizeArabicText(query);
      const shouldSkipSemantic = normalizedQuery.replace(/\s/g, '').length < MIN_CHARS_FOR_SEMANTIC || hasQuotedPhrases(query);
      const fetchLimit = mode === "hybrid" ? 50 : limit;
      const ayahLimit = Math.min(limit, 30);
      const hadithLimit = Math.min(limit, 30);

      // PHASE 1: Start keyword searches AND embedding generation in parallel
      const _embStart = Date.now();
      const embeddingPromise = shouldSkipSemantic
        ? Promise.resolve(undefined)
        : generateEmbedding(normalizedQuery, embeddingModel);

      const _kwBooksStart = Date.now();
      const keywordBooksPromise = (shouldSkipKeyword || !includeBooks)
        ? Promise.resolve([] as RankedResult[])
        : keywordSearchES(query, fetchLimit, bookId, fuzzyOptions)
            .then(res => { _timing.keyword.books = Date.now() - _kwBooksStart; return res; })
            .catch(() => [] as RankedResult[]);

      const _kwAyahsStart = Date.now();
      const keywordAyahsPromise = (shouldSkipKeyword || bookId || !includeQuran)
        ? Promise.resolve([] as AyahRankedResult[])
        : keywordSearchAyahsES(query, fetchLimit, fuzzyOptions)
            .then(res => { _timing.keyword.ayahs = Date.now() - _kwAyahsStart; return res; })
            .catch(() => [] as AyahRankedResult[]);

      const _kwHadithsStart = Date.now();
      const keywordHadithsPromise = (shouldSkipKeyword || bookId || !includeHadith)
        ? Promise.resolve([] as HadithRankedResult[])
        : keywordSearchHadithsES(query, fetchLimit, fuzzyOptions)
            .then(res => { _timing.keyword.hadiths = Date.now() - _kwHadithsStart; return res; })
            .catch(() => [] as HadithRankedResult[]);

      // Wait for embedding
      const queryEmbedding = await embeddingPromise;
      _timing.embedding = Date.now() - _embStart;

      // PHASE 2: Start semantic searches
      const _semBooksStart = Date.now();
      const semanticBooksPromise = (mode === "keyword" || !includeBooks)
        ? Promise.resolve([] as RankedResult[])
        : semanticSearch(query, fetchLimit, bookId, similarityCutoff, queryEmbedding, pageCollection, embeddingModel)
            .then(res => { _timing.semantic.books = Date.now() - _semBooksStart; return res; })
            .catch(() => [] as RankedResult[]);

      const _semAyahsStart = Date.now();
      const defaultAyahMeta: AyahSearchMeta = { collection: quranCollection, usedFallback: false, embeddingTechnique: "metadata-translation" };
      const semanticAyahsPromise = (mode === "keyword" || bookId || !includeQuran)
        ? Promise.resolve({ results: [] as AyahRankedResult[], meta: defaultAyahMeta })
        : searchAyahsSemantic(query, fetchLimit, similarityCutoff, queryEmbedding, quranCollection, embeddingModel)
            .then(res => { _timing.semantic.ayahs = Date.now() - _semAyahsStart; return res; })
            .catch(() => ({ results: [] as AyahRankedResult[], meta: defaultAyahMeta }));

      const _semHadithsStart = Date.now();
      const semanticHadithsPromise = (mode === "keyword" || bookId || !includeHadith)
        ? Promise.resolve([] as HadithRankedResult[])
        : searchHadithsSemantic(query, fetchLimit, similarityCutoff, queryEmbedding, hadithCollection, embeddingModel)
            .then(res => { _timing.semantic.hadiths = Date.now() - _semHadithsStart; return res; })
            .catch(() => [] as HadithRankedResult[]);

      // PHASE 3: Wait for all searches and merge
      const [
        keywordBooksResults,
        keywordAyahsResults,
        keywordHadithsResults,
        semanticBooksResults,
        semanticAyahsSearchResult,
        semanticHadithsResults,
      ] = await Promise.all([
        keywordBooksPromise,
        keywordAyahsPromise,
        keywordHadithsPromise,
        semanticBooksPromise,
        semanticAyahsPromise,
        semanticHadithsPromise,
      ]);

      const semanticAyahsResults = semanticAyahsSearchResult.results;
      ayahSearchMeta = semanticAyahsSearchResult.meta;

      const _mergeStart = Date.now();

      // Books: merge semantic + keyword
      if (!includeBooks) {
        rankedResults = [];
      } else if (mode === "keyword") {
        rankedResults = keywordBooksResults.slice(0, limit);
      } else if (mode === "semantic") {
        rankedResults = semanticBooksResults.slice(0, limit);
      } else {
        const merged = mergeWithRRF(semanticBooksResults, keywordBooksResults, query);
        totalAboveCutoff = merged.length;
        rankedResults = merged.slice(0, bookLimit);
      }

      // Ayahs & Hadiths: merge semantic + keyword using shared helper
      ayahsRaw = mergeByMode({
        include: !bookId && includeQuran, mode, limit: ayahLimit,
        keywordResults: keywordAyahsResults, semanticResults: semanticAyahsResults,
        merge: () => mergeWithRRFGeneric(semanticAyahsResults, keywordAyahsResults, (a) => `${a.surahNumber}-${a.ayahNumber}`, query),
        normalizeKeyword: (items) => items.map(a => ({ ...a, score: normalizeBM25Score(a.bm25Score ?? a.score ?? 0) })),
      });

      hadiths = mergeByMode({
        include: !bookId && includeHadith, mode, limit: hadithLimit,
        keywordResults: keywordHadithsResults, semanticResults: semanticHadithsResults,
        merge: () => mergeWithRRFGeneric(semanticHadithsResults, keywordHadithsResults, (h) => `${h.collectionSlug}-${h.hadithNumber}`, query),
        normalizeKeyword: (items) => items.map(h => ({ ...h, score: normalizeBM25Score(h.bm25Score ?? h.score ?? 0) })),
      });

      _timing.merge = Date.now() - _mergeStart;
    }

    // Wait for graph + author search
    const graphResult = await graphPromise;
    const _authorSearchStart = Date.now();
    const authorsRaw = await authorsPromise;
    _timing.authorSearch = Date.now() - _authorSearchStart;
    const authors = authorsRaw;

    // Fetch translations in parallel
    const _translationsStart = Date.now();
    const [ayahTranslations, hadithTranslationsRaw, bookContentTranslationsRaw] = await Promise.all([
      (quranTranslation && quranTranslation !== "none" && ayahsRaw.length > 0)
        ? prisma.ayahTranslation.findMany({
            where: {
              language: quranTranslation,
              OR: ayahsRaw.map((a) => ({
                surahNumber: a.surahNumber,
                ayahNumber: a.ayahNumber,
              })),
            },
            select: { surahNumber: true, ayahNumber: true, text: true },
          })
        : Promise.resolve([]),
      (hadithTranslation && hadithTranslation !== "none" && hadiths.length > 0)
        ? prisma.hadithTranslation.findMany({
            where: {
              language: hadithTranslation,
              OR: hadiths.map((h) => ({
                bookId: h.bookId,
                hadithNumber: h.hadithNumber,
              })),
            },
            select: { bookId: true, hadithNumber: true, text: true },
          })
        : Promise.resolve([]),
      (bookContentTranslation && bookContentTranslation !== "none" && rankedResults.length > 0)
        ? prisma.pageTranslation.findMany({
            where: {
              language: bookContentTranslation,
              page: {
                OR: rankedResults.map((r) => ({
                  bookId: r.bookId,
                  pageNumber: r.pageNumber,
                })),
              },
            },
            select: {
              page: {
                select: { bookId: true, pageNumber: true, contentHtml: true },
              },
              paragraphs: true,
            },
          })
        : Promise.resolve([]),
    ]);
    _timing.translations = Date.now() - _translationsStart;

    // Merge ayah translations
    let ayahs = ayahsRaw;
    if (ayahTranslations.length > 0) {
      const translationMap = new Map(
        ayahTranslations.map((t) => [`${t.surahNumber}-${t.ayahNumber}`, t.text])
      );
      ayahs = ayahsRaw.map((ayah) => ({
        ...ayah,
        translation: translationMap.get(`${ayah.surahNumber}-${ayah.ayahNumber}`),
      }));
    }

    // Merge hadith translations
    if (hadithTranslationsRaw.length > 0) {
      const hadithTranslationMap = new Map(
        hadithTranslationsRaw.map((t) => [`${t.bookId}-${t.hadithNumber}`, t.text])
      );
      hadiths = hadiths.map((hadith) => ({
        ...hadith,
        translation: hadithTranslationMap.get(`${hadith.bookId}-${hadith.hadithNumber}`),
      }));
    }

    // Merge book content translations
    type BookTranslationData = {
      paragraphs: Array<{ index: number; translation: string }>;
      contentHtml: string;
    };
    const bookContentTranslationMap = new Map<string, BookTranslationData>();
    if (bookContentTranslationsRaw.length > 0) {
      for (const t of bookContentTranslationsRaw) {
        const key = `${t.page.bookId}-${t.page.pageNumber}`;
        bookContentTranslationMap.set(key, {
          paragraphs: t.paragraphs as Array<{ index: number; translation: string }>,
          contentHtml: t.page.contentHtml,
        });
      }

      rankedResults = rankedResults.map((r) => {
        const translationData = bookContentTranslationMap.get(`${r.bookId}-${r.pageNumber}`);
        if (!translationData) return r;

        const { paragraphs: translations, contentHtml } = translationData;
        const pageParagraphs = extractParagraphTexts(contentHtml);
        const matchIndex = findMatchingParagraphIndex(r.textSnippet, pageParagraphs);
        const matchedTranslation = translations.find((t) => t.index === matchIndex);

        return { ...r, contentTranslation: matchedTranslation?.translation || null };
      });
    }

    // Limit final results
    rankedResults = rankedResults.slice(0, limit);

    // Fetch urlPageIndex
    if (rankedResults.length > 0) {
      const pages = await prisma.page.findMany({
        where: {
          OR: rankedResults.map(r => ({ bookId: r.bookId, pageNumber: r.pageNumber })),
        },
        select: { bookId: true, pageNumber: true, urlPageIndex: true },
      });

      const pageMap = new Map(
        pages.map(p => [`${p.bookId}-${p.pageNumber}`, p.urlPageIndex])
      );

      rankedResults = rankedResults.map(r => ({
        ...r,
        urlPageIndex: pageMap.get(`${r.bookId}-${r.pageNumber}`) || String(r.pageNumber),
      }));
    }

    // Fetch book details
    const bookIds = [...new Set(rankedResults.map((r) => r.bookId))];
    const _bookMetaStart = Date.now();
    const booksRaw = await prisma.book.findMany({
      where: { id: { in: bookIds } },
      select: {
        id: true,
        titleArabic: true,
        titleLatin: true,
        filename: true,
        publicationYearHijri: true,
        author: {
          select: { nameArabic: true, nameLatin: true, deathDateHijri: true },
        },
        ...(bookTitleLang && bookTitleLang !== "none" && bookTitleLang !== "transliteration"
          ? {
              titleTranslations: {
                where: { language: bookTitleLang },
                select: { title: true },
                take: 1,
              },
            }
          : {}),
      },
    });
    _timing.bookMetadata = Date.now() - _bookMetaStart;

    const books = booksRaw.map((book) => {
      const { titleTranslations, ...rest } = book as typeof book & {
        titleTranslations?: { title: string }[];
      };
      return { ...rest, titleTranslated: titleTranslations?.[0]?.title || null };
    });

    // ========================================================================
    // Graph context resolution
    // ========================================================================
    let graphContext: GraphContext | undefined;
    if (includeGraph && graphResult.entities.length > 0) {
      try {
        const _graphResolveStart = Date.now();
        const [resolvedSources, resolvedMentions] = await Promise.all([
          resolveSources(graphResult.allSourceRefs),
          resolveGraphMentions(graphResult.entities),
        ]);

        const contextEntities: GraphContextEntity[] = graphResult.entities.map((entity, i) => ({
          id: entity.id,
          type: entity.type,
          nameArabic: entity.nameArabic,
          nameEnglish: entity.nameEnglish,
          descriptionArabic: entity.descriptionArabic,
          descriptionEnglish: entity.descriptionEnglish,
          sources: entity.sources
            .map((s) => resolvedSources.get(`${s.type}:${s.ref}`))
            .filter((s): s is ResolvedSource => s !== undefined),
          relationships: entity.relationships.map((rel) => ({
            type: rel.type,
            targetNameArabic: rel.targetNameArabic,
            targetNameEnglish: rel.targetNameEnglish,
            description: rel.description,
            sources: rel.sources
              .map((s) => resolvedSources.get(`${s.type}:${s.ref}`))
              .filter((s): s is ResolvedSource => s !== undefined),
          })),
          mentionedIn: resolvedMentions[i] || [],
        }));

        graphContext = {
          entities: contextEntities,
          coverage: "partial",
          timingMs: graphResult.timingMs + (Date.now() - _graphResolveStart),
        };

        // Graph confirmation boost for ayahs
        const graphQuranRefs = new Set<string>();
        for (const entity of graphResult.entities) {
          for (const s of entity.sources) {
            if (s.type === "quran") graphQuranRefs.add(s.ref);
          }
          for (const m of entity.mentionedIn) {
            graphQuranRefs.add(m.ayahGroupId);
          }
        }

        ayahsRaw = ayahsRaw.map(ayah => {
          const ayahRef = `${ayah.surahNumber}:${ayah.ayahNumber}`;
          if (graphQuranRefs.has(ayahRef)) {
            return { ...ayah, score: Math.min(1.0, ayah.score + 0.05), graphConfirmed: true } as AyahResult & { graphConfirmed: boolean };
          }
          return ayah;
        }).sort((a, b) => b.score - a.score);
      } catch (err) {
        console.error("[GraphContext] resolution error:", err);
      }
    }

    // Format results
    const bookMap = new Map(books.map((b) => [b.id, b]));

    const results: SearchResult[] = rankedResults.map((result, index) => {
      const matchType = getMatchType(result);
      const book = bookMap.get(result.bookId) || null;
      const r = result as typeof result & { fusedScore?: number };

      const scoreByMode: Record<string, number> = {
        hybrid: r.fusedScore ?? result.semanticScore ?? calculateRRFScore([result.semanticRank, result.keywordRank]),
        semantic: result.semanticScore || 0,
        keyword: result.keywordScore || 0,
      };
      const score = scoreByMode[mode] ?? 0;

      return {
        score,
        semanticScore: result.semanticScore,
        rank: index + 1,
        bookId: result.bookId,
        pageNumber: result.pageNumber,
        volumeNumber: result.volumeNumber,
        textSnippet: result.textSnippet,
        highlightedSnippet: result.highlightedSnippet,
        matchType,
        urlPageIndex: result.urlPageIndex,
        shamelaUrl: result.shamelaUrl || generateShamelaPageUrl(result.bookId, result.pageNumber),
        contentTranslation: result.contentTranslation,
        book,
      };
    });

    // Build unified ranking for top results breakdown
    const unifiedResults: Array<{
      type: 'book' | 'quran' | 'hadith';
      score: number;
      data: SearchResult | AyahResult | HadithResult;
      rankedData?: RankedResult | AyahRankedResult | HadithRankedResult;
    }> = [];

    for (let i = 0; i < results.length; i++) {
      unifiedResults.push({ type: 'book', score: results[i].score, data: results[i], rankedData: rankedResults[i] });
    }
    for (const a of ayahs) {
      const rankedAyah = a as AyahRankedResult & { fusedScore?: number };
      unifiedResults.push({ type: 'quran', score: rankedAyah.fusedScore ?? a.score, data: a, rankedData: a as AyahRankedResult });
    }
    for (const h of hadiths) {
      const rankedHadith = h as HadithRankedResult & { fusedScore?: number };
      unifiedResults.push({ type: 'hadith', score: rankedHadith.fusedScore ?? h.score, data: h, rankedData: h as HadithRankedResult });
    }

    unifiedResults.sort((a, b) => b.score - a.score);

    const top5Breakdown: TopResultBreakdown[] = unifiedResults
      .slice(0, 5)
      .map((item, index) => {
        const rank = index + 1;
        const hasSemantic = item.data.semanticScore != null;
        const hasKeyword = item.rankedData?.bm25Score != null;
        const matchType: 'semantic' | 'keyword' | 'both' = hasSemantic && hasKeyword ? 'both' : hasSemantic ? 'semantic' : 'keyword';

        let title: string;
        if (item.type === 'book') {
          const r = item.data as SearchResult;
          title = r.book?.titleArabic?.slice(0, 50) || `Book ${r.bookId}`;
        } else if (item.type === 'quran') {
          const a = item.data as AyahResult;
          title = `${a.surahNameArabic} ${a.ayahNumber}`;
        } else {
          const h = item.data as HadithResult;
          title = `${h.collectionNameArabic} ${h.hadithNumber}`;
        }

        return {
          rank, type: item.type, title, matchType,
          keywordScore: item.rankedData?.bm25Score ?? null,
          semanticScore: item.data.semanticScore ?? null,
          finalScore: item.data.score,
        };
      });

    // Build debug stats
    const debugStats: SearchDebugStats = {
      databaseStats,
      searchParams: {
        mode,
        cutoff: similarityCutoff,
        totalAboveCutoff: totalAboveCutoff || results.length + ayahs.length + hadiths.length,
        totalShown: results.length + ayahs.length + hadiths.length,
      },
      algorithm: {
        fusionMethod: shouldSkipKeyword ? 'semantic_only' : 'weighted_combination',
        fusionWeights: shouldSkipKeyword
          ? { semantic: 1.0, keyword: 0 }
          : { semantic: SEMANTIC_WEIGHT, keyword: KEYWORD_WEIGHT },
        keywordEngine: 'elasticsearch',
        bm25Params: { k1: 1.2, b: 0.75, normK: 5 },
        rrfK: RRF_K,
        embeddingModel: getEmbeddingModelName(embeddingModel),
        embeddingDimensions: embeddingDimensions,
        rerankerModel: reranker === 'none' ? null : reranker,
        queryExpansionModel: refine ? getQueryExpansionModelId(queryExpansionModel) : null,
        quranCollection: ayahSearchMeta.collection,
        quranCollectionFallback: ayahSearchMeta.usedFallback,
        embeddingTechnique: ayahSearchMeta.embeddingTechnique,
      },
      topResultsBreakdown: top5Breakdown,
      ...(refine && refineQueryStats.length > 0 && {
        refineStats: {
          expandedQueries: refineQueryStats,
          originalQueryDocs: refineQueryStats.find(q => q.weight === 1.0)?.docsRetrieved || 0,
          timing: {
            queryExpansion: _refineTiming.queryExpansion,
            parallelSearches: _refineTiming.parallelSearches,
            merge: _refineTiming.merge,
            rerank: _refineTiming.rerank,
            total: _refineTiming.queryExpansion + _refineTiming.parallelSearches + _refineTiming.merge + _refineTiming.rerank,
          },
          candidates: _refineCandidates,
          queryExpansionCached: _refineQueryExpansionCached,
        },
      }),
      ...(rerankerTimedOut && { rerankerTimedOut: true }),
      timing: {
        total: Date.now() - _timing.start,
        embedding: _timing.embedding,
        semantic: _timing.semantic,
        keyword: _timing.keyword,
        merge: _timing.merge,
        authorSearch: _timing.authorSearch,
        ...(_timing.rerank > 0 && { rerank: _timing.rerank }),
        translations: _timing.translations,
        bookMetadata: _timing.bookMetadata,
        ...(_timing.graph > 0 && { graph: _timing.graph }),
      },
    };

    return c.json({
      query,
      mode,
      count: results.length,
      results,
      authors,
      ayahs,
      hadiths,
      ...(process.env.NODE_ENV !== "production" && { debugStats }),
      ...(graphContext && { graphContext }),
      ...(refine && {
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
