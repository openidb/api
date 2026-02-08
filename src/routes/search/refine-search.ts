import { generateEmbedding, normalizeArabicText } from "../../embeddings";
import { startTimer } from "../../utils/timing";
import { keywordSearchES } from "../../search/elasticsearch-search";
import { shouldSkipSemanticSearch, getSearchStrategy } from "./query-utils";
import { mergeWithRRF, mergeAndDeduplicateBooks, mergeAndDeduplicateAyahs, mergeAndDeduplicateHadiths } from "./fusion";
import { rerankUnifiedRefine } from "./rerankers";
import { semanticSearch, searchAyahsSemantic, searchAyahsHybrid, searchHadithsSemantic, searchHadithsHybrid } from "./engines";
import { expandQueryWithCacheInfo } from "./refine";
import { getBookMetadataForReranking } from "./helpers";
import type { SearchParams } from "./params";
import type {
  RerankerType,
  RankedResult,
  AyahResult,
  HadithResult,
  AyahRankedResult,
  HadithRankedResult,
  AyahSearchMeta,
  ExpandedQueryStats,
} from "./types";

export interface RefineSearchResult {
  rankedResults: RankedResult[];
  ayahsRaw: AyahResult[];
  hadiths: HadithResult[];
  expandedQueries: { query: string; reason: string }[];
  rerankerTimedOut: boolean;
  refineStats: {
    queryStats: ExpandedQueryStats[];
    candidates: {
      totalBeforeMerge: number;
      afterMerge: { books: number; ayahs: number; hadiths: number };
      sentToReranker: number;
    };
    queryExpansionCached: boolean;
    timing: { queryExpansion: number; parallelSearches: number; merge: number; rerank: number };
  };
}

export async function executeRefineSearch(params: SearchParams): Promise<RefineSearchResult> {
  const {
    query, includeQuran, includeHadith, includeBooks,
    fuzzyEnabled, embeddingModel, pageCollection, quranCollection, hadithCollection,
    reranker, refineSimilarityCutoff,
    refineOriginalWeight, refineExpandedWeight,
    refineBookPerQuery, refineAyahPerQuery, refineHadithPerQuery,
    refineBookRerank, refineAyahRerank, refineHadithRerank,
    queryExpansionModel, similarityCutoff,
  } = params;

  const fuzzyOptions = { fuzzyFallback: fuzzyEnabled };
  const searchStrategy = getSearchStrategy(query);
  const shouldSkipKeyword = searchStrategy === 'semantic_only';
  const refineSearchOptions = { reranker, similarityCutoff: refineSimilarityCutoff };
  const refineHybridOptions = { ...refineSearchOptions, fuzzyFallback: fuzzyEnabled };
  const bookMetadataCache = new Map<string, { id: string; titleArabic: string; author: { nameArabic: string } }>();

  const _refineTiming = { queryExpansion: 0, parallelSearches: 0, merge: 0, rerank: 0 };

  // Step 1: Expand the query
  const expansionTimer = startTimer();
  const { queries: expandedRaw, cached: expansionCached } = await expandQueryWithCacheInfo(query, queryExpansionModel);
  _refineTiming.queryExpansion = expansionTimer();

  const expanded = expandedRaw.map((exp, idx) => ({
    ...exp,
    weight: idx === 0 ? refineOriginalWeight : refineExpandedWeight,
  }));
  const expandedQueries = expanded.map(e => ({ query: e.query, reason: e.reason }));

  // Step 2: Execute parallel searches for all expanded queries
  const searchesTimer = startTimer();
  const perQueryTimings: number[] = [];

  const querySearches = expanded.map(async (exp, queryIndex) => {
    const queryTimer = startTimer();
    const q = exp.query;
    const weight = exp.weight;

    const normalizedQ = normalizeArabicText(q);
    const shouldSkipSemantic = shouldSkipSemanticSearch(q);
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

    perQueryTimings[queryIndex] = queryTimer();

    return {
      books: { results: mergedBooks, weight },
      ayahs: { results: ayahResults as AyahRankedResult[], weight },
      hadiths: { results: hadithResults as HadithRankedResult[], weight },
    };
  });

  const allResults = await Promise.all(querySearches);
  _refineTiming.parallelSearches = searchesTimer();

  const queryStats = expanded.map((exp, idx) => ({
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

  const totalBeforeMerge = queryStats.reduce((sum, q) => sum + q.docsRetrieved, 0);

  // Step 3: Merge and deduplicate
  const mergeTimer = startTimer();
  const mergedBooks = includeBooks ? mergeAndDeduplicateBooks(allResults.map(r => r.books)) : [];
  const mergedAyahs = includeQuran ? mergeAndDeduplicateAyahs(allResults.map(r => r.ayahs)) : [];
  const mergedHadiths = includeHadith ? mergeAndDeduplicateHadiths(allResults.map(r => r.hadiths)) : [];
  _refineTiming.merge = mergeTimer();

  const afterMerge = { books: mergedBooks.length, ayahs: mergedAyahs.length, hadiths: mergedHadiths.length };

  // Step 4: Unified cross-type reranking
  const rerankTimer = startTimer();
  const preRerankBookIds = [...new Set(mergedBooks.slice(0, 30).map((r) => r.bookId))];
  const preRerankBookMap = await getBookMetadataForReranking(preRerankBookIds, bookMetadataCache);

  const rerankLimits = { books: refineBookRerank, ayahs: refineAyahRerank, hadiths: refineHadithRerank };
  const sentToReranker = Math.min(mergedBooks.length, rerankLimits.books) +
                          Math.min(mergedAyahs.length, rerankLimits.ayahs) +
                          Math.min(mergedHadiths.length, rerankLimits.hadiths);

  const unifiedResult = await rerankUnifiedRefine(
    query, mergedAyahs, mergedHadiths, mergedBooks, preRerankBookMap, rerankLimits, reranker
  );
  _refineTiming.rerank = rerankTimer();

  return {
    rankedResults: unifiedResult.books,
    ayahsRaw: unifiedResult.ayahs,
    hadiths: unifiedResult.hadiths,
    expandedQueries,
    rerankerTimedOut: unifiedResult.timedOut,
    refineStats: {
      queryStats,
      candidates: { totalBeforeMerge, afterMerge, sentToReranker },
      queryExpansionCached: expansionCached,
      timing: _refineTiming,
    },
  };
}
