import { generateEmbedding, normalizeArabicText } from "../../embeddings";
import { normalizeBM25Score } from "../../search/bm25";
import {
  keywordSearchES,
  keywordSearchHadithsES,
  keywordSearchAyahsES,
} from "../../search/elasticsearch-search";
import { MIN_CHARS_FOR_SEMANTIC, STANDARD_FETCH_LIMIT, DEFAULT_AYAH_LIMIT, DEFAULT_HADITH_LIMIT } from "./config";
import { hasQuotedPhrases, getSearchStrategy } from "./query-utils";
import { mergeWithRRF, mergeWithRRFGeneric } from "./fusion";
import { semanticSearch, searchAyahsSemantic, searchHadithsSemantic } from "./engines";
import type { SearchParams } from "./params";
import type {
  RankedResult,
  AyahResult,
  HadithResult,
  AyahRankedResult,
  HadithRankedResult,
  AyahSearchMeta,
} from "./types";

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

export interface StandardSearchResult {
  rankedResults: RankedResult[];
  ayahsRaw: AyahResult[];
  hadiths: HadithResult[];
  ayahSearchMeta: AyahSearchMeta;
  totalAboveCutoff: number;
  timing: {
    embedding: number;
    semantic: { books: number; ayahs: number; hadiths: number };
    keyword: { books: number; ayahs: number; hadiths: number };
    merge: number;
  };
}

export async function executeStandardSearch(params: SearchParams): Promise<StandardSearchResult> {
  const {
    query, mode, bookId, limit, bookLimit, similarityCutoff,
    includeQuran, includeHadith, includeBooks,
    fuzzyEnabled, embeddingModel, pageCollection, quranCollection, hadithCollection,
  } = params;

  const fuzzyOptions = { fuzzyFallback: fuzzyEnabled };
  const searchStrategy = getSearchStrategy(query);
  const shouldSkipKeyword = searchStrategy === 'semantic_only' || mode === "semantic";

  const normalizedQuery = normalizeArabicText(query);
  const shouldSkipSemantic = normalizedQuery.replace(/\s/g, '').length < MIN_CHARS_FOR_SEMANTIC || hasQuotedPhrases(query);
  const fetchLimit = mode === "hybrid" ? STANDARD_FETCH_LIMIT : limit;
  const ayahLimit = Math.min(limit, DEFAULT_AYAH_LIMIT);
  const hadithLimit = Math.min(limit, DEFAULT_HADITH_LIMIT);

  const timing = {
    embedding: 0,
    semantic: { books: 0, ayahs: 0, hadiths: 0 },
    keyword: { books: 0, ayahs: 0, hadiths: 0 },
    merge: 0,
  };

  // PHASE 1: Start keyword searches AND embedding generation in parallel
  const _embStart = Date.now();
  const embeddingPromise = shouldSkipSemantic
    ? Promise.resolve(undefined)
    : generateEmbedding(normalizedQuery, embeddingModel);

  const _kwBooksStart = Date.now();
  const keywordBooksPromise = (shouldSkipKeyword || !includeBooks)
    ? Promise.resolve([] as RankedResult[])
    : keywordSearchES(query, fetchLimit, bookId, fuzzyOptions)
        .then(res => { timing.keyword.books = Date.now() - _kwBooksStart; return res; })
        .catch(() => [] as RankedResult[]);

  const _kwAyahsStart = Date.now();
  const keywordAyahsPromise = (shouldSkipKeyword || bookId || !includeQuran)
    ? Promise.resolve([] as AyahRankedResult[])
    : keywordSearchAyahsES(query, fetchLimit, fuzzyOptions)
        .then(res => { timing.keyword.ayahs = Date.now() - _kwAyahsStart; return res; })
        .catch(() => [] as AyahRankedResult[]);

  const _kwHadithsStart = Date.now();
  const keywordHadithsPromise = (shouldSkipKeyword || bookId || !includeHadith)
    ? Promise.resolve([] as HadithRankedResult[])
    : keywordSearchHadithsES(query, fetchLimit, fuzzyOptions)
        .then(res => { timing.keyword.hadiths = Date.now() - _kwHadithsStart; return res; })
        .catch(() => [] as HadithRankedResult[]);

  // Wait for embedding
  const queryEmbedding = await embeddingPromise;
  timing.embedding = Date.now() - _embStart;

  // PHASE 2: Start semantic searches
  const _semBooksStart = Date.now();
  const semanticBooksPromise = (mode === "keyword" || !includeBooks)
    ? Promise.resolve([] as RankedResult[])
    : semanticSearch(query, fetchLimit, bookId, similarityCutoff, queryEmbedding, pageCollection, embeddingModel)
        .then(res => { timing.semantic.books = Date.now() - _semBooksStart; return res; })
        .catch(() => [] as RankedResult[]);

  const defaultAyahMeta: AyahSearchMeta = { collection: quranCollection, usedFallback: false, embeddingTechnique: "metadata-translation" };

  const _semAyahsStart = Date.now();
  const semanticAyahsPromise = (mode === "keyword" || bookId || !includeQuran)
    ? Promise.resolve({ results: [] as AyahRankedResult[], meta: defaultAyahMeta })
    : searchAyahsSemantic(query, fetchLimit, similarityCutoff, queryEmbedding, quranCollection, embeddingModel)
        .then(res => { timing.semantic.ayahs = Date.now() - _semAyahsStart; return res; })
        .catch(() => ({ results: [] as AyahRankedResult[], meta: defaultAyahMeta }));

  const _semHadithsStart = Date.now();
  const semanticHadithsPromise = (mode === "keyword" || bookId || !includeHadith)
    ? Promise.resolve([] as HadithRankedResult[])
    : searchHadithsSemantic(query, fetchLimit, similarityCutoff, queryEmbedding, hadithCollection, embeddingModel)
        .then(res => { timing.semantic.hadiths = Date.now() - _semHadithsStart; return res; })
        .catch(() => [] as HadithRankedResult[]);

  // PHASE 3: Wait for all searches and merge
  const [
    keywordBooksResults, keywordAyahsResults, keywordHadithsResults,
    semanticBooksResults, semanticAyahsSearchResult, semanticHadithsResults,
  ] = await Promise.all([
    keywordBooksPromise, keywordAyahsPromise, keywordHadithsPromise,
    semanticBooksPromise, semanticAyahsPromise, semanticHadithsPromise,
  ]);

  const semanticAyahsResults = semanticAyahsSearchResult.results;
  const ayahSearchMeta = semanticAyahsSearchResult.meta;

  const _mergeStart = Date.now();

  let rankedResults: RankedResult[];
  let totalAboveCutoff = 0;

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

  const ayahsRaw = mergeByMode({
    include: !bookId && includeQuran, mode, limit: ayahLimit,
    keywordResults: keywordAyahsResults, semanticResults: semanticAyahsResults,
    merge: () => mergeWithRRFGeneric(semanticAyahsResults, keywordAyahsResults, (a) => `${a.surahNumber}-${a.ayahNumber}`, query),
    normalizeKeyword: (items) => items.map(a => ({ ...a, score: normalizeBM25Score(a.bm25Score ?? a.score ?? 0) })),
  });

  const hadiths = mergeByMode({
    include: !bookId && includeHadith, mode, limit: hadithLimit,
    keywordResults: keywordHadithsResults, semanticResults: semanticHadithsResults,
    merge: () => mergeWithRRFGeneric(semanticHadithsResults, keywordHadithsResults, (h) => `${h.collectionSlug}-${h.hadithNumber}`, query),
    normalizeKeyword: (items) => items.map(h => ({ ...h, score: normalizeBM25Score(h.bm25Score ?? h.score ?? 0) })),
  });

  timing.merge = Date.now() - _mergeStart;

  return { rankedResults, ayahsRaw, hadiths, ayahSearchMeta, totalAboveCutoff, timing };
}
