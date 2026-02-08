import { prisma } from "../../db";
import { startTimer } from "../../utils/timing";
import { getEmbeddingModelName } from "../../embeddings";
import {
  searchEntities,
  resolveSources,
  resolveGraphMentions,
  type GraphSearchResult,
  type GraphContext,
  type GraphContextEntity,
  type ResolvedSource,
} from "../../graph/search";
import { normalizeArabicText } from "../../embeddings";
import { generateShamelaPageUrl } from "../../utils/source-urls";
import { calculateRRFScore, getMatchType } from "./fusion";
import { getSearchStrategy } from "./query-utils";
import { RRF_K, SEMANTIC_WEIGHT, KEYWORD_WEIGHT } from "./config";
import { getQueryExpansionModelId } from "./refine";
import { getDatabaseStats } from "./helpers";
import type { SearchParams } from "./params";
import type {
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

export async function startGraphSearch(
  query: string,
  includeGraph: boolean,
  timingRef: { graph: number },
): Promise<GraphSearchResult> {
  const emptyGraphResult: GraphSearchResult = { entities: [], allSourceRefs: [], timingMs: 0 };
  if (!includeGraph) return emptyGraphResult;

  const graphTimer = startTimer();
  return searchEntities(normalizeArabicText(query))
    .then(res => { timingRef.graph = graphTimer(); return res; })
    .catch(() => emptyGraphResult);
}

export async function resolveGraphContext(
  graphResult: GraphSearchResult,
  includeGraph: boolean,
  ayahsRaw: AyahResult[],
): Promise<{ graphContext?: GraphContext; ayahsRaw: AyahResult[] }> {
  if (!includeGraph || graphResult.entities.length === 0) {
    return { ayahsRaw };
  }

  try {
    const graphResolveTimer = startTimer();
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

    const graphContext: GraphContext = {
      entities: contextEntities,
      coverage: "partial",
      timingMs: graphResult.timingMs + graphResolveTimer(),
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

    const boostedAyahs = ayahsRaw.map(ayah => {
      const ayahRef = `${ayah.surahNumber}:${ayah.ayahNumber}`;
      if (graphQuranRefs.has(ayahRef)) {
        return { ...ayah, score: Math.min(1.0, ayah.score + 0.05), graphConfirmed: true } as AyahResult & { graphConfirmed: boolean };
      }
      return ayah;
    }).sort((a, b) => b.score - a.score);

    return { graphContext, ayahsRaw: boostedAyahs };
  } catch (err) {
    console.error("[GraphContext] resolution error:", err);
    return { ayahsRaw };
  }
}

export async function fetchBookDetails(
  rankedResults: RankedResult[],
  bookTitleLang: string | undefined,
): Promise<{
  results: RankedResult[];
  books: Array<{ id: string; titleArabic: string; titleLatin: string; filename: string; publicationYearHijri: string | null; titleTranslated: string | null; author: { nameArabic: string; nameLatin: string; deathDateHijri: string | null } }>;
  timing: { bookMetadata: number };
}> {
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
  const bookMetaTimer = startTimer();
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
  const bookMetadataTime = bookMetaTimer();

  const books = booksRaw.map((book) => {
    const { titleTranslations, ...rest } = book as typeof book & {
      titleTranslations?: { title: string }[];
    };
    return { ...rest, titleTranslated: titleTranslations?.[0]?.title || null };
  });

  return { results: rankedResults, books, timing: { bookMetadata: bookMetadataTime } };
}

export function formatSearchResults(
  rankedResults: RankedResult[],
  books: Array<{ id: string; titleArabic: string; titleLatin: string; filename: string; titleTranslated: string | null; author: { nameArabic: string; nameLatin: string; deathDateHijri?: string | null } }>,
  mode: string,
): SearchResult[] {
  const bookMap = new Map(books.map((b) => [b.id, b]));

  return rankedResults.map((result, index) => {
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
}

export function buildTopResultsBreakdown(
  results: SearchResult[],
  rankedResults: RankedResult[],
  ayahs: AyahResult[],
  hadiths: HadithResult[],
): TopResultBreakdown[] {
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

  return unifiedResults.slice(0, 5).map((item, index) => {
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
}

export async function buildDebugStats(
  params: SearchParams,
  results: SearchResult[],
  ayahs: AyahResult[],
  hadiths: HadithResult[],
  rankedResults: RankedResult[],
  ayahSearchMeta: AyahSearchMeta,
  totalAboveCutoff: number,
  rerankerTimedOut: boolean,
  timing: {
    start: number;
    embedding: number;
    semantic: { books: number; ayahs: number; hadiths: number };
    keyword: { books: number; ayahs: number; hadiths: number };
    merge: number;
    authorSearch: number;
    rerank: number;
    translations: number;
    bookMetadata: number;
    graph: number;
  },
  refineStats?: {
    queryStats: ExpandedQueryStats[];
    candidates: { totalBeforeMerge: number; afterMerge: { books: number; ayahs: number; hadiths: number }; sentToReranker: number };
    queryExpansionCached: boolean;
    timing: { queryExpansion: number; parallelSearches: number; merge: number; rerank: number };
  },
): Promise<SearchDebugStats> {
  const { mode, similarityCutoff, reranker, refine, queryExpansionModel, embeddingModel, embeddingDimensions, query } = params;
  const shouldSkipKeyword = getSearchStrategy(query) === 'semantic_only' || mode === "semantic";
  const databaseStats = await getDatabaseStats();

  const top5Breakdown = buildTopResultsBreakdown(results, rankedResults, ayahs, hadiths);

  return {
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
      embeddingDimensions,
      rerankerModel: reranker === 'none' ? null : reranker,
      queryExpansionModel: refine ? getQueryExpansionModelId(queryExpansionModel) : null,
      quranCollection: ayahSearchMeta.collection,
      quranCollectionFallback: ayahSearchMeta.usedFallback,
      embeddingTechnique: ayahSearchMeta.embeddingTechnique,
    },
    topResultsBreakdown: top5Breakdown,
    ...(refine && refineStats && refineStats.queryStats.length > 0 && {
      refineStats: {
        expandedQueries: refineStats.queryStats,
        originalQueryDocs: refineStats.queryStats.find(q => q.weight === 1.0)?.docsRetrieved || 0,
        timing: {
          queryExpansion: refineStats.timing.queryExpansion,
          parallelSearches: refineStats.timing.parallelSearches,
          merge: refineStats.timing.merge,
          rerank: refineStats.timing.rerank,
          total: refineStats.timing.queryExpansion + refineStats.timing.parallelSearches + refineStats.timing.merge + refineStats.timing.rerank,
        },
        candidates: refineStats.candidates,
        queryExpansionCached: refineStats.queryExpansionCached,
      },
    }),
    ...(rerankerTimedOut && { rerankerTimedOut: true }),
    timing: {
      total: Date.now() - timing.start,
      embedding: timing.embedding,
      semantic: timing.semantic,
      keyword: timing.keyword,
      merge: timing.merge,
      authorSearch: timing.authorSearch,
      ...(timing.rerank > 0 && { rerank: timing.rerank }),
      translations: timing.translations,
      bookMetadata: timing.bookMetadata,
      ...(timing.graph > 0 && { graph: timing.graph }),
    },
  };
}
