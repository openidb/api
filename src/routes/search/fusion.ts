import { normalizeBM25Score } from "../../search/bm25";
import {
  RRF_K, SEMANTIC_WEIGHT, KEYWORD_WEIGHT, FLOAT_TOLERANCE, BM25_NORM_K,
} from "./config";
import type { RankedResult, AyahRankedResult, HadithRankedResult } from "./types";

/**
 * Reciprocal Rank Fusion score calculation
 */
export function calculateRRFScore(ranks: (number | undefined)[]): number {
  return ranks.reduce((sum: number, rank) => {
    if (rank === undefined) return sum;
    return sum + 1 / (RRF_K + rank);
  }, 0);
}

/**
 * Calculate fused score from semantic and keyword signals.
 * Keyword search only runs for Arabic queries, so weights are tuned for
 * Arabic text where BM25 exact matching is highly reliable.
 */
function calculateFusedScore(
  hasSemantic: boolean,
  hasKeyword: boolean,
  semanticScore: number,
  bm25Score: number | undefined,
  keywordScore: number | undefined,
): number {
  if (hasSemantic && hasKeyword) {
    return SEMANTIC_WEIGHT * semanticScore + KEYWORD_WEIGHT * normalizeBM25Score(bm25Score ?? 0, BM25_NORM_K);
  }
  if (hasSemantic) return semanticScore;
  return normalizeBM25Score(bm25Score ?? keywordScore ?? 0, BM25_NORM_K);
}

/**
 * Generic RRF merge function for any content type with weighted score fusion.
 * getKeywordScore extracts the keyword score from an item (defaults to item.score).
 * onMerge is called when a keyword result matches an existing semantic result.
 */
export function mergeWithRRFGeneric<T extends { semanticRank?: number; keywordRank?: number; semanticScore?: number; score?: number; tsRank?: number; bm25Score?: number }>(
  semanticResults: T[],
  keywordResults: T[],
  getKey: (item: T) => string,
  query: string,
  opts?: {
    getKeywordScore?: (item: T) => number | undefined;
    onMerge?: (existing: T, incoming: T) => void;
  }
): (T & { rrfScore: number; fusedScore: number })[] {
  const getKwScore = opts?.getKeywordScore ?? ((item: T) => item.score);
  const resultMap = new Map<string, T & { rrfScore: number; fusedScore: number; keywordScore?: number }>();

  for (const item of semanticResults) {
    const key = getKey(item);
    resultMap.set(key, { ...item, semanticRank: item.semanticRank, rrfScore: 0, fusedScore: 0 });
  }

  for (const item of keywordResults) {
    const key = getKey(item);
    const existing = resultMap.get(key);
    if (existing) {
      existing.keywordRank = item.keywordRank;
      existing.keywordScore = getKwScore(item);
      existing.tsRank = item.tsRank;
      existing.bm25Score = item.bm25Score;
      opts?.onMerge?.(existing, item);
    } else {
      resultMap.set(key, { ...item, rrfScore: 0, fusedScore: 0, keywordScore: getKwScore(item) });
    }
  }

  const merged = Array.from(resultMap.values()).map((item) => {
    const fusedScore = calculateFusedScore(
      item.semanticRank !== undefined,
      item.keywordRank !== undefined,
      item.semanticScore ?? 0,
      item.bm25Score,
      item.keywordScore,
    );
    const rrfScore = calculateRRFScore([item.semanticRank, item.keywordRank]);

    return { ...item, fusedScore, rrfScore, score: fusedScore };
  });

  return merged.sort((a, b) => {
    const fusedDiff = b.fusedScore - a.fusedScore;
    if (Math.abs(fusedDiff) > FLOAT_TOLERANCE) return fusedDiff;
    return b.rrfScore - a.rrfScore;
  });
}

/**
 * Merge results using weighted score fusion for books.
 * Delegates to mergeWithRRFGeneric with book-specific highlightedSnippet merge.
 */
export function mergeWithRRF(
  semanticResults: RankedResult[],
  keywordResults: RankedResult[],
  query: string
): (RankedResult & { fusedScore: number })[] {
  return mergeWithRRFGeneric(
    semanticResults,
    keywordResults,
    (r) => `${r.bookId}-${r.pageNumber}`,
    query,
    {
      getKeywordScore: (r) => r.keywordScore,
      onMerge: (existing, incoming) => {
        existing.highlightedSnippet = incoming.highlightedSnippet;
      },
    }
  );
}

/**
 * Determine match type based on which search methods found the result
 */
export function getMatchType(
  result: RankedResult
): "semantic" | "keyword" | "both" {
  if (result.semanticRank !== undefined && result.keywordRank !== undefined) {
    return "both";
  }
  if (result.semanticRank !== undefined) {
    return "semantic";
  }
  return "keyword";
}

/**
 * Keep the higher value for a numeric field during merge
 */
function keepBest<T>(existing: T, incoming: T, field: keyof T): void {
  if (incoming[field] != null && (existing[field] == null || (incoming[field] as number) > (existing[field] as number))) {
    existing[field] = incoming[field];
  }
}

/**
 * Generic merge and deduplicate results from multiple queries using weighted RRF
 */
function mergeAndDeduplicateGeneric<T extends { semanticScore?: number }>(
  resultsPerQuery: { results: T[]; weight: number }[],
  getKey: (item: T) => string,
  mergeBest?: (existing: T, incoming: T) => void,
): T[] {
  const merged = new Map<string, T & { weightedRrfScore: number }>();

  for (const { results, weight } of resultsPerQuery) {
    for (let rank = 0; rank < results.length; rank++) {
      const result = results[rank];
      const key = getKey(result);
      const rrfContribution = weight / (RRF_K + rank + 1);

      const existing = merged.get(key);
      if (existing) {
        existing.weightedRrfScore += rrfContribution;
        keepBest(existing, result, 'semanticScore');
        mergeBest?.(existing, result);
      } else {
        merged.set(key, { ...result, weightedRrfScore: rrfContribution });
      }
    }
  }

  return Array.from(merged.values())
    .sort((a, b) => b.weightedRrfScore - a.weightedRrfScore);
}

export function mergeAndDeduplicateBooks(
  resultsPerQuery: { results: RankedResult[]; weight: number }[]
): RankedResult[] {
  return mergeAndDeduplicateGeneric(
    resultsPerQuery,
    (r) => `${r.bookId}-${r.pageNumber}`,
    (existing, incoming) => {
      if (incoming.highlightedSnippet && incoming.highlightedSnippet !== incoming.textSnippet) {
        existing.highlightedSnippet = incoming.highlightedSnippet;
      }
      keepBest(existing, incoming, 'keywordScore');
      keepBest(existing, incoming, 'tsRank');
      keepBest(existing, incoming, 'bm25Score');
    }
  );
}

export function mergeAndDeduplicateAyahs(
  resultsPerQuery: { results: AyahRankedResult[]; weight: number }[]
): AyahRankedResult[] {
  return mergeAndDeduplicateGeneric(resultsPerQuery, (r) => `${r.surahNumber}-${r.ayahNumber}`);
}

export function mergeAndDeduplicateHadiths(
  resultsPerQuery: { results: HadithRankedResult[]; weight: number }[]
): HadithRankedResult[] {
  return mergeAndDeduplicateGeneric(resultsPerQuery, (r) => `${r.collectionSlug}-${r.hadithNumber}`);
}
