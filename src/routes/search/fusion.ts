import { normalizeBM25Score } from "../../search/bm25";
import { RRF_K, SEMANTIC_WEIGHT, KEYWORD_WEIGHT } from "./config";
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
 * Calculate fused score from semantic and keyword signals
 */
function calculateFusedScore(
  hasSemantic: boolean,
  hasKeyword: boolean,
  semanticScore: number,
  bm25Score: number | undefined,
  keywordScore: number | undefined
): number {
  if (hasSemantic && hasKeyword) {
    return SEMANTIC_WEIGHT * semanticScore + KEYWORD_WEIGHT * normalizeBM25Score(bm25Score ?? 0);
  }
  if (hasSemantic) return semanticScore;
  return normalizeBM25Score(bm25Score ?? keywordScore ?? 0);
}

/**
 * Generic RRF merge function for any content type with weighted score fusion
 */
export function mergeWithRRFGeneric<T extends { semanticRank?: number; keywordRank?: number; semanticScore?: number; score?: number; tsRank?: number; bm25Score?: number }>(
  semanticResults: T[],
  keywordResults: T[],
  getKey: (item: T) => string,
  query: string
): (T & { rrfScore: number; fusedScore: number })[] {
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
      existing.keywordScore = item.score;
      existing.tsRank = item.tsRank;
      existing.bm25Score = item.bm25Score;
    } else {
      resultMap.set(key, { ...item, rrfScore: 0, fusedScore: 0, keywordScore: item.score });
    }
  }

  const merged = Array.from(resultMap.values()).map((item) => {
    const fusedScore = calculateFusedScore(
      item.semanticRank !== undefined,
      item.keywordRank !== undefined,
      item.semanticScore ?? 0,
      item.bm25Score,
      item.keywordScore
    );
    const rrfScore = calculateRRFScore([item.semanticRank, item.keywordRank]);

    return { ...item, fusedScore, rrfScore, score: fusedScore };
  });

  return merged.sort((a, b) => {
    const fusedDiff = b.fusedScore - a.fusedScore;
    if (Math.abs(fusedDiff) > 0.001) return fusedDiff;
    return b.rrfScore - a.rrfScore;
  });
}

/**
 * Merge results using weighted score fusion for books
 */
export function mergeWithRRF(
  semanticResults: RankedResult[],
  keywordResults: RankedResult[],
  query: string
): (RankedResult & { fusedScore: number })[] {
  const resultMap = new Map<string, RankedResult & { fusedScore: number }>();

  for (const result of semanticResults) {
    const key = `${result.bookId}-${result.pageNumber}`;
    resultMap.set(key, { ...result, fusedScore: 0 });
  }

  for (const result of keywordResults) {
    const key = `${result.bookId}-${result.pageNumber}`;
    const existing = resultMap.get(key);

    if (existing) {
      existing.keywordRank = result.keywordRank;
      existing.keywordScore = result.keywordScore;
      existing.highlightedSnippet = result.highlightedSnippet;
      existing.tsRank = result.tsRank;
      existing.bm25Score = result.bm25Score;
    } else {
      resultMap.set(key, { ...result, fusedScore: 0 });
    }
  }

  const merged = Array.from(resultMap.values()).map((result) => {
    const fusedScore = calculateFusedScore(
      result.semanticRank !== undefined,
      result.keywordRank !== undefined,
      result.semanticScore ?? 0,
      result.bm25Score,
      result.keywordScore
    );
    const rrfScore = calculateRRFScore([result.semanticRank, result.keywordRank]);

    return { ...result, fusedScore, rrfScore };
  });

  merged.sort((a, b) => {
    const fusedDiff = b.fusedScore - a.fusedScore;
    if (Math.abs(fusedDiff) > 0.001) return fusedDiff;
    return b.rrfScore - a.rrfScore;
  });

  return merged;
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
