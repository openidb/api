/**
 * Evaluation metrics for retrieval benchmark.
 * Ported from training/scripts/evaluate-model.ts with additions: NDCG, Precision@K.
 */

import type {
  EvaluationResult,
  AggregateMetrics,
  CategoryMetrics,
} from "../types";

/**
 * Recall@K: fraction of relevant docs found in top K results
 */
export function recallAtK(
  relevant: string[],
  retrieved: string[],
  k: number
): number {
  const retrievedAtK = new Set(retrieved.slice(0, k));
  const relevantSet = new Set(relevant);
  let found = 0;
  for (const rel of relevantSet) {
    if (retrievedAtK.has(rel)) found++;
  }
  return relevantSet.size > 0 ? found / relevantSet.size : 0;
}

/**
 * Precision@K: fraction of top K results that are relevant
 */
export function precisionAtK(
  relevant: string[],
  retrieved: string[],
  k: number
): number {
  const retrievedAtK = retrieved.slice(0, k);
  const relevantSet = new Set(relevant);
  let found = 0;
  for (const doc of retrievedAtK) {
    if (relevantSet.has(doc)) found++;
  }
  return retrievedAtK.length > 0 ? found / retrievedAtK.length : 0;
}

/**
 * Reciprocal Rank: 1 / rank of first relevant result (for MRR)
 */
export function reciprocalRank(
  relevant: string[],
  retrieved: string[]
): number {
  const relevantSet = new Set(relevant);
  for (let i = 0; i < retrieved.length; i++) {
    if (relevantSet.has(retrieved[i])) {
      return 1 / (i + 1);
    }
  }
  return 0;
}

/**
 * NDCG@K: Normalized Discounted Cumulative Gain
 * Uses binary relevance (1 if relevant, 0 otherwise)
 */
export function ndcgAtK(
  relevant: string[],
  retrieved: string[],
  k: number
): number {
  const relevantSet = new Set(relevant);
  const retrievedAtK = retrieved.slice(0, k);

  // DCG: sum of 1/log2(rank+1) for relevant docs
  let dcg = 0;
  for (let i = 0; i < retrievedAtK.length; i++) {
    if (relevantSet.has(retrievedAtK[i])) {
      dcg += 1 / Math.log2(i + 2); // +2 because rank is 1-indexed
    }
  }

  // Ideal DCG: all relevant docs at top
  const idealK = Math.min(relevantSet.size, k);
  let idcg = 0;
  for (let i = 0; i < idealK; i++) {
    idcg += 1 / Math.log2(i + 2);
  }

  return idcg > 0 ? dcg / idcg : 0;
}

/**
 * Find rank of first relevant result (1-indexed)
 */
export function firstRelevantRank(
  relevant: string[],
  retrieved: string[]
): number | null {
  const relevantSet = new Set(relevant);
  for (let i = 0; i < retrieved.length; i++) {
    if (relevantSet.has(retrieved[i])) return i + 1;
  }
  return null;
}

/**
 * Compute all metrics for a single query
 */
export function computeQueryMetrics(
  relevant: string[],
  retrieved: string[],
  scores: number[]
): Pick<
  EvaluationResult,
  | "recall_at_1"
  | "recall_at_5"
  | "recall_at_10"
  | "recall_at_20"
  | "reciprocal_rank"
  | "ndcg_at_10"
  | "precision_at_5"
  | "precision_at_10"
  | "first_relevant_rank"
  | "hit"
> {
  return {
    recall_at_1: recallAtK(relevant, retrieved, 1),
    recall_at_5: recallAtK(relevant, retrieved, 5),
    recall_at_10: recallAtK(relevant, retrieved, 10),
    recall_at_20: recallAtK(relevant, retrieved, 20),
    reciprocal_rank: reciprocalRank(relevant, retrieved),
    ndcg_at_10: ndcgAtK(relevant, retrieved, 10),
    precision_at_5: precisionAtK(relevant, retrieved, 5),
    precision_at_10: precisionAtK(relevant, retrieved, 10),
    first_relevant_rank: firstRelevantRank(relevant, retrieved),
    hit: firstRelevantRank(relevant, retrieved) !== null,
  };
}

/**
 * Calculate aggregate metrics from individual evaluation results
 */
export function calculateAggregateMetrics(
  results: EvaluationResult[]
): AggregateMetrics {
  if (results.length === 0) {
    return {
      total_queries: 0,
      recall_at_1: 0,
      recall_at_5: 0,
      recall_at_10: 0,
      recall_at_20: 0,
      mrr: 0,
      ndcg_at_10: 0,
      precision_at_5: 0,
      precision_at_10: 0,
      hit_rate: 0,
      avg_first_relevant_rank: 0,
      by_category: {},
      by_language: {},
      by_difficulty: {},
    };
  }

  const byCategory: Record<string, EvaluationResult[]> = {};
  const byLanguage: Record<string, EvaluationResult[]> = {};
  const byDifficulty: Record<string, EvaluationResult[]> = {};

  for (const r of results) {
    (byCategory[r.category] ??= []).push(r);
    (byLanguage[r.language] ??= []).push(r);
    (byDifficulty[r.difficulty] ??= []).push(r);
  }

  const avg = (arr: EvaluationResult[], key: keyof EvaluationResult) =>
    arr.reduce((sum, r) => sum + (r[key] as number), 0) / arr.length;

  const calcGroupMetrics = (group: EvaluationResult[]): CategoryMetrics => ({
    count: group.length,
    recall_at_1: avg(group, "recall_at_1"),
    recall_at_5: avg(group, "recall_at_5"),
    recall_at_10: avg(group, "recall_at_10"),
    recall_at_20: avg(group, "recall_at_20"),
    mrr: avg(group, "reciprocal_rank"),
    ndcg_at_10: avg(group, "ndcg_at_10"),
    precision_at_5: avg(group, "precision_at_5"),
    precision_at_10: avg(group, "precision_at_10"),
    hit_rate: group.filter((r) => r.hit).length / group.length,
  });

  const firstRanks = results
    .map((r) => r.first_relevant_rank)
    .filter((r): r is number => r !== null);

  return {
    total_queries: results.length,
    recall_at_1: avg(results, "recall_at_1"),
    recall_at_5: avg(results, "recall_at_5"),
    recall_at_10: avg(results, "recall_at_10"),
    recall_at_20: avg(results, "recall_at_20"),
    mrr: avg(results, "reciprocal_rank"),
    ndcg_at_10: avg(results, "ndcg_at_10"),
    precision_at_5: avg(results, "precision_at_5"),
    precision_at_10: avg(results, "precision_at_10"),
    hit_rate: results.filter((r) => r.hit).length / results.length,
    avg_first_relevant_rank:
      firstRanks.length > 0
        ? firstRanks.reduce((a, b) => a + b, 0) / firstRanks.length
        : 0,
    by_category: Object.fromEntries(
      Object.entries(byCategory).map(([k, v]) => [k, calcGroupMetrics(v)])
    ),
    by_language: Object.fromEntries(
      Object.entries(byLanguage).map(([k, v]) => [k, calcGroupMetrics(v)])
    ),
    by_difficulty: Object.fromEntries(
      Object.entries(byDifficulty).map(([k, v]) => [k, calcGroupMetrics(v)])
    ),
  };
}
