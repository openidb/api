/**
 * BM25 Score Normalization Utility
 *
 * Provides score normalization for combining BM25 scores (from Elasticsearch)
 * with other scoring mechanisms (e.g., semantic similarity) in hybrid search.
 *
 * Note: BM25 scoring is handled natively by Elasticsearch. This module only
 * provides utilities for normalizing and combining scores in the RRF fusion.
 */

// BM25 parameters (for reference - these are Elasticsearch defaults)
export const BM25_K1 = 1.2; // Term frequency saturation parameter (ES default)
export const BM25_B = 0.75; // Length normalization parameter (ES default)

/**
 * Normalize BM25 score to 0-1 range using sigmoid-like function
 * Maps: 0→0, k→0.5, ∞→1
 *
 * This allows combining BM25 scores with other normalized scores (e.g., semantic similarity)
 *
 * @param score - Raw BM25 score from Elasticsearch (unbounded, typically 0-20+)
 * @param k - Normalization parameter (score at which output = 0.5). Default: 5
 * @returns Normalized score in [0, 1] range
 */
export function normalizeBM25Score(score: number, k: number = 5): number {
  if (score <= 0) return 0;
  return score / (score + k);
}
