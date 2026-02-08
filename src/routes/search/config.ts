// RRF constant (standard value is 60)
export const RRF_K = 60;

// Weighted fusion: combine semantic and keyword scores
// Max fused score = 0.8 + 0.3 = 1.1 (rewards results found by both methods)
export const SEMANTIC_WEIGHT = 0.8;
export const KEYWORD_WEIGHT = 0.3;

// Minimum character count for semantic search (queries below this skip semantic)
// Short queries (≤3 chars) lack meaningful semantic content and produce noisy results
export const MIN_CHARS_FOR_SEMANTIC = 4;

// Database stats cache TTL (5 minutes)
export const DB_STATS_CACHE_TTL = 5 * 60 * 1000;

// Books to exclude from search results
// These books contain sources that negatively impact search quality
export const EXCLUDED_BOOK_IDS = new Set([
  "2", // كتاب النوازل في الرضاع - excluded due to sources that negatively impacted search relevance
]);
