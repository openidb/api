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

// Search limits
export const DEFAULT_SEARCH_LIMIT = 20;
export const MAX_SEARCH_LIMIT = 100;
export const MAX_QUERY_LENGTH = 500;
export const DEFAULT_SIMILARITY_CUTOFF = 0.6;
export const REFINE_SIMILARITY_CUTOFF = 0.25;
export const DEFAULT_BOOK_LIMIT = 10;
export const MAX_BOOK_LIMIT = 50;
export const MIN_BOOK_LIMIT = 5;
export const DEFAULT_AYAH_LIMIT = 30;
export const DEFAULT_HADITH_LIMIT = 30;
export const STANDARD_FETCH_LIMIT = 50;

// Tolerance for comparing floating-point fused scores
export const FLOAT_TOLERANCE = 0.001;
