// RRF constant (standard value is 60)
export const RRF_K = 60;

// Weighted fusion: combine semantic and keyword scores
// Only Arabic queries use keyword search (non-Arabic is semantic-only)
export const SEMANTIC_WEIGHT = 0.4;
export const KEYWORD_WEIGHT = 0.7;

// BM25 normalization k: score/(score+k). Lower k preserves gaps between high BM25 scores.
export const BM25_NORM_K = 3;

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

// Author search score threshold for Qdrant results
export const AUTHOR_SCORE_THRESHOLD = 0.3;

// Text truncation limits for reranking prompts
export const RERANKER_TEXT_LIMIT = 800;
export const UNIFIED_RERANKER_TEXT_LIMIT = 600;

// Unified rerank timeout
export const UNIFIED_RERANK_TIMEOUT_MS = 25000;

// Hybrid search pre-rerank caps
export const AYAH_PRE_RERANK_CAP = 60;
export const HADITH_PRE_RERANK_CAP = 75;

// Fetch limit cap for hybrid search
export const FETCH_LIMIT_CAP = 100;

// Default ayah similarity cutoff for semantic search
export const DEFAULT_AYAH_SIMILARITY_CUTOFF = 0.28;

// Hadith collections excluded from default search results
export const EXCLUDED_HADITH_COLLECTIONS = new Set<string>();
