/**
 * In-memory cache for embeddings with TTL
 *
 * Caches embedding vectors to avoid redundant API calls for repeated queries.
 * Especially useful in refine mode where the same query may be used multiple times
 * or when users search for similar terms.
 */

interface CacheEntry {
  embedding: number[];
  timestamp: number;
}

// Cache TTL: 30 minutes (embeddings don't change, longer TTL = higher hit rate)
const TTL_MS = 30 * 60 * 1000;

// Maximum cache size to prevent memory issues (increased for better hit rate)
const MAX_CACHE_SIZE = 5000;

// Number of entries to evict when cache is full
const EVICTION_COUNT = 100;

const cache = new Map<string, CacheEntry>();

/**
 * Get a cached embedding if available and not expired
 */
export function getCachedEmbedding(text: string): number[] | null {
  const entry = cache.get(text);
  if (!entry) return null;

  // Check if entry has expired
  if (Date.now() - entry.timestamp > TTL_MS) {
    cache.delete(text);
    return null;
  }

  return entry.embedding;
}

/**
 * Get multiple cached embeddings at once
 * Returns a map of text -> embedding for found entries
 * Missing or expired entries are not included
 */
export function getCachedEmbeddings(texts: string[]): Map<string, number[]> {
  const result = new Map<string, number[]>();
  const now = Date.now();

  for (const text of texts) {
    const entry = cache.get(text);
    if (entry) {
      if (now - entry.timestamp > TTL_MS) {
        cache.delete(text);
      } else {
        result.set(text, entry.embedding);
      }
    }
  }

  return result;
}

/**
 * Store an embedding in the cache
 */
export function setCachedEmbedding(text: string, embedding: number[]): void {
  // Evict oldest entries if cache is full
  if (cache.size >= MAX_CACHE_SIZE) {
    evictOldestEntries();
  }

  cache.set(text, { embedding, timestamp: Date.now() });
}

/**
 * Store multiple embeddings in the cache at once
 */
export function setCachedEmbeddings(
  entries: Array<{ text: string; embedding: number[] }>
): void {
  // Evict if we would exceed max size
  if (cache.size + entries.length >= MAX_CACHE_SIZE) {
    evictOldestEntries();
  }

  const now = Date.now();
  for (const { text, embedding } of entries) {
    cache.set(text, { embedding, timestamp: now });
  }
}

/**
 * Evict the oldest entries from the cache
 */
function evictOldestEntries(): void {
  const entries = [...cache.entries()];
  entries.sort((a, b) => a[1].timestamp - b[1].timestamp);

  const toEvict = entries.slice(0, EVICTION_COUNT);
  for (const [key] of toEvict) {
    cache.delete(key);
  }

  console.log(
    `[EmbeddingCache] Evicted ${toEvict.length} entries, cache size: ${cache.size}`
  );
}

/**
 * Get cache statistics for debugging
 */
export function getCacheStats(): { size: number; maxSize: number; ttlMs: number } {
  return {
    size: cache.size,
    maxSize: MAX_CACHE_SIZE,
    ttlMs: TTL_MS,
  };
}

/**
 * Clear the entire cache (useful for testing)
 */
export function clearCache(): void {
  cache.clear();
}
