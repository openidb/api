/**
 * In-memory cache for query expansions with TTL
 *
 * Caches LLM-generated query expansions to avoid redundant API calls.
 * Uses conservative limits to prevent memory bloat from unique queries.
 */

interface ExpandedQuery {
  query: string;
  weight: number;
  reason: string;
}

interface CacheEntry {
  expansions: ExpandedQuery[];
  timestamp: number;
}

// Cache TTL: 10 minutes (shorter than embedding cache since queries are more unique)
const TTL_MS = 10 * 60 * 1000;

// Small maximum cache size to limit memory (unique queries are common)
const MAX_CACHE_SIZE = 200;

// Aggressive eviction to keep memory bounded
const EVICTION_COUNT = 50;

const cache = new Map<string, CacheEntry>();

/**
 * Get cached query expansions if available and not expired
 */
export function getCachedExpansion(query: string): ExpandedQuery[] | null {
  const entry = cache.get(query);
  if (!entry) return null;

  // Check if entry has expired
  if (Date.now() - entry.timestamp > TTL_MS) {
    cache.delete(query);
    return null;
  }

  return entry.expansions;
}

/**
 * Store query expansions in the cache
 */
export function setCachedExpansion(query: string, expansions: ExpandedQuery[]): void {
  // Evict oldest entries if cache is full
  if (cache.size >= MAX_CACHE_SIZE) {
    evictOldestEntries();
  }

  cache.set(query, { expansions, timestamp: Date.now() });
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
    `[QueryExpansionCache] Evicted ${toEvict.length} entries, cache size: ${cache.size}`
  );
}
