import { TTLCache } from "./lib/ttl-cache";
import type { ExpandedQuery } from "./routes/search/types";

const cache = new TTLCache<ExpandedQuery[]>({
  maxSize: 200,
  ttlMs: 10 * 60 * 1000,
  evictionCount: 50,
  label: "QueryExpansion",
});

export function getCachedExpansion(query: string): ExpandedQuery[] | null {
  return cache.get(query);
}

export function setCachedExpansion(query: string, expansions: ExpandedQuery[]): void {
  cache.set(query, expansions);
}
