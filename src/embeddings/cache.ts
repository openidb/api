import { TTLCache } from "../lib/ttl-cache";

const cache = new TTLCache<number[]>({
  maxSize: 20_000,
  ttlMs: 24 * 60 * 60 * 1000, // 24 hours — embeddings are deterministic
  evictionCount: 500,
  label: "Embedding",
});

export function getCachedEmbedding(text: string): number[] | null {
  return cache.get(text);
}

export function getCachedEmbeddings(texts: string[]): Map<string, number[]> {
  return cache.getMany(texts);
}

export function setCachedEmbedding(text: string, embedding: number[]): void {
  cache.set(text, embedding);
}

export function setCachedEmbeddings(entries: Array<{ text: string; embedding: number[] }>): void {
  cache.setMany(entries.map(({ text, embedding }) => ({ key: text, value: embedding })));
}

