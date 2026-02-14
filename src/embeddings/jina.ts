/**
 * Jina Embeddings v3 Client
 *
 * Generates embeddings using Jina jina-embeddings-v3 (1024 dimensions).
 * Supports task-specific LoRA adapters for retrieval.query / retrieval.passage.
 * Uses two-tier cache with "jina:" prefix to avoid collision with Gemini cache.
 */

import { JINA_EMBEDDING_DIMENSIONS } from "../constants";
import {
  getCachedEmbedding,
  setCachedEmbedding,
  getCachedEmbeddings,
  setCachedEmbeddings,
} from "./cache";
import {
  getPersistentCachedEmbedding,
  setPersistentCachedEmbedding,
  getPersistentCachedEmbeddings,
  setPersistentCachedEmbeddings,
} from "./cache-persistent";

export { JINA_EMBEDDING_DIMENSIONS };

const JINA_API_URL = "https://api.jina.ai/v1/embeddings";
const JINA_MODEL = "jina-embeddings-v3";
const CACHE_PREFIX = "jina:";

export type JinaTask = "retrieval.query" | "retrieval.passage";

interface JinaEmbeddingResponse {
  data: Array<{ embedding: number[]; index: number }>;
  usage: { total_tokens: number };
}

function getApiKey(): string {
  const key = process.env.JINA_API_KEY;
  if (!key) throw new Error("JINA_API_KEY is not set");
  return key;
}

function cacheKey(text: string): string {
  return CACHE_PREFIX + text;
}

async function callJinaAPI(
  input: string[],
  task: JinaTask,
  timeoutMs = 15000,
  maxRetries = 8,
): Promise<number[][]> {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(JINA_API_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${getApiKey()}`,
        },
        body: JSON.stringify({
          model: JINA_MODEL,
          input,
          task,
          dimensions: JINA_EMBEDDING_DIMENSIONS,
        }),
        signal: controller.signal,
      });

      if (response.status === 429 && attempt < maxRetries) {
        clearTimeout(timeoutId);
        const waitMs = Math.min(3000 * Math.pow(2, attempt), 60000);
        console.warn(`[Jina] Rate limited (429), retrying in ${waitMs}ms (attempt ${attempt + 1}/${maxRetries})`);
        await new Promise((r) => setTimeout(r, waitMs));
        continue;
      }

      if (!response.ok) {
        const errorBody = await response.text().catch(() => "");
        throw new Error(`Jina API error ${response.status}: ${errorBody}`);
      }

      const data = (await response.json()) as JinaEmbeddingResponse;
      // Sort by index to ensure correct ordering
      data.data.sort((a, b) => a.index - b.index);
      return data.data.map((d) => d.embedding);
    } finally {
      clearTimeout(timeoutId);
    }
  }
  throw new Error("Jina API: max retries exceeded");
}

/**
 * Generate embedding for a single text string using Jina v3
 * Uses two-tier cache: in-memory (fast) + SQLite persistent (survives restarts)
 */
export async function generateJinaEmbedding(
  text: string,
  task: JinaTask = "retrieval.query",
): Promise<number[]> {
  const key = cacheKey(text);

  // Check in-memory cache first
  const memCached = getCachedEmbedding(key);
  if (memCached) return memCached;

  // Check persistent SQLite cache
  const persistentCached = getPersistentCachedEmbedding(key);
  if (persistentCached) {
    setCachedEmbedding(key, persistentCached);
    return persistentCached;
  }

  // Generate via API
  const [embedding] = await callJinaAPI([text], task);

  // Cache in both tiers
  setCachedEmbedding(key, embedding);
  setPersistentCachedEmbedding(key, embedding);

  return embedding;
}

/**
 * Generate embeddings for multiple text strings in a single API call using Jina v3
 * More efficient than calling generateJinaEmbedding multiple times
 */
export async function generateJinaEmbeddings(
  texts: string[],
  task: JinaTask = "retrieval.query",
): Promise<number[][]> {
  if (texts.length === 0) return [];

  const keys = texts.map(cacheKey);

  // Check in-memory cache
  const memCachedMap = getCachedEmbeddings(keys);

  // For keys not in memory, check persistent cache
  const notInMemory = keys.filter((k) => !memCachedMap.has(k));
  const persistentCachedMap =
    notInMemory.length > 0
      ? getPersistentCachedEmbeddings(notInMemory)
      : new Map<string, number[]>();

  // Promote persistent hits to memory
  if (persistentCachedMap.size > 0) {
    const toPromote = Array.from(persistentCachedMap.entries()).map(
      ([text, embedding]) => ({ text, embedding }),
    );
    setCachedEmbeddings(toPromote);
  }

  // Merge caches
  const cachedMap = new Map(memCachedMap);
  for (const [k, v] of persistentCachedMap) {
    cachedMap.set(k, v);
  }

  // Find uncached texts
  const uncachedTexts: string[] = [];
  const uncachedIndices: number[] = [];
  for (let i = 0; i < texts.length; i++) {
    if (!cachedMap.has(keys[i])) {
      uncachedTexts.push(texts[i]);
      uncachedIndices.push(i);
    }
  }

  // All cached — return immediately
  if (uncachedTexts.length === 0) {
    return keys.map((k) => cachedMap.get(k)!);
  }

  // Generate embeddings for uncached texts (Jina supports up to 2048 inputs per call)
  const newEmbeddings = await callJinaAPI(uncachedTexts, task);

  // Cache in both tiers
  const entriesToCache = uncachedTexts.map((text, i) => ({
    text: cacheKey(text),
    embedding: newEmbeddings[i],
  }));
  setCachedEmbeddings(entriesToCache);
  setPersistentCachedEmbeddings(
    entriesToCache.map(({ text, embedding }) => ({ text, embedding })),
  );

  // Build result preserving original order
  const result: number[][] = new Array(texts.length);
  for (let i = 0; i < texts.length; i++) {
    const cached = cachedMap.get(keys[i]);
    if (cached) {
      result[i] = cached;
    }
  }
  for (let i = 0; i < uncachedIndices.length; i++) {
    result[uncachedIndices[i]] = newEmbeddings[i];
  }

  return result;
}
