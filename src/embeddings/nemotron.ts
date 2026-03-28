/**
 * Nemotron Embedding Client
 *
 * Generates embeddings using nvidia/llama-nemotron-embed-vl-1b-v2:free via OpenRouter (2048 dimensions).
 * Uses two-tier cache with "nemotron:" prefix to avoid collision with Gemini/Jina caches.
 * Includes rate limiting for free tier (20 req/min).
 */

import { NEMOTRON_EMBEDDING_DIMENSIONS } from "../constants";
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

export { NEMOTRON_EMBEDDING_DIMENSIONS };

const OPENROUTER_URL = "https://openrouter.ai/api/v1/embeddings";
const NEMOTRON_MODEL = "nvidia/llama-nemotron-embed-vl-1b-v2:free";
const CACHE_PREFIX = "nemotron:";

// Rate limiter: 20 requests per minute for free tier
const RATE_LIMIT_WINDOW_MS = 60_000;
const RATE_LIMIT_MAX = 20;
const requestTimestamps: number[] = [];

async function waitForRateLimit(): Promise<void> {
  const now = Date.now();
  // Remove timestamps older than the window
  while (requestTimestamps.length > 0 && requestTimestamps[0] < now - RATE_LIMIT_WINDOW_MS) {
    requestTimestamps.shift();
  }
  if (requestTimestamps.length >= RATE_LIMIT_MAX) {
    const oldestInWindow = requestTimestamps[0];
    const waitMs = oldestInWindow + RATE_LIMIT_WINDOW_MS - now + 100; // 100ms buffer
    if (waitMs > 0) {
      console.log(`[Nemotron] Rate limit reached, waiting ${(waitMs / 1000).toFixed(1)}s...`);
      await new Promise((r) => setTimeout(r, waitMs));
    }
  }
  requestTimestamps.push(Date.now());
}

interface EmbeddingResponse {
  data: Array<{ embedding: number[]; index: number }>;
  usage: { prompt_tokens: number; total_tokens: number };
}

function getApiKey(): string {
  const key = process.env.OPENROUTER_API_KEY;
  if (!key) throw new Error("OPENROUTER_API_KEY is not set");
  return key;
}

function cacheKey(text: string): string {
  return CACHE_PREFIX + text;
}

async function callNemotronAPI(
  input: string | string[],
  timeoutMs = 30000,
  maxRetries = 8,
): Promise<number[][]> {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    await waitForRateLimit();

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(OPENROUTER_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${getApiKey()}`,
        },
        body: JSON.stringify({
          model: NEMOTRON_MODEL,
          input,
          encoding_format: "float",
        }),
        signal: controller.signal,
      });

      if (response.status === 429 && attempt < maxRetries) {
        clearTimeout(timeoutId);
        const waitMs = Math.min(5000 * Math.pow(2, attempt), 120000);
        console.warn(`[Nemotron] Rate limited (429), retrying in ${(waitMs / 1000).toFixed(0)}s (attempt ${attempt + 1}/${maxRetries})`);
        await new Promise((r) => setTimeout(r, waitMs));
        continue;
      }

      if (!response.ok) {
        const errorBody = await response.text().catch(() => "");
        throw new Error(`Nemotron API error ${response.status}: ${errorBody}`);
      }

      const data = (await response.json()) as EmbeddingResponse;
      // Sort by index to ensure correct ordering
      data.data.sort((a, b) => a.index - b.index);
      return data.data.map((d) => d.embedding);
    } finally {
      clearTimeout(timeoutId);
    }
  }
  throw new Error("Nemotron API: max retries exceeded");
}

/**
 * Generate embedding for a single text string using Nemotron
 * Uses two-tier cache: in-memory (fast) + SQLite persistent (survives restarts)
 */
export async function generateNemotronEmbedding(text: string): Promise<number[]> {
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
  const [embedding] = await callNemotronAPI(text);

  // Cache in both tiers
  setCachedEmbedding(key, embedding);
  setPersistentCachedEmbedding(key, embedding);

  return embedding;
}

/**
 * Generate embeddings for multiple text strings in a single API call using Nemotron
 * More efficient than calling generateNemotronEmbedding multiple times
 */
export async function generateNemotronEmbeddings(texts: string[]): Promise<number[][]> {
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

  // Generate embeddings for uncached texts
  const newEmbeddings = await callNemotronAPI(uncachedTexts);

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
