/**
 * Gemini Embeddings Utility
 *
 * Generates embeddings using Google Gemini embedding-001 via OpenRouter.
 * Optimized for Arabic/multilingual text with 3072 dimensions.
 */

import OpenAI from "openai";
import { GEMINI_DIMENSIONS } from "../constants";
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

// Re-export dimensions
export { GEMINI_DIMENSIONS };

// Use OpenRouter to access Gemini embedding models
const openai = new OpenAI({
  apiKey: process.env.OPENROUTER_API_KEY,
  baseURL: "https://openrouter.ai/api/v1",
});

// Model configuration
const EMBEDDING_MODEL = "google/gemini-embedding-001";

/**
 * Generate embedding for a single text string
 * Uses two-tier cache: in-memory (fast) + SQLite persistent (survives restarts)
 */
export async function generateEmbedding(text: string): Promise<number[]> {
  // Check in-memory cache first (fastest)
  const memCached = getCachedEmbedding(text);
  if (memCached) {
    return memCached;
  }

  // Check persistent SQLite cache (survives restarts)
  const persistentCached = getPersistentCachedEmbedding(text);
  if (persistentCached) {
    // Promote to in-memory cache for faster subsequent access
    setCachedEmbedding(text, persistentCached);
    return persistentCached;
  }

  // Generate embedding via API
  const response = await openai.embeddings.create({
    model: EMBEDDING_MODEL,
    input: text,
  });
  const embedding = response.data[0].embedding;

  // Cache in both tiers
  setCachedEmbedding(text, embedding);
  setPersistentCachedEmbedding(text, embedding);

  return embedding;
}

/**
 * Generate embeddings for multiple text strings in a single API call
 * More efficient than calling generateEmbedding multiple times
 * Uses two-tier caching: in-memory + persistent SQLite
 */
export async function generateEmbeddings(texts: string[]): Promise<number[][]> {
  if (texts.length === 0) return [];

  // Check in-memory cache first
  const memCachedMap = getCachedEmbeddings(texts);

  // For texts not in memory, check persistent cache
  const notInMemory = texts.filter((t) => !memCachedMap.has(t));
  const persistentCachedMap = notInMemory.length > 0
    ? getPersistentCachedEmbeddings(notInMemory)
    : new Map<string, number[]>();

  // Promote persistent cache hits to memory cache
  if (persistentCachedMap.size > 0) {
    const toPromote = Array.from(persistentCachedMap.entries()).map(([text, embedding]) => ({
      text,
      embedding,
    }));
    setCachedEmbeddings(toPromote);
  }

  // Merge both cache maps
  const cachedMap = new Map(memCachedMap);
  for (const [text, embedding] of persistentCachedMap) {
    cachedMap.set(text, embedding);
  }

  // Find texts that need API call
  const uncachedTexts: string[] = [];
  const uncachedIndices: number[] = [];

  for (let i = 0; i < texts.length; i++) {
    if (!cachedMap.has(texts[i])) {
      uncachedTexts.push(texts[i]);
      uncachedIndices.push(i);
    }
  }

  // If all texts are cached, return immediately
  if (uncachedTexts.length === 0) {
    return texts.map((text) => cachedMap.get(text)!);
  }

  // Generate embeddings for uncached texts
  const response = await openai.embeddings.create({
    model: EMBEDDING_MODEL,
    input: uncachedTexts,
  });

  const newEmbeddings = response.data.map((d) => d.embedding);

  // Cache in both tiers
  const entriesToCache = uncachedTexts.map((text, i) => ({
    text,
    embedding: newEmbeddings[i],
  }));
  setCachedEmbeddings(entriesToCache);
  setPersistentCachedEmbeddings(entriesToCache);

  // Build result array preserving original order
  const result: number[][] = new Array(texts.length);
  for (let i = 0; i < texts.length; i++) {
    const cached = cachedMap.get(texts[i]);
    if (cached) {
      result[i] = cached;
    }
  }
  for (let i = 0; i < uncachedIndices.length; i++) {
    result[uncachedIndices[i]] = newEmbeddings[i];
  }

  return result;
}

/**
 * Normalize Arabic text for better embedding quality
 * - Removes diacritics (tashkeel) for consistent matching
 * - Normalizes whitespace
 * - Removes excessive punctuation
 */
export function normalizeArabicText(text: string): string {
  return (
    text
      // Remove Arabic diacritics (tashkeel)
      .replace(/[\u064B-\u065F\u0670]/g, "")
      // Normalize alef variants to plain alef
      .replace(/[\u0622\u0623\u0625\u0671]/g, "\u0627")
      // Remove standalone hamza
      .replace(/\u0621/g, "")
      // Normalize alef maksura to yeh
      .replace(/\u0649/g, "\u064A")
      // Normalize teh marbuta to heh
      .replace(/\u0629/g, "\u0647")
      // Normalize whitespace
      .replace(/\s+/g, " ")
      .trim()
  );
}

/**
 * Truncate text to fit within token limits
 * text-embedding-3-large has 8191 token limit
 * Rough estimate: 1 token ~ 4 characters for Arabic
 */
export function truncateForEmbedding(
  text: string,
  maxChars: number = 6000
): string {
  if (text.length <= maxChars) return text;

  // Try to cut at a sentence boundary
  const truncated = text.slice(0, maxChars);
  const lastPeriod = truncated.lastIndexOf(".");
  const lastArabicPeriod = truncated.lastIndexOf("\u06D4");

  const cutPoint = Math.max(lastPeriod, lastArabicPeriod);

  if (cutPoint > maxChars * 0.7) {
    return truncated.slice(0, cutPoint + 1);
  }

  return truncated;
}
