/**
 * Unified Embeddings Router
 *
 * Routes embedding requests to either Gemini (cloud) or BGE-M3 (local) based on
 * user configuration. Provides a consistent API for the rest of the application.
 *
 * Models:
 * - gemini: Google Gemini embedding-001 via OpenRouter (3072 dimensions)
 * - bge-m3: BAAI/bge-m3 via local server (1024 dimensions)
 */

import {
  generateEmbedding as generateEmbeddingGemini,
  generateEmbeddings as generateEmbeddingsGemini,
  normalizeArabicText,
  truncateForEmbedding,
  GEMINI_DIMENSIONS,
} from "./gemini";

import {
  generateEmbeddingBGE,
  generateEmbeddingsBGE,
  isBGEServerAvailable,
  BGE_DIMENSIONS,
} from "./bge";

import { EMBEDDING_DIMENSIONS } from "../constants";

// Re-export utilities and constants
export {
  normalizeArabicText,
  truncateForEmbedding,
  EMBEDDING_DIMENSIONS,
  GEMINI_DIMENSIONS,
  BGE_DIMENSIONS,
  isBGEServerAvailable,
};

// Embedding model type
export type EmbeddingModel = "gemini" | "bge-m3";

/**
 * Generate embedding for a single text string
 * Routes to the appropriate model based on the model parameter
 *
 * @param text - Text to embed
 * @param model - Which embedding model to use (default: 'gemini')
 * @returns Promise<number[]> - Embedding vector
 */
export async function generateEmbedding(
  text: string,
  model: EmbeddingModel = "gemini"
): Promise<number[]> {
  if (model === "bge-m3") {
    return generateEmbeddingBGE(text, "query");
  }
  return generateEmbeddingGemini(text);
}

/**
 * Generate embeddings for multiple text strings
 * Routes to the appropriate model based on the model parameter
 *
 * @param texts - Array of texts to embed
 * @param model - Which embedding model to use (default: 'gemini')
 * @returns Promise<number[][]> - Array of embedding vectors
 */
export async function generateEmbeddings(
  texts: string[],
  model: EmbeddingModel = "gemini"
): Promise<number[][]> {
  if (model === "bge-m3") {
    return generateEmbeddingsBGE(texts, "passage");
  }
  return generateEmbeddingsGemini(texts);
}

/**
 * Get the model display name
 */
export function getEmbeddingModelName(model: EmbeddingModel): string {
  return model === "bge-m3"
    ? "BAAI/bge-m3 (fine-tuned)"
    : "Google Gemini embedding-001";
}
