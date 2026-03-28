/**
 * Embeddings Module
 *
 * Generates embeddings using Google Gemini embedding-001 via OpenRouter (3072 dimensions),
 * Jina embeddings-v3 (1024 dimensions), and Nemotron (2048 dimensions).
 */

import {
  generateEmbedding as generateEmbeddingGemini,
  generateEmbeddings as generateEmbeddingsGemini,
  normalizeArabicText,
  truncateForEmbedding,
} from "./gemini";

import {
  generateJinaEmbedding,
  generateJinaEmbeddings,
} from "./jina";

import {
  generateNemotronEmbedding,
  generateNemotronEmbeddings,
} from "./nemotron";

import { EMBEDDING_DIMENSIONS, JINA_EMBEDDING_DIMENSIONS, NEMOTRON_EMBEDDING_DIMENSIONS } from "../constants";

// Re-export utilities and constants
export {
  normalizeArabicText,
  truncateForEmbedding,
  EMBEDDING_DIMENSIONS,
  JINA_EMBEDDING_DIMENSIONS,
  NEMOTRON_EMBEDDING_DIMENSIONS,
};

// Re-export Jina functions
export { generateJinaEmbedding, generateJinaEmbeddings };

// Re-export Nemotron functions
export { generateNemotronEmbedding, generateNemotronEmbeddings };

/**
 * Generate embedding for a single text string (Gemini, default)
 */
export async function generateEmbedding(text: string): Promise<number[]> {
  return generateEmbeddingGemini(text);
}

/**
 * Generate embeddings for multiple text strings (Gemini, default)
 */
export async function generateEmbeddings(texts: string[]): Promise<number[][]> {
  return generateEmbeddingsGemini(texts);
}
