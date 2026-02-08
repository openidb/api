/**
 * BGE-M3 Embeddings Client
 *
 * Connects to a local FastAPI server running BAAI/bge-m3 model.
 * Provides embeddings for Arabic/multilingual text with 1024 dimensions.
 *
 * Model: BAAI/bge-m3 (or fine-tuned variant)
 * Dimensions: 1024
 *
 * Usage:
 *   1. Start the Python server: cd embedding-server && uvicorn main:app --port 8000
 *   2. Use these functions to generate embeddings
 *
 * Environment variables:
 *   BGE_M3_URL - Server URL (default: http://localhost:8000)
 */

import { BGE_DIMENSIONS } from "../constants";

// Re-export for convenience
export { BGE_DIMENSIONS };

// Default server URL
const BGE_SERVER_URL = process.env.BGE_M3_URL || "http://localhost:8000";

// Request timeout in milliseconds
const REQUEST_TIMEOUT = 30000;

interface EmbedResponse {
  embedding: number[];
  dimensions: number;
  latency_ms: number;
}

interface EmbedBatchResponse {
  embeddings: number[][];
  dimensions: number;
  count: number;
  latency_ms: number;
}

interface HealthResponse {
  status: string;
  model: string;
  dimensions: number;
  device: string;
}

/**
 * Check if the BGE-M3 embedding server is available
 */
export async function isBGEServerAvailable(): Promise<boolean> {
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 2000);

    const response = await fetch(`${BGE_SERVER_URL}/health`, {
      signal: controller.signal,
    });

    clearTimeout(timeoutId);
    return response.ok;
  } catch {
    return false;
  }
}

/**
 * Get health information from the BGE-M3 embedding server
 */
export async function getBGEServerHealth(): Promise<HealthResponse> {
  const response = await fetch(`${BGE_SERVER_URL}/health`);

  if (!response.ok) {
    throw new Error(`Health check failed: ${response.status}`);
  }

  return response.json();
}

/**
 * Generate embedding for a single text using BGE-M3 model
 *
 * @param text - Text to embed
 * @param textType - Type of text: 'query' for search queries, 'passage' for documents (default: 'query')
 * @returns Promise<number[]> - 1024-dimensional embedding vector
 * @throws Error if server is unavailable or request fails
 */
export async function generateEmbeddingBGE(
  text: string,
  textType: "query" | "passage" = "query"
): Promise<number[]> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);

  try {
    const response = await fetch(`${BGE_SERVER_URL}/embed`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ text, text_type: textType }),
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(
        `BGE embedding request failed: ${response.status} - ${errorText}`
      );
    }

    const data: EmbedResponse = await response.json();

    if (data.dimensions !== BGE_DIMENSIONS) {
      console.warn(
        `Unexpected BGE embedding dimensions: ${data.dimensions}, expected ${BGE_DIMENSIONS}`
      );
    }

    return data.embedding;
  } catch (error) {
    clearTimeout(timeoutId);

    if (error instanceof Error && error.name === "AbortError") {
      throw new Error(`BGE embedding request timed out after ${REQUEST_TIMEOUT}ms`);
    }

    throw error;
  }
}

/**
 * Generate embeddings for multiple texts in a single batch request
 *
 * More efficient than calling generateEmbeddingBGE multiple times.
 * Maximum batch size is 32 texts.
 *
 * @param texts - Array of texts to embed
 * @param textType - Type of text: 'query' for search queries, 'passage' for documents (default: 'passage')
 * @returns Promise<number[][]> - Array of 1024-dimensional embedding vectors
 * @throws Error if server is unavailable or request fails
 */
export async function generateEmbeddingsBGE(
  texts: string[],
  textType: "query" | "passage" = "passage"
): Promise<number[][]> {
  if (texts.length === 0) {
    return [];
  }

  // Split into chunks of 32 if needed
  const MAX_BATCH_SIZE = 32;
  if (texts.length > MAX_BATCH_SIZE) {
    const results: number[][] = [];
    for (let i = 0; i < texts.length; i += MAX_BATCH_SIZE) {
      const chunk = texts.slice(i, i + MAX_BATCH_SIZE);
      const chunkEmbeddings = await generateEmbeddingsBGE(chunk, textType);
      results.push(...chunkEmbeddings);
    }
    return results;
  }

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);

  try {
    const response = await fetch(`${BGE_SERVER_URL}/embed/batch`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ texts, text_type: textType }),
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(
        `BGE batch embedding request failed: ${response.status} - ${errorText}`
      );
    }

    const data: EmbedBatchResponse = await response.json();

    if (data.dimensions !== BGE_DIMENSIONS) {
      console.warn(
        `Unexpected BGE embedding dimensions: ${data.dimensions}, expected ${BGE_DIMENSIONS}`
      );
    }

    return data.embeddings;
  } catch (error) {
    clearTimeout(timeoutId);

    if (error instanceof Error && error.name === "AbortError") {
      throw new Error(
        `BGE batch embedding request timed out after ${REQUEST_TIMEOUT}ms`
      );
    }

    throw error;
  }
}
