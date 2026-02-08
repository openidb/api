/**
 * Shared constants across the application
 */

// Embedding dimensions for Gemini embedding-001 model (cloud)
export const GEMINI_DIMENSIONS = 3072;

// Embedding dimensions for BGE-M3 model (local)
export const BGE_DIMENSIONS = 1024;

// Default embedding dimensions (Gemini for backwards compatibility)
export const EMBEDDING_DIMENSIONS = GEMINI_DIMENSIONS;
