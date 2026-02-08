/**
 * Qdrant Vector Database Client Singleton
 *
 * Provides a singleton Qdrant client instance for the application.
 * Prevents multiple instances in development (hot reload).
 */

import { QdrantClient } from "@qdrant/js-client-rest";

const globalForQdrant = globalThis as unknown as {
  qdrant: QdrantClient | undefined;
};

export const qdrant =
  globalForQdrant.qdrant ??
  new QdrantClient({
    url: process.env.QDRANT_URL || "http://localhost:6333",
  });

if (process.env.NODE_ENV !== "production") {
  globalForQdrant.qdrant = qdrant;
}

// Collection name for Arabic text pages
export const QDRANT_COLLECTION =
  process.env.QDRANT_COLLECTION || "arabic_texts_pages";

// Collection name for authors
export const QDRANT_AUTHORS_COLLECTION =
  process.env.QDRANT_AUTHORS_COLLECTION || "arabic_texts_authors";

/**
 * Quran ayahs collection
 * Embeddings include: metadata prefix + Arabic text + English translation
 * Payload contains: original text for display + embeddedText for debugging
 */
export const QDRANT_QURAN_COLLECTION =
  process.env.QDRANT_QURAN_COLLECTION || "quran_ayahs";

// Collection name for Hadith
export const QDRANT_HADITH_COLLECTION =
  process.env.QDRANT_HADITH_COLLECTION || "sunnah_hadiths";

// BGE-M3 collection variants (1024 dimensions)
export const QDRANT_COLLECTION_BGE = "arabic_texts_pages_bge";
export const QDRANT_QURAN_COLLECTION_BGE = "quran_ayahs_bge";
export const QDRANT_HADITH_COLLECTION_BGE = "sunnah_hadiths_bge";
// Aliases for backwards compatibility with scripts
export const PAGES_COLLECTION = QDRANT_COLLECTION;
export const PAGES_COLLECTION_BGE = QDRANT_COLLECTION_BGE;
export const HADITHS_COLLECTION = QDRANT_HADITH_COLLECTION;
export const HADITHS_COLLECTION_BGE = QDRANT_HADITH_COLLECTION_BGE;
export const QURAN_COLLECTION = QDRANT_QURAN_COLLECTION;
export const QURAN_COLLECTION_BGE = QDRANT_QURAN_COLLECTION_BGE;

// Re-export from constants for backwards compatibility
export { EMBEDDING_DIMENSIONS, GEMINI_DIMENSIONS, BGE_DIMENSIONS } from "./constants";

export default qdrant;
