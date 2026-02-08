/**
 * Persistent SQLite-based embedding cache
 *
 * Stores embeddings in SQLite to persist across server restarts.
 * Falls back to in-memory cache on any SQLite errors.
 */

import Database from "better-sqlite3";
import path from "path";
import fs from "fs";

// Cache directory - stored in project root
const CACHE_DIR = path.join(process.cwd(), ".cache");
const DB_PATH = path.join(CACHE_DIR, "embeddings.db");

// TTL for cached embeddings (7 days - embeddings don't change)
const TTL_MS = 7 * 24 * 60 * 60 * 1000;

let db: Database.Database | null = null;
let getStmt: Database.Statement | null = null;
let setStmt: Database.Statement | null = null;

/**
 * Initialize SQLite database and create table if needed
 */
function initDb(): boolean {
  if (db) return true;

  try {
    // Ensure cache directory exists
    if (!fs.existsSync(CACHE_DIR)) {
      fs.mkdirSync(CACHE_DIR, { recursive: true });
    }

    db = new Database(DB_PATH);

    // Enable WAL mode for better concurrent read performance
    db.pragma("journal_mode = WAL");

    // Create table if it doesn't exist
    db.exec(`
      CREATE TABLE IF NOT EXISTS embeddings (
        text_hash TEXT PRIMARY KEY,
        embedding BLOB NOT NULL,
        created_at INTEGER NOT NULL
      )
    `);

    // Create index on created_at for efficient cleanup
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_embeddings_created_at ON embeddings(created_at)
    `);

    // Prepare statements for reuse
    getStmt = db.prepare("SELECT embedding, created_at FROM embeddings WHERE text_hash = ?");
    setStmt = db.prepare("INSERT OR REPLACE INTO embeddings (text_hash, embedding, created_at) VALUES (?, ?, ?)");
    console.log("[PersistentEmbeddingCache] Initialized SQLite cache");
    return true;
  } catch (err) {
    console.error("[PersistentEmbeddingCache] Failed to initialize:", err);
    db = null;
    return false;
  }
}

/**
 * Generate a hash for the text to use as cache key
 * Using a simple hash to reduce storage size
 */
function hashText(text: string): string {
  // Simple hash function (FNV-1a)
  let hash = 2166136261;
  for (let i = 0; i < text.length; i++) {
    hash ^= text.charCodeAt(i);
    hash = (hash * 16777619) >>> 0;
  }
  return hash.toString(16) + "_" + text.length;
}

/**
 * Convert embedding array to Buffer for storage
 */
function embeddingToBuffer(embedding: number[]): Buffer {
  const buffer = Buffer.alloc(embedding.length * 4);
  for (let i = 0; i < embedding.length; i++) {
    buffer.writeFloatLE(embedding[i], i * 4);
  }
  return buffer;
}

/**
 * Convert Buffer back to embedding array
 */
function bufferToEmbedding(buffer: Buffer): number[] {
  const embedding: number[] = new Array(buffer.length / 4);
  for (let i = 0; i < embedding.length; i++) {
    embedding[i] = buffer.readFloatLE(i * 4);
  }
  return embedding;
}

/**
 * Get a cached embedding if available and not expired
 */
export function getPersistentCachedEmbedding(text: string): number[] | null {
  if (!initDb() || !getStmt) return null;

  try {
    const hash = hashText(text);
    const row = getStmt.get(hash) as { embedding: Buffer; created_at: number } | undefined;

    if (!row) return null;

    // Check if expired
    if (Date.now() - row.created_at > TTL_MS) {
      return null;
    }

    return bufferToEmbedding(row.embedding);
  } catch (err) {
    console.error("[PersistentEmbeddingCache] Error getting cached embedding:", err);
    return null;
  }
}

/**
 * Get multiple cached embeddings at once
 */
export function getPersistentCachedEmbeddings(texts: string[]): Map<string, number[]> {
  const result = new Map<string, number[]>();

  if (!initDb() || !getStmt) return result;

  try {
    const now = Date.now();
    for (const text of texts) {
      const hash = hashText(text);
      const row = getStmt.get(hash) as { embedding: Buffer; created_at: number } | undefined;

      if (row && now - row.created_at <= TTL_MS) {
        result.set(text, bufferToEmbedding(row.embedding));
      }
    }
  } catch (err) {
    console.error("[PersistentEmbeddingCache] Error getting cached embeddings:", err);
  }

  return result;
}

/**
 * Store an embedding in the cache
 */
export function setPersistentCachedEmbedding(text: string, embedding: number[]): void {
  if (!initDb() || !setStmt) return;

  try {
    const hash = hashText(text);
    const buffer = embeddingToBuffer(embedding);
    setStmt.run(hash, buffer, Date.now());
  } catch (err) {
    console.error("[PersistentEmbeddingCache] Error setting cached embedding:", err);
  }
}

/**
 * Store multiple embeddings in the cache at once
 */
export function setPersistentCachedEmbeddings(
  entries: Array<{ text: string; embedding: number[] }>
): void {
  if (!initDb() || !setStmt || !db) return;

  try {
    const now = Date.now();
    const insertMany = db.transaction((entries: Array<{ text: string; embedding: number[] }>) => {
      for (const { text, embedding } of entries) {
        const hash = hashText(text);
        const buffer = embeddingToBuffer(embedding);
        setStmt!.run(hash, buffer, now);
      }
    });
    insertMany(entries);
  } catch (err) {
    console.error("[PersistentEmbeddingCache] Error setting cached embeddings:", err);
  }
}
