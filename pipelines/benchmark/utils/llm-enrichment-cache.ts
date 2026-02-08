/**
 * SQLite cache for LLM-generated contextual enrichments.
 * Keyed by (technique_id, content_type, content_id) to avoid redundant LLM calls.
 * Uses bun:sqlite (Bun's built-in SQLite) instead of better-sqlite3.
 */

import { Database } from "bun:sqlite";
import path from "path";
import fs from "fs";

const CACHE_DIR = path.join(process.cwd(), ".cache");
const DB_PATH = path.join(CACHE_DIR, "llm-enrichment.db");

let db: Database | null = null;

function initDb(): boolean {
  if (db) return true;

  try {
    if (!fs.existsSync(CACHE_DIR)) {
      fs.mkdirSync(CACHE_DIR, { recursive: true });
    }

    db = new Database(DB_PATH);
    db.exec("PRAGMA journal_mode = WAL");

    db.exec(`
      CREATE TABLE IF NOT EXISTS enrichments (
        technique_id TEXT NOT NULL,
        content_type TEXT NOT NULL,
        content_id TEXT NOT NULL,
        enriched_text TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        PRIMARY KEY (technique_id, content_type, content_id)
      )
    `);

    return true;
  } catch (err) {
    console.error("[LLMEnrichmentCache] Failed to initialize:", err);
    db = null;
    return false;
  }
}

/**
 * Get a cached enrichment for a specific item.
 */
export function getCachedEnrichment(
  techniqueId: string,
  contentType: "quran" | "hadith",
  contentId: string
): string | null {
  if (!initDb() || !db) return null;

  try {
    const row = db
      .query(
        "SELECT enriched_text FROM enrichments WHERE technique_id = ? AND content_type = ? AND content_id = ?"
      )
      .get(techniqueId, contentType, contentId) as
      | { enriched_text: string }
      | null;
    return row?.enriched_text ?? null;
  } catch {
    return null;
  }
}

/**
 * Get multiple cached enrichments at once.
 */
export function getCachedEnrichments(
  techniqueId: string,
  contentType: "quran" | "hadith",
  contentIds: string[]
): Map<string, string> {
  const result = new Map<string, string>();
  if (!initDb() || !db) return result;

  try {
    const stmt = db.query(
      "SELECT enriched_text FROM enrichments WHERE technique_id = ? AND content_type = ? AND content_id = ?"
    );
    for (const id of contentIds) {
      const row = stmt.get(techniqueId, contentType, id) as
        | { enriched_text: string }
        | null;
      if (row) {
        result.set(id, row.enriched_text);
      }
    }
  } catch {
    // ignore
  }

  return result;
}

/**
 * Store an enrichment in the cache.
 */
export function setCachedEnrichment(
  techniqueId: string,
  contentType: "quran" | "hadith",
  contentId: string,
  enrichedText: string
): void {
  if (!initDb() || !db) return;

  try {
    db.query(
      "INSERT OR REPLACE INTO enrichments (technique_id, content_type, content_id, enriched_text, created_at) VALUES (?, ?, ?, ?, ?)"
    ).run(techniqueId, contentType, contentId, enrichedText, Date.now());
  } catch {
    // ignore
  }
}

/**
 * Store multiple enrichments in the cache (transactional).
 */
export function setCachedEnrichments(
  entries: Array<{
    techniqueId: string;
    contentType: "quran" | "hadith";
    contentId: string;
    enrichedText: string;
  }>
): void {
  if (!initDb() || !db) return;

  try {
    const now = Date.now();
    const stmt = db.query(
      "INSERT OR REPLACE INTO enrichments (technique_id, content_type, content_id, enriched_text, created_at) VALUES (?, ?, ?, ?, ?)"
    );

    const insertMany = db.transaction(() => {
      for (const item of entries) {
        stmt.run(
          item.techniqueId,
          item.contentType,
          item.contentId,
          item.enrichedText,
          now
        );
      }
    });
    insertMany();
  } catch {
    // ignore
  }
}

/**
 * Get cache statistics.
 */
export function getEnrichmentCacheStats(): {
  count: number;
  byTechnique: Record<string, number>;
} | null {
  if (!initDb() || !db) return null;

  try {
    const total = db
      .query("SELECT COUNT(*) as count FROM enrichments")
      .get() as { count: number };
    const byTechnique = db
      .query(
        "SELECT technique_id, COUNT(*) as count FROM enrichments GROUP BY technique_id"
      )
      .all() as Array<{ technique_id: string; count: number }>;

    return {
      count: total.count,
      byTechnique: Object.fromEntries(
        byTechnique.map((r) => [r.technique_id, r.count])
      ),
    };
  } catch {
    return null;
  }
}

/**
 * Close the database connection.
 */
export function closeEnrichmentDb(): void {
  if (db) {
    db.close();
    db = null;
  }
}
