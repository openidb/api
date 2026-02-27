/**
 * Elasticsearch Catalog Search
 *
 * Search functions for books and authors indices.
 * Returns ranked IDs for hybrid ES + PostgreSQL queries.
 * Returns null on ES errors to signal fallback to ILIKE.
 */

import { elasticsearch, ES_BOOKS_INDEX, ES_AUTHORS_INDEX, ES_PAGES_INDEX } from "./elasticsearch";
import { prisma } from "../db";
import { qdrant, PAGES_COLLECTION } from "../qdrant";

let indexedBookIdsCache: { ids: Set<string>; expiry: number } | null = null;

// 23 Turath hadith source book IDs — their content is indexed per-hadith
// (in separate ES/Qdrant collections), not per-page, so they always count as fully indexed.
const HADITH_SOURCE_BOOK_IDS = new Set([
  "1681", "1727", "1726", "7895", "1339", "1198", "25794", "1699",
  "21795", "8360", "17757", "12991", "2348", "13037", "12836", "19482",
  "31307", "8494", "1424", "537", "1733", "148486", "8361",
]);

/**
 * Get the set of book IDs that are fully indexed in both ES and Qdrant.
 * Hadith source books are always included (indexed per-hadith, not per-page).
 * Results are cached for 5 minutes. Returns null if ES is unavailable.
 */
export async function getIndexedBookIds(): Promise<Set<string> | null> {
  if (indexedBookIdsCache && Date.now() < indexedBookIdsCache.expiry) {
    return indexedBookIdsCache.ids;
  }
  try {
    // 1. Get per-book page counts from DB and ES in parallel
    const [dbRows, esResult] = await Promise.all([
      prisma.$queryRawUnsafe<{ book_id: string; count: bigint }[]>(
        `SELECT book_id, COUNT(*)::bigint AS count FROM pages GROUP BY book_id`
      ),
      elasticsearch.search({
        index: ES_PAGES_INDEX,
        size: 0,
        aggs: { book_ids: { terms: { field: "book_id", size: 15000 } } },
      }),
    ]);

    const dbCounts = new Map(dbRows.map((r) => [r.book_id, Number(r.count)]));

    const esBuckets = (esResult.aggregations?.book_ids as any)?.buckets ?? [];
    const esCounts = new Map<string, number>(
      esBuckets.map((b: any) => [String(b.key), b.doc_count as number])
    );

    // 2. Filter to books fully indexed in ES (es_count >= db_count)
    const fullyInES: string[] = [];
    esCounts.forEach((esCount, bookId) => {
      const dbCount = dbCounts.get(bookId);
      if (dbCount && esCount >= dbCount) {
        fullyInES.push(bookId);
      }
    });

    // 3. Check Qdrant for those books (parallel batches of 20)
    const fullyIndexed = new Set<string>(HADITH_SOURCE_BOOK_IDS);
    const BATCH_SIZE = 20;
    for (let i = 0; i < fullyInES.length; i += BATCH_SIZE) {
      const batch = fullyInES.slice(i, i + BATCH_SIZE);
      const results = await Promise.all(
        batch.map(async (bookId) => {
          try {
            const result = await qdrant.count(PAGES_COLLECTION, {
              filter: {
                must: [{ key: "bookId", match: { value: parseInt(bookId) } }],
              },
              exact: true,
            });
            return { bookId, qdrantCount: result.count };
          } catch {
            return { bookId, qdrantCount: 0 };
          }
        })
      );
      for (const { bookId, qdrantCount } of results) {
        const dbCount = dbCounts.get(bookId)!;
        if (qdrantCount >= dbCount) {
          fullyIndexed.add(bookId);
        }
      }
    }

    indexedBookIdsCache = { ids: fullyIndexed, expiry: Date.now() + 5 * 60 * 1000 };
    return fullyIndexed;
  } catch (error) {
    console.error("[ES] Failed to fetch indexed book IDs:", error);
    return null;
  }
}

const ARABIC_REGEX = /[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]/;
const NUMERIC_REGEX = /^\d+$/;

/**
 * Search books in Elasticsearch.
 * Returns ranked book IDs or null if ES is unavailable.
 */
export async function searchBooksES(query: string, limit: number): Promise<string[] | null> {
  try {
    const trimmed = query.trim();
    if (!trimmed) return [];

    let esQuery: Record<string, unknown>;

    if (NUMERIC_REGEX.test(trimmed)) {
      // Numeric query: exact ID match (boosted) + prefix match
      esQuery = {
        bool: {
          should: [
            { term: { id: { value: trimmed, boost: 100 } } },
            { prefix: { id: { value: trimmed, boost: 10 } } },
          ],
        },
      };
    } else if (ARABIC_REGEX.test(trimmed)) {
      // Arabic query: search Arabic title + author name
      esQuery = {
        multi_match: {
          query: trimmed,
          fields: ["title_arabic^3", "title_arabic.exact^2", "author_name_arabic"],
          fuzziness: "AUTO",
          type: "best_fields",
        },
      };
    } else {
      // Latin query: search Latin title + author name
      esQuery = {
        multi_match: {
          query: trimmed,
          fields: ["title_latin^3", "author_name_latin"],
          fuzziness: "AUTO",
          type: "best_fields",
        },
      };
    }

    const result = await elasticsearch.search({
      index: ES_BOOKS_INDEX,
      size: limit,
      _source: ["id"],
      query: esQuery,
    });

    return result.hits.hits.map((hit) => (hit._source as { id: string }).id);
  } catch (error) {
    console.error("[ES] Books search failed, falling back to ILIKE:", error);
    return null;
  }
}

/**
 * Search authors in Elasticsearch.
 * Returns ranked author IDs or null if ES is unavailable.
 */
export async function searchAuthorsES(query: string, limit: number): Promise<string[] | null> {
  try {
    const trimmed = query.trim();
    if (!trimmed) return [];

    let esQuery: Record<string, unknown>;

    if (NUMERIC_REGEX.test(trimmed)) {
      // Numeric query: exact ID match (boosted) + prefix match
      esQuery = {
        bool: {
          should: [
            { term: { id: { value: trimmed, boost: 100 } } },
            { prefix: { id: { value: trimmed, boost: 10 } } },
          ],
        },
      };
    } else if (ARABIC_REGEX.test(trimmed)) {
      // Arabic query: search name + kunya/nasab/nisba/laqab
      esQuery = {
        multi_match: {
          query: trimmed,
          fields: [
            "name_arabic^3",
            "name_arabic.exact^2",
            "kunya^2",
            "nasab",
            "nisba^2",
            "laqab",
          ],
          fuzziness: "AUTO",
          type: "best_fields",
        },
      };
    } else {
      // Latin query: search Latin name
      esQuery = {
        multi_match: {
          query: trimmed,
          fields: ["name_latin^3"],
          fuzziness: "AUTO",
          type: "best_fields",
        },
      };
    }

    const result = await elasticsearch.search({
      index: ES_AUTHORS_INDEX,
      size: limit,
      _source: ["id"],
      query: esQuery,
    });

    return result.hits.hits.map((hit) => (hit._source as { id: string }).id);
  } catch (error) {
    console.error("[ES] Authors search failed, falling back to ILIKE:", error);
    return null;
  }
}
