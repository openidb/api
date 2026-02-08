/**
 * Elasticsearch Search Functions
 *
 * Replaces PostgreSQL FTS with Elasticsearch for keyword search.
 * Provides the same interfaces as the original functions for seamless integration.
 */

import {
  elasticsearch,
  ES_PAGES_INDEX,
  ES_HADITHS_INDEX,
  ES_AYAHS_INDEX,
} from "./elasticsearch";
import { normalizeArabicText } from "../embeddings";
import { generateSunnahComUrl, generateQuranComUrl, generateShamelaPageUrl } from "../utils/source-urls";
import type { QueryDslQueryContainer, SearchHit } from "@elastic/elasticsearch/lib/api/types";

export { generateSunnahComUrl };

// ============================================================================
// Type Definitions (matching route.ts interfaces)
// ============================================================================

export interface RankedResult {
  bookId: string;
  pageNumber: number;
  volumeNumber: number;
  textSnippet: string;
  highlightedSnippet: string;
  semanticRank?: number;
  keywordRank?: number;
  semanticScore?: number;
  keywordScore?: number;
  tsRank?: number;
  bm25Score?: number;
  fusedScore?: number;
  urlPageIndex?: string;
  shamelaUrl?: string;
}

export interface HadithRankedResult {
  score: number;
  semanticScore?: number;
  rank?: number;
  bookId: number;
  collectionSlug: string;
  collectionNameArabic: string;
  collectionNameEnglish: string;
  bookNumber: number;
  bookNameArabic: string;
  bookNameEnglish: string;
  hadithNumber: string;
  text: string;
  chapterArabic: string | null;
  chapterEnglish: string | null;
  sunnahComUrl: string;
  translation?: string;
  semanticRank?: number;
  keywordRank?: number;
  tsRank?: number;
  bm25Score?: number;
}

export interface AyahRankedResult {
  score: number;
  semanticScore?: number;
  rank?: number;
  surahNumber: number;
  ayahNumber: number;
  ayahEnd?: number;
  ayahNumbers?: number[];
  surahNameArabic: string;
  surahNameEnglish: string;
  text: string;
  translation?: string;
  juzNumber: number;
  pageNumber: number;
  quranComUrl: string;
  isChunk?: boolean;
  wordCount?: number;
  semanticRank?: number;
  keywordRank?: number;
  tsRank?: number;
  bm25Score?: number;
}

interface ParsedQuery {
  phrases: string[];
  terms: string[];
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Check if query contains Arabic characters
 */
function isArabicQuery(query: string): boolean {
  const arabicPattern = /[\u0600-\u06FF\u0750-\u077F]/;
  return arabicPattern.test(query);
}

/**
 * Prepare search terms (normalize and clean)
 */
function prepareSearchTerms(query: string): string[] {
  const normalized = normalizeArabicText(query);
  return normalized
    .trim()
    .split(/\s+/)
    .filter((term) => term.length > 0)
    .map((term) => term.replace(/[^\u0600-\u06FF\w]/g, ""))
    .filter((term) => term.length > 0);
}

/**
 * Parse search query to extract quoted phrases and individual terms
 * Supports regular quotes (""), Arabic quotes (\u00AB\u00BB), and guillemets
 */
function parseSearchQuery(query: string): ParsedQuery {
  const phrases: string[] = [];
  const terms: string[] = [];

  const quoteRegex = /["\u00AB\u00BB\u201E\u201C\u201D](.*?)["\u00AB\u00BB\u201E\u201C\u201D]/g;
  let match;
  let lastIndex = 0;

  while ((match = quoteRegex.exec(query)) !== null) {
    const before = query.slice(lastIndex, match.index).trim();
    if (before) {
      terms.push(...prepareSearchTerms(before));
    }

    const phrase = normalizeArabicText(match[1]).trim();
    if (phrase && phrase.includes(" ")) {
      const words = phrase
        .split(/\s+/)
        .map((w) => w.replace(/[^\u0600-\u06FF\w]/g, ""))
        .filter((w) => w.length > 0);
      if (words.length > 1) {
        phrases.push(words.join(" "));
      } else if (words.length === 1) {
        terms.push(words[0]);
      }
    } else if (phrase) {
      const cleaned = phrase.replace(/[^\u0600-\u06FF\w]/g, "");
      if (cleaned.length > 0) {
        terms.push(cleaned);
      }
    }

    lastIndex = quoteRegex.lastIndex;
  }

  const remaining = query.slice(lastIndex).trim();
  if (remaining) {
    terms.push(...prepareSearchTerms(remaining));
  }

  return { phrases, terms };
}

/**
 * Build Elasticsearch query from parsed query
 * - Phrases use match_phrase for exact sequence matching
 * - Terms use match with OR
 */
function buildESQuery(
  parsed: ParsedQuery,
  textField: string,
  bookId?: string | null
): QueryDslQueryContainer {
  const must: QueryDslQueryContainer[] = [];
  const should: QueryDslQueryContainer[] = [];

  // Phrases: require exact sequence matching
  for (const phrase of parsed.phrases) {
    must.push({
      match_phrase: {
        [`${textField}.exact`]: {
          query: phrase,
          slop: 0, // Exact order
        },
      },
    });
  }

  // Terms: at least one should match (OR)
  if (parsed.terms.length > 0) {
    should.push({
      match: {
        [textField]: {
          query: parsed.terms.join(" "),
          operator: "or",
        },
      },
    });
  }

  const boolQuery: QueryDslQueryContainer = {
    bool: {
      ...(must.length > 0 ? { must } : {}),
      ...(should.length > 0
        ? { should, minimum_should_match: must.length > 0 ? 0 : 1 }
        : {}),
    },
  };

  // Add book filter if specified
  if (bookId) {
    return {
      bool: {
        must: [boolQuery],
        filter: [{ term: { book_id: bookId } }],
      },
    };
  }

  return boolQuery;
}

/**
 * Build fuzzy Elasticsearch query for fallback search
 */
function buildFuzzyESQuery(
  query: string,
  textField: string,
  fuzziness: string = "AUTO",
  bookId?: string | null
): QueryDslQueryContainer {
  const normalized = normalizeArabicText(query);

  const matchQuery: QueryDslQueryContainer = {
    match: {
      [textField]: {
        query: normalized,
        fuzziness,
        operator: "or",
      },
    },
  };

  if (bookId) {
    return {
      bool: {
        must: [matchQuery],
        filter: [{ term: { book_id: bookId } }],
      },
    };
  }

  return matchQuery;
}

// ============================================================================
// Elasticsearch Type Definitions
// ============================================================================

interface PageDoc {
  book_id: string;
  page_number: number;
  volume_number: number;
  content_plain: string;
  text_searchable: string;
  url_page_index?: string;
}

interface HadithDoc {
  id: number;
  book_id: number;
  hadith_number: string;
  text_arabic: string;
  text_plain: string;
  text_searchable: string;
  chapter_arabic: string | null;
  chapter_english: string | null;
  is_chain_variation: boolean;
  book_number: number;
  book_name_arabic: string;
  book_name_english: string;
  collection_slug: string;
  collection_name_arabic: string;
  collection_name_english: string;
}

interface AyahDoc {
  id: number;
  ayah_number: number;
  text_uthmani: string;
  text_plain: string;
  text_searchable: string;
  juz_number: number;
  page_number: number;
  surah_id: number;
  surah_number: number;
  surah_name_arabic: string;
  surah_name_english: string;
}

// ============================================================================
// Pages Search Functions
// ============================================================================

/**
 * Keyword search for book pages using Elasticsearch
 * Replaces PostgreSQL FTS keywordSearch function
 */
export async function keywordSearchES(
  query: string,
  limit: number,
  bookId: string | null,
  options: { fuzzyFallback?: boolean } = {}
): Promise<RankedResult[]> {
  const { fuzzyFallback = true } = options;

  // Skip keyword search for non-Arabic queries
  if (!isArabicQuery(query)) {
    console.log(`[ES PageKeyword] skipped: non-Arabic query`);
    return [];
  }

  const parsed = parseSearchQuery(query);
  if (parsed.phrases.length === 0 && parsed.terms.length === 0) {
    return [];
  }

  const esQuery = buildESQuery(parsed, "text_searchable", bookId);

  try {
    const response = await elasticsearch.search<PageDoc>({
      index: ES_PAGES_INDEX,
      query: esQuery,
      size: limit,
      highlight: {
        fields: {
          text_searchable: {
            pre_tags: ["<mark>"],
            post_tags: ["</mark>"],
            fragment_size: 200,
            number_of_fragments: 1,
          },
        },
      },
      _source: [
        "book_id",
        "page_number",
        "volume_number",
        "content_plain",
        "url_page_index",
      ],
    });

    const hits = response.hits.hits;

    if (hits.length === 0 && fuzzyFallback && parsed.terms.length > 0) {
      console.log(
        `[ES PageKeyword] No exact matches for "${query}", trying fuzzy...`
      );
      const termsOnlyQuery = parsed.terms.join(" ");
      return fuzzyKeywordSearchES(termsOnlyQuery, limit, bookId);
    }

    return hits.map((hit, index) => mapPageHitToResult(hit, index));
  } catch (error) {
    console.error("[ES PageKeyword] Search error:", error);
    return [];
  }
}

/**
 * Fuzzy keyword search for book pages using Elasticsearch
 */
export async function fuzzyKeywordSearchES(
  query: string,
  limit: number,
  bookId: string | null,
  fuzziness: string = "AUTO"
): Promise<RankedResult[]> {
  const normalized = normalizeArabicText(query);
  if (normalized.trim().length < 2) return [];

  const esQuery = buildFuzzyESQuery(query, "text_searchable", fuzziness, bookId);

  try {
    const response = await elasticsearch.search<PageDoc>({
      index: ES_PAGES_INDEX,
      query: esQuery,
      size: limit,
      _source: [
        "book_id",
        "page_number",
        "volume_number",
        "content_plain",
        "url_page_index",
      ],
    });

    return response.hits.hits.map((hit, index) => mapPageHitToResult(hit, index));
  } catch (error) {
    console.error("[ES PageKeyword Fuzzy] Search error:", error);
    return [];
  }
}

function mapPageHitToResult(
  hit: SearchHit<PageDoc>,
  index: number
): RankedResult {
  const source = hit._source!;
  const highlight = hit.highlight?.text_searchable?.[0];

  return {
    bookId: source.book_id,
    pageNumber: source.page_number,
    volumeNumber: source.volume_number,
    textSnippet: source.content_plain.slice(0, 300),
    highlightedSnippet: highlight || source.content_plain.slice(0, 300),
    keywordRank: index + 1,
    keywordScore: hit._score || 0,
    tsRank: hit._score || 0,
    bm25Score: hit._score || 0,
    urlPageIndex: source.url_page_index,
    shamelaUrl: generateShamelaPageUrl(source.book_id, source.page_number),
  };
}

// ============================================================================
// Hadiths Search Functions
// ============================================================================

/**
 * Keyword search for hadiths using Elasticsearch
 * Replaces PostgreSQL FTS keywordSearchHadiths function
 */
export async function keywordSearchHadithsES(
  query: string,
  limit: number,
  options: { fuzzyFallback?: boolean } = {}
): Promise<HadithRankedResult[]> {
  const _t0 = Date.now();
  const { fuzzyFallback = true } = options;

  // Skip keyword search for non-Arabic queries
  if (!isArabicQuery(query)) {
    console.log(`[ES HadithKeyword] skipped: non-Arabic query`);
    return [];
  }

  const parsed = parseSearchQuery(query);
  if (parsed.phrases.length === 0 && parsed.terms.length === 0) {
    return [];
  }

  console.log(
    `[ES HadithKeyword] parsed:`,
    JSON.stringify(parsed)
  );

  const baseQuery = buildESQuery(parsed, "text_searchable", null);

  // Wrap with filter to exclude chain variations
  const esQuery: QueryDslQueryContainer = {
    bool: {
      must: [baseQuery],
      must_not: [{ term: { is_chain_variation: true } }],
    },
  };

  try {
    const _tSearch = Date.now();
    const response = await elasticsearch.search<HadithDoc>({
      index: ES_HADITHS_INDEX,
      query: esQuery,
      size: limit,
      _source: [
        "id",
        "book_id",
        "hadith_number",
        "text_arabic",
        "text_plain",
        "chapter_arabic",
        "chapter_english",
        "book_number",
        "book_name_arabic",
        "book_name_english",
        "collection_slug",
        "collection_name_arabic",
        "collection_name_english",
      ],
    });
    console.log(
      `[ES HadithKeyword] Search: ${Date.now() - _tSearch}ms (${response.hits.hits.length} results)`
    );

    const hits = response.hits.hits;

    if (hits.length === 0 && fuzzyFallback && parsed.terms.length > 0) {
      console.log(
        `[ES HadithKeyword] No exact matches for "${query}", trying fuzzy...`
      );
      const _tFuzzy = Date.now();
      const termsOnlyQuery = parsed.terms.join(" ");
      const fuzzyResults = await fuzzyKeywordSearchHadithsES(
        termsOnlyQuery,
        limit
      );
      console.log(
        `[ES HadithKeyword] Fuzzy fallback: ${Date.now() - _tFuzzy}ms (${fuzzyResults.length} results)`
      );
      return fuzzyResults;
    }

    const results = hits.map((hit, index) => mapHadithHitToResult(hit, index));
    console.log(`[ES HadithKeyword] total: ${Date.now() - _t0}ms`);
    return results;
  } catch (error) {
    console.error("[ES HadithKeyword] Search error:", error);
    return [];
  }
}

/**
 * Fuzzy keyword search for hadiths using Elasticsearch
 */
export async function fuzzyKeywordSearchHadithsES(
  query: string,
  limit: number,
  fuzziness: string = "AUTO"
): Promise<HadithRankedResult[]> {
  const normalized = normalizeArabicText(query);
  if (normalized.trim().length < 2) return [];

  const baseQuery = buildFuzzyESQuery(query, "text_searchable", fuzziness, null);

  // Wrap with filter to exclude chain variations
  const esQuery: QueryDslQueryContainer = {
    bool: {
      must: [baseQuery],
      must_not: [{ term: { is_chain_variation: true } }],
    },
  };

  try {
    const response = await elasticsearch.search<HadithDoc>({
      index: ES_HADITHS_INDEX,
      query: esQuery,
      size: limit,
      _source: [
        "id",
        "book_id",
        "hadith_number",
        "text_arabic",
        "text_plain",
        "chapter_arabic",
        "chapter_english",
        "book_number",
        "book_name_arabic",
        "book_name_english",
        "collection_slug",
        "collection_name_arabic",
        "collection_name_english",
      ],
    });

    return response.hits.hits.map((hit, index) =>
      mapHadithHitToResult(hit, index)
    );
  } catch (error) {
    console.error("[ES HadithKeyword Fuzzy] Search error:", error);
    return [];
  }
}

function mapHadithHitToResult(
  hit: SearchHit<HadithDoc>,
  index: number
): HadithRankedResult {
  const source = hit._source!;

  return {
    score: hit._score || 0,
    bookId: source.book_id,
    collectionSlug: source.collection_slug,
    collectionNameArabic: source.collection_name_arabic,
    collectionNameEnglish: source.collection_name_english,
    bookNumber: source.book_number,
    bookNameArabic: source.book_name_arabic,
    bookNameEnglish: source.book_name_english,
    hadithNumber: source.hadith_number,
    text: source.text_arabic,
    chapterArabic: source.chapter_arabic,
    chapterEnglish: source.chapter_english,
    sunnahComUrl: generateSunnahComUrl(source.collection_slug, source.hadith_number, source.book_number),
    keywordRank: index + 1,
    tsRank: hit._score || 0,
    bm25Score: hit._score || 0,
  };
}

// ============================================================================
// Ayahs Search Functions
// ============================================================================

/**
 * Keyword search for Quran ayahs using Elasticsearch
 * Replaces PostgreSQL FTS keywordSearchAyahs function
 */
export async function keywordSearchAyahsES(
  query: string,
  limit: number,
  options: { fuzzyFallback?: boolean } = {}
): Promise<AyahRankedResult[]> {
  const { fuzzyFallback = true } = options;

  // Skip keyword search for non-Arabic queries
  if (!isArabicQuery(query)) {
    console.log(`[ES AyahKeyword] skipped: non-Arabic query`);
    return [];
  }

  const parsed = parseSearchQuery(query);
  if (parsed.phrases.length === 0 && parsed.terms.length === 0) {
    return [];
  }

  const esQuery = buildESQuery(parsed, "text_searchable", null);

  try {
    const response = await elasticsearch.search<AyahDoc>({
      index: ES_AYAHS_INDEX,
      query: esQuery,
      size: limit,
      _source: [
        "id",
        "ayah_number",
        "text_uthmani",
        "text_plain",
        "juz_number",
        "page_number",
        "surah_id",
        "surah_number",
        "surah_name_arabic",
        "surah_name_english",
      ],
    });

    const hits = response.hits.hits;

    if (hits.length === 0 && fuzzyFallback && parsed.terms.length > 0) {
      console.log(
        `[ES AyahKeyword] No exact matches for "${query}", trying fuzzy...`
      );
      const termsOnlyQuery = parsed.terms.join(" ");
      return fuzzyKeywordSearchAyahsES(termsOnlyQuery, limit);
    }

    return hits.map((hit, index) => mapAyahHitToResult(hit, index));
  } catch (error) {
    console.error("[ES AyahKeyword] Search error:", error);
    return [];
  }
}

/**
 * Fuzzy keyword search for Quran ayahs using Elasticsearch
 */
export async function fuzzyKeywordSearchAyahsES(
  query: string,
  limit: number,
  fuzziness: string = "AUTO"
): Promise<AyahRankedResult[]> {
  const normalized = normalizeArabicText(query);
  if (normalized.trim().length < 2) return [];

  const esQuery = buildFuzzyESQuery(query, "text_searchable", fuzziness, null);

  try {
    const response = await elasticsearch.search<AyahDoc>({
      index: ES_AYAHS_INDEX,
      query: esQuery,
      size: limit,
      _source: [
        "id",
        "ayah_number",
        "text_uthmani",
        "text_plain",
        "juz_number",
        "page_number",
        "surah_id",
        "surah_number",
        "surah_name_arabic",
        "surah_name_english",
      ],
    });

    return response.hits.hits.map((hit, index) => mapAyahHitToResult(hit, index));
  } catch (error) {
    console.error("[ES AyahKeyword Fuzzy] Search error:", error);
    return [];
  }
}

function mapAyahHitToResult(
  hit: SearchHit<AyahDoc>,
  index: number
): AyahRankedResult {
  const source = hit._source!;

  return {
    score: hit._score || 0,
    surahNumber: source.surah_number,
    ayahNumber: source.ayah_number,
    surahNameArabic: source.surah_name_arabic,
    surahNameEnglish: source.surah_name_english,
    text: source.text_uthmani,
    juzNumber: source.juz_number,
    pageNumber: source.page_number,
    quranComUrl: generateQuranComUrl(source.surah_number, source.ayah_number),
    keywordRank: index + 1,
    tsRank: hit._score || 0,
    bm25Score: hit._score || 0,
  };
}
