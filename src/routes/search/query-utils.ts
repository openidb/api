import { normalizeArabicText } from "../../embeddings";
import { MIN_CHARS_FOR_SEMANTIC } from "./config";
import type { ParsedQuery, SearchStrategy } from "./types";

/**
 * Prepare search terms for PostgreSQL full-text search
 */
export function prepareSearchTerms(query: string): string[] {
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
 * Supports regular quotes (""), Arabic quotes, and guillemets
 */
export function parseSearchQuery(query: string): ParsedQuery {
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
    if (phrase && phrase.includes(' ')) {
      const words = phrase.split(/\s+/)
        .map((w) => w.replace(/[^\u0600-\u06FF\w]/g, ""))
        .filter((w) => w.length > 0);
      if (words.length > 1) {
        phrases.push(words.join(' '));
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
 * Check if query contains quoted phrases (user wants exact match)
 */
export function hasQuotedPhrases(query: string): boolean {
  const quoteRegex = /["\u00AB\u00BB\u201E\u201C\u201D](.*?)["\u00AB\u00BB\u201E\u201C\u201D]/;
  return quoteRegex.test(query);
}

/**
 * Check if semantic search should be skipped for this query
 * (quoted phrases request exact match; too-short queries produce noise)
 */
export function shouldSkipSemanticSearch(query: string): boolean {
  if (hasQuotedPhrases(query)) return true;
  const normalized = normalizeArabicText(query);
  return normalized.replace(/\s/g, '').length < MIN_CHARS_FOR_SEMANTIC;
}

/**
 * Check if query contains Arabic characters
 */
export function isArabicQuery(query: string): boolean {
  const arabicPattern = /[\u0600-\u06FF\u0750-\u077F]/;
  return arabicPattern.test(query);
}

/**
 * Search strategy based on query language
 */
export function getSearchStrategy(query: string): SearchStrategy {
  if (!isArabicQuery(query)) {
    return 'semantic_only';
  }
  return 'hybrid';
}

/**
 * Calculate dynamic similarity threshold based on query characteristics
 */
const THRESHOLD_RULES: Array<{ maxChars: number; threshold: number }> = [
  { maxChars: 3, threshold: 0.55 },
  { maxChars: 6, threshold: 0.40 },
  { maxChars: 12, threshold: 0.30 },
];

export function getDynamicSimilarityThreshold(query: string, baseThreshold: number): number {
  const normalized = normalizeArabicText(query).trim();
  const charCount = normalized.replace(/\s/g, '').length;
  const wordCount = normalized.split(/\s+/).filter(w => w.length > 0).length;

  const effectiveChars = wordCount === 1 ? Math.min(charCount, 6) : charCount;
  const rule = THRESHOLD_RULES.find(r => effectiveChars <= r.maxChars);
  return rule ? Math.max(baseThreshold, rule.threshold) : baseThreshold;
}
