import { prisma } from "../../db";
import { DB_STATS_CACHE_TTL } from "./config";
import type { DatabaseStats } from "./types";

// Database stats cache
let databaseStatsCache: { stats: DatabaseStats; expires: number } | null = null;

/**
 * Get database statistics for debug panel (cached)
 */
export async function getDatabaseStats(): Promise<DatabaseStats> {
  if (databaseStatsCache && databaseStatsCache.expires > Date.now()) {
    return databaseStatsCache.stats;
  }

  const [booksResult, pagesResult, hadithsResult, ayahsResult] = await Promise.all([
    prisma.book.count(),
    prisma.page.count(),
    prisma.hadith.count(),
    prisma.ayah.count(),
  ]);

  const stats: DatabaseStats = {
    totalBooks: booksResult,
    totalPages: pagesResult,
    totalHadiths: hadithsResult,
    totalAyahs: ayahsResult,
  };

  databaseStatsCache = { stats, expires: Date.now() + DB_STATS_CACHE_TTL };
  return stats;
}

/**
 * Extract paragraph texts from page HTML content
 */
export function extractParagraphTexts(html: string): string[] {
  const paragraphs: string[] = [];
  const pRegex = /<p[^>]*>([\s\S]*?)<\/p>/gi;
  let match;
  while ((match = pRegex.exec(html)) !== null) {
    const text = match[1]
      .replace(/<[^>]+>/g, "")
      .replace(/&nbsp;/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    if (text.length > 1) paragraphs.push(text);
  }
  return paragraphs;
}

/**
 * Find the paragraph index that best matches a search snippet
 */
export function findMatchingParagraphIndex(snippet: string, paragraphs: string[]): number {
  const cleanSnippet = snippet
    .replace(/<\/?mark>/gi, "")
    .replace(/\s+/g, " ")
    .trim();

  for (let i = 0; i < paragraphs.length; i++) {
    const normalizedParagraph = paragraphs[i].replace(/\s+/g, " ").trim();
    if (normalizedParagraph.includes(cleanSnippet) || cleanSnippet.includes(normalizedParagraph)) {
      return i;
    }
  }

  const snippetWords = new Set(cleanSnippet.split(/\s+/).filter(w => w.length > 2));
  let bestIndex = 0;
  let bestOverlap = 0;

  for (let i = 0; i < paragraphs.length; i++) {
    const paragraphWords = paragraphs[i].split(/\s+/).filter(w => w.length > 2);
    const overlap = paragraphWords.filter(w => snippetWords.has(w)).length;
    if (overlap > bestOverlap) {
      bestOverlap = overlap;
      bestIndex = i;
    }
  }

  return bestIndex;
}

/**
 * Get book metadata from cache or fetch from DB (request-scoped)
 */
export async function getBookMetadataForReranking(
  bookIds: string[],
  cache: Map<string, { id: string; titleArabic: string; author: { nameArabic: string } }>
): Promise<Map<string, { id: string; titleArabic: string; author: { nameArabic: string } }>> {
  const uncachedIds = bookIds.filter(id => !cache.has(id));

  if (uncachedIds.length > 0) {
    const fetchedBooks = await prisma.book.findMany({
      where: { id: { in: uncachedIds } },
      select: {
        id: true,
        titleArabic: true,
        author: { select: { nameArabic: true } },
      },
    });

    for (const book of fetchedBooks) {
      cache.set(book.id, book);
    }
  }

  const result = new Map<string, { id: string; titleArabic: string; author: { nameArabic: string } }>();
  for (const id of bookIds) {
    const book = cache.get(id);
    if (book) {
      result.set(id, book);
    }
  }
  return result;
}
