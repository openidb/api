/**
 * Source URL Generation Utilities
 *
 * Centralized URL generation for all external data sources.
 * Used by both API routes (for attribution) and pipelines (for embedding payloads).
 */

// Collections that use /collection/book/hadith format instead of /collection:hadith
const BOOK_PATH_COLLECTIONS = new Set(["malik", "bulugh"]);

/**
 * Generate the correct sunnah.com URL for a hadith.
 * Most collections use /collection:hadith format, but some use /collection/book/hadith.
 */
export function generateSunnahComUrl(
  collectionSlug: string,
  hadithNumber: string,
  bookNumber: number
): string {
  const cleanHadithNumber = hadithNumber.replace(/[A-Za-z]+$/, "");
  if (BOOK_PATH_COLLECTIONS.has(collectionSlug)) {
    return `https://sunnah.com/${collectionSlug}/${bookNumber}/${cleanHadithNumber}`;
  }
  return `https://sunnah.com/${collectionSlug}:${cleanHadithNumber}`;
}

/**
 * Generate a quran.com URL for a specific ayah.
 */
export function generateQuranComUrl(surahNumber: number, ayahNumber: number): string {
  return `https://quran.com/${surahNumber}?startingVerse=${ayahNumber}`;
}

/**
 * Generate a shamela.ws URL for a book.
 */
export function generateShamelaBookUrl(bookId: string): string {
  return `https://shamela.ws/book/${bookId}`;
}

/**
 * Generate a shamela.ws URL for a specific page in a book.
 */
export function generateShamelaPageUrl(bookId: string, pageNumber: number): string {
  return `https://shamela.ws/book/${bookId}/${pageNumber}`;
}

/**
 * Generate a source URL for a tafsir entry.
 */
export function generateTafsirSourceUrl(
  source: string,
  surahNumber: number,
  ayahNumber: number
): string {
  if (source === "ibn_kathir") {
    return `https://cdn.jsdelivr.net/gh/spa5k/tafsir_api@main/tafsir/ar-tafsir-ibn-kathir/${surahNumber}.json`;
  }
  // jalalayn (default)
  return `http://api.quran-tafseer.com/tafseer/2/${surahNumber}/${ayahNumber}`;
}

/**
 * Generate a source URL for a Quran translation edition.
 */
export function generateTranslationSourceUrl(editionId: string): string {
  return `https://cdn.jsdelivr.net/gh/fawazahmed0/quran-api@1/editions/${editionId}.json`;
}
