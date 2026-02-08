/**
 * Source URL Generation Utilities
 *
 * Centralized URL generation for all external data sources.
 * Used by both API routes (for attribution) and pipelines (for embedding payloads).
 */

export const SOURCES = {
  shamela: [{ name: "Maktaba Shamela", url: "https://shamela.ws", type: "backup" }],
  sunnah: [{ name: "sunnah.com", url: "https://sunnah.com", type: "scrape" }],
  quranCloud: [{ name: "Al Quran Cloud API", url: "https://api.alquran.cloud", type: "api" }],
  tafsir: [
    { name: "spa5k/tafsir_api", url: "https://github.com/spa5k/tafsir_api", type: "api" },
    { name: "quran-tafseer.com", url: "http://api.quran-tafseer.com", type: "api" },
  ],
  quranTranslation: [{ name: "fawazahmed0/quran-api", url: "https://github.com/fawazahmed0/quran-api", type: "api" }],
} as const;

// Collections that use /collection/book/hadith format instead of /collection:hadith
const BOOK_PATH_COLLECTIONS = new Set(["malik", "bulugh"]);

/**
 * Generate the correct sunnah.com URL for a hadith.
 * Most collections use /collection:hadith format, but some use /collection/book/hadith.
 */
export function generateSunnahUrl(
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
export function generateQuranUrl(surahNumber: number, ayahNumber: number): string {
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
 * Accepts editionId (e.g. "ar-tafsir-ibn-kathir") or legacy source slug.
 */
export function generateTafsirSourceUrl(
  editionIdOrSource: string,
  surahNumber: number
): string {
  // Legacy source values
  if (editionIdOrSource === "jalalayn") {
    return `https://cdn.jsdelivr.net/gh/spa5k/tafsir_api@main/tafsir/ar-jalalayn/${surahNumber}.json`;
  }
  if (editionIdOrSource === "ibn_kathir") {
    return `https://cdn.jsdelivr.net/gh/spa5k/tafsir_api@main/tafsir/ar-tafsir-ibn-kathir/${surahNumber}.json`;
  }
  // Edition ID — direct CDN URL
  return `https://cdn.jsdelivr.net/gh/spa5k/tafsir_api@main/tafsir/${editionIdOrSource}/${surahNumber}.json`;
}

/**
 * Generate a source URL for a Quran translation edition.
 */
export function generateTranslationSourceUrl(editionId: string): string {
  return `https://cdn.jsdelivr.net/gh/fawazahmed0/quran-api@1/editions/${editionId}.json`;
}
