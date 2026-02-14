/**
 * Content Integrity Hashes
 *
 * SHA-256 hash functions for all content models. Each function uses a documented
 * canonical input format so users can independently verify content by recomputing
 * the hash from API response fields.
 *
 * Canonical formats:
 *   Ayah:              ayah:{surahNumber}:{ayahNumber}:{textUthmani}
 *   AyahTranslation:   ayah-translation:{surahNumber}:{ayahNumber}:{editionId}:{text}
 *   AyahTafsir:        ayah-tafsir:{surahNumber}:{ayahNumber}:{editionId}:{text}
 *   Hadith:            hadith:{collectionSlug}:{hadithNumber}:{textArabic}
 *   HadithTranslation: hadith-translation:{collectionSlug}:{hadithNumber}:{language}:{text}
 *   Page:              page:{bookId}:{pageNumber}:{contentPlain}
 *   PageTranslation:   page-translation:{bookId}:{pageNumber}:{language}:{paragraphsJson}
 *   DictionaryEntry:   dict-entry:{sourceSlug}:{headword}:{definitionPlain}
 */

import { createHash } from "crypto";

export function sha256(input: string): string {
  return createHash("sha256").update(input, "utf8").digest("hex");
}

export function hashAyah(surahNumber: number, ayahNumber: number, textUthmani: string): string {
  return sha256(`ayah:${surahNumber}:${ayahNumber}:${textUthmani}`);
}

export function hashAyahTranslation(
  surahNumber: number,
  ayahNumber: number,
  editionId: string,
  text: string,
): string {
  return sha256(`ayah-translation:${surahNumber}:${ayahNumber}:${editionId}:${text}`);
}

export function hashAyahTafsir(
  surahNumber: number,
  ayahNumber: number,
  editionId: string,
  text: string,
): string {
  return sha256(`ayah-tafsir:${surahNumber}:${ayahNumber}:${editionId}:${text}`);
}

export function hashHadith(collectionSlug: string, hadithNumber: string, textArabic: string): string {
  return sha256(`hadith:${collectionSlug}:${hadithNumber}:${textArabic}`);
}

export function hashHadithTranslation(
  collectionSlug: string,
  hadithNumber: string,
  language: string,
  text: string,
): string {
  return sha256(`hadith-translation:${collectionSlug}:${hadithNumber}:${language}:${text}`);
}

export function hashPage(bookId: string, pageNumber: number, contentPlain: string): string {
  return sha256(`page:${bookId}:${pageNumber}:${contentPlain}`);
}

export function hashPageTranslation(
  bookId: string,
  pageNumber: number,
  language: string,
  paragraphs: unknown,
): string {
  return sha256(`page-translation:${bookId}:${pageNumber}:${language}:${JSON.stringify(paragraphs)}`);
}

export function hashDictionaryEntry(
  sourceSlug: string,
  headword: string,
  definitionPlain: string,
): string {
  return sha256(`dict-entry:${sourceSlug}:${headword}:${definitionPlain}`);
}
