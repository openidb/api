/**
 * Technique 5: Metadata Prepending
 * Prepend structured metadata (surah name, ayah number, collection info) to text.
 */

import type { RetrievalTechnique, QuranAyahData, HadithData } from "../types";
import {
  normalizeArabicText,
  truncateForEmbedding,
} from "../../../src/embeddings/gemini";

export const metadataPrependingTechnique: RetrievalTechnique = {
  id: "metadata",
  name: "Metadata Prepending",
  description: "Prepend source metadata (surah/collection, ayah/hadith number) before text",

  async prepareQuranText(ayah: QuranAyahData): Promise<string> {
    const prefix = `سورة ${ayah.surahNameArabic}، آية ${ayah.ayahNumber}:`;
    const text = normalizeArabicText(ayah.textPlain);
    return truncateForEmbedding(`${prefix} ${text}`);
  },

  async prepareHadithText(hadith: HadithData): Promise<string> {
    let prefix = `${hadith.collectionNameArabic}`;
    if (hadith.chapterArabic) {
      prefix += `، ${hadith.chapterArabic}`;
    }
    prefix += ":";
    const text = normalizeArabicText(hadith.textPlain);
    return truncateForEmbedding(`${prefix} ${text}`);
  },
};
