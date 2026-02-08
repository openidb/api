/**
 * Technique 4: Tafsir Enrichment
 * Use Al-Jalalayn tafsir as embedding text for Quran (already includes ayah text).
 * For Hadith, prepend chapter title for additional context.
 */

import type { RetrievalTechnique, QuranAyahData, HadithData } from "../types";
import {
  normalizeArabicText,
  truncateForEmbedding,
} from "../../../src/embeddings/gemini";

export const tafsirEnrichmentTechnique: RetrievalTechnique = {
  id: "tafsir",
  name: "Tafsir Enrichment",
  description: "Use Al-Jalalayn tafsir for Quran; chapter titles for Hadith",

  async prepareQuranText(ayah: QuranAyahData): Promise<string | null> {
    // Use tafsir text if available (it includes the ayah text within it)
    if (!ayah.tafsirText) {
      // Fall back to plain text if no tafsir available
      return truncateForEmbedding(normalizeArabicText(ayah.textPlain));
    }
    return truncateForEmbedding(normalizeArabicText(ayah.tafsirText));
  },

  async prepareHadithText(hadith: HadithData): Promise<string> {
    const text = normalizeArabicText(hadith.textPlain);
    // Prepend chapter title if available
    if (hadith.chapterArabic) {
      return truncateForEmbedding(`${hadith.chapterArabic}، ${text}`);
    }
    return truncateForEmbedding(text);
  },
};
