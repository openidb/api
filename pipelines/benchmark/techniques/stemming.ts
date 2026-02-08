/**
 * Technique 7: Stemming
 * Apply Light10 Arabic stemmer (prefix/suffix removal) before embedding.
 */

import type { RetrievalTechnique, QuranAyahData, HadithData } from "../types";
import {
  normalizeArabicText,
  truncateForEmbedding,
} from "../../../src/embeddings/gemini";
import { stemText } from "../utils/arabic-stemmer";

export const stemmingTechnique: RetrievalTechnique = {
  id: "stemming",
  name: "Stemming",
  description: "Light Arabic stemmer (prefix/suffix removal, not root extraction)",

  async prepareQuranText(ayah: QuranAyahData): Promise<string> {
    const normalized = normalizeArabicText(ayah.textPlain);
    const stemmed = stemText(normalized);
    return truncateForEmbedding(stemmed);
  },

  async prepareHadithText(hadith: HadithData): Promise<string> {
    const normalized = normalizeArabicText(hadith.textPlain);
    const stemmed = stemText(normalized);
    return truncateForEmbedding(stemmed);
  },
};
