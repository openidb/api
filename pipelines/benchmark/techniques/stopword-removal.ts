/**
 * Technique 2: Stopword Removal
 * Remove Arabic filler words before embedding to focus on content words.
 */

import type { RetrievalTechnique, QuranAyahData, HadithData } from "../types";
import {
  normalizeArabicText,
  truncateForEmbedding,
} from "../../../src/embeddings/gemini";
import { removeStopwords } from "../utils/arabic-stopwords";

export const stopwordRemovalTechnique: RetrievalTechnique = {
  id: "stopword",
  name: "Stopword Removal",
  description: "Remove Arabic filler words (prepositions, conjunctions, pronouns) before embedding",

  async prepareQuranText(ayah: QuranAyahData): Promise<string> {
    const normalized = normalizeArabicText(ayah.textPlain);
    const filtered = removeStopwords(normalized);
    return truncateForEmbedding(filtered);
  },

  async prepareHadithText(hadith: HadithData): Promise<string> {
    const normalized = normalizeArabicText(hadith.textPlain);
    const filtered = removeStopwords(normalized);
    return truncateForEmbedding(filtered);
  },
};
