/**
 * Technique 1: Baseline
 * Current production behavior: normalizeArabicText(textPlain)
 */

import type { RetrievalTechnique, QuranAyahData, HadithData } from "../types";
import {
  normalizeArabicText,
  truncateForEmbedding,
} from "../../../src/embeddings/gemini";

export const baselineTechnique: RetrievalTechnique = {
  id: "baseline",
  name: "Baseline",
  description: "Current production: normalizeArabicText(textPlain)",

  async prepareQuranText(ayah: QuranAyahData): Promise<string> {
    return truncateForEmbedding(normalizeArabicText(ayah.textPlain));
  },

  async prepareHadithText(hadith: HadithData): Promise<string> {
    return truncateForEmbedding(normalizeArabicText(hadith.textPlain));
  },
};
