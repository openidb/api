/**
 * Technique 6: Translation Augmentation
 * Append English translation to Arabic text with separator.
 * Format: "arabic ||| english"
 */

import type { RetrievalTechnique, QuranAyahData, HadithData } from "../types";
import {
  normalizeArabicText,
  truncateForEmbedding,
} from "../../../src/embeddings/gemini";

const SEPARATOR = " ||| ";

export const translationAugmentationTechnique: RetrievalTechnique = {
  id: "translation",
  name: "Translation Augmentation",
  description: "Append English translation: arabic ||| english",

  async prepareQuranText(ayah: QuranAyahData): Promise<string> {
    const arabic = normalizeArabicText(ayah.textPlain);
    if (ayah.translationText) {
      return truncateForEmbedding(arabic + SEPARATOR + ayah.translationText);
    }
    return truncateForEmbedding(arabic);
  },

  async prepareHadithText(hadith: HadithData): Promise<string> {
    const arabic = normalizeArabicText(hadith.textPlain);
    if (hadith.translationText) {
      return truncateForEmbedding(arabic + SEPARATOR + hadith.translationText);
    }
    return truncateForEmbedding(arabic);
  },
};
