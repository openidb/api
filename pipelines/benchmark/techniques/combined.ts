/**
 * Technique 8: Combined Best
 * Combines top-performing techniques: contextual enrichment + translation + metadata.
 * Format: "[metadata] [contextual description]\narabic_text ||| english_translation"
 */

import type { RetrievalTechnique, QuranAyahData, HadithData } from "../types";
import {
  normalizeArabicText,
  truncateForEmbedding,
} from "../../../src/embeddings/gemini";
import {
  getCachedEnrichment,
} from "../utils/llm-enrichment-cache";

const CONTEXTUAL_TECHNIQUE_ID = "contextual";
const SEPARATOR = " ||| ";

export const combinedTechnique: RetrievalTechnique = {
  id: "combined",
  name: "Combined Best",
  description: "Combine contextual enrichment + translation + metadata",

  async prepareQuranText(ayah: QuranAyahData): Promise<string> {
    const parts: string[] = [];

    // 1. Metadata prefix
    parts.push(`سورة ${ayah.surahNameArabic}، آية ${ayah.ayahNumber}:`);

    // 2. Contextual enrichment (from cache, generated during contextual technique run)
    const contentId = `quran_${ayah.surahNumber}_${ayah.ayahNumber}`;
    const enrichment = getCachedEnrichment(
      CONTEXTUAL_TECHNIQUE_ID,
      "quran",
      contentId
    );
    if (enrichment) {
      parts.push(enrichment);
    }

    // 3. Arabic text
    const arabic = normalizeArabicText(ayah.textPlain);
    parts.push(arabic);

    // 4. English translation
    if (ayah.translationText) {
      parts.push(SEPARATOR + ayah.translationText);
    }

    return truncateForEmbedding(parts.join("\n"));
  },

  async prepareHadithText(hadith: HadithData): Promise<string> {
    const parts: string[] = [];

    // 1. Metadata prefix
    let prefix = hadith.collectionNameArabic;
    if (hadith.chapterArabic) {
      prefix += `، ${hadith.chapterArabic}`;
    }
    parts.push(prefix + ":");

    // 2. Contextual enrichment (from cache)
    const contentId = `hadith_${hadith.collectionSlug}_${hadith.hadithNumber}`;
    const enrichment = getCachedEnrichment(
      CONTEXTUAL_TECHNIQUE_ID,
      "hadith",
      contentId
    );
    if (enrichment) {
      parts.push(enrichment);
    }

    // 3. Arabic text
    const arabic = normalizeArabicText(hadith.textPlain);
    parts.push(arabic);

    // 4. English translation
    if (hadith.translationText) {
      parts.push(SEPARATOR + hadith.translationText);
    }

    return truncateForEmbedding(parts.join("\n"));
  },
};
