/**
 * Technique registry: maps technique IDs to implementations.
 */

import type { RetrievalTechnique } from "./types";
import { baselineTechnique } from "./techniques/baseline";
import { stopwordRemovalTechnique } from "./techniques/stopword-removal";
import { contextualEnrichmentTechnique } from "./techniques/contextual-enrichment";
import { tafsirEnrichmentTechnique } from "./techniques/tafsir-enrichment";
import { metadataPrependingTechnique } from "./techniques/metadata-prepending";
import { translationAugmentationTechnique } from "./techniques/translation-augmentation";
import { stemmingTechnique } from "./techniques/stemming";
import { combinedTechnique } from "./techniques/combined";

export const ALL_TECHNIQUES: RetrievalTechnique[] = [
  baselineTechnique,
  stopwordRemovalTechnique,
  contextualEnrichmentTechnique,
  tafsirEnrichmentTechnique,
  metadataPrependingTechnique,
  translationAugmentationTechnique,
  stemmingTechnique,
  combinedTechnique,
];

const techniqueMap = new Map<string, RetrievalTechnique>(
  ALL_TECHNIQUES.map((t) => [t.id, t])
);

/**
 * Get techniques by IDs. If no IDs provided, returns all techniques.
 */
export function getTechniques(ids?: string[]): RetrievalTechnique[] {
  if (!ids || ids.length === 0) return ALL_TECHNIQUES;

  const result: RetrievalTechnique[] = [];
  for (const id of ids) {
    const technique = techniqueMap.get(id);
    if (!technique) {
      console.error(`Unknown technique: ${id}`);
      console.error(`Available: ${ALL_TECHNIQUES.map((t) => t.id).join(", ")}`);
      process.exit(1);
    }
    result.push(technique);
  }
  return result;
}

/**
 * Get Qdrant collection names for a technique.
 */
export function getCollectionNames(techniqueId: string): {
  quran: string;
  hadith: string;
} {
  return {
    quran: `bench_quran_${techniqueId}`,
    hadith: `bench_hadith_${techniqueId}`,
  };
}
