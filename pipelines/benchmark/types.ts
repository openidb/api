/**
 * Core interfaces for the retrieval techniques benchmark.
 */

// ============================================================================
// Data types loaded from the database
// ============================================================================

export interface QuranAyahData {
  surahNumber: number;
  ayahNumber: number;
  textUthmani: string;
  textPlain: string;
  surahNameArabic: string;
  surahNameEnglish: string;
  juzNumber: number;
  pageNumber: number;
  tafsirText: string | null; // Al-Jalalayn tafsir
  translationText: string | null; // English translation
}

export interface HadithData {
  collectionSlug: string;
  collectionNameArabic: string;
  collectionNameEnglish: string;
  bookNameArabic: string;
  bookNameEnglish: string;
  hadithNumber: string;
  textArabic: string;
  textPlain: string;
  chapterArabic: string | null;
  chapterEnglish: string | null;
  translationText: string | null; // English translation
}

// ============================================================================
// Technique interface - each technique transforms text differently before embedding
// ============================================================================

export interface RetrievalTechnique {
  id: string; // e.g. "baseline", "stopword", "contextual"
  name: string; // Human-readable name
  description: string;
  prepareQuranText(ayah: QuranAyahData): Promise<string | null>;
  prepareHadithText(hadith: HadithData): Promise<string | null>;
}

// ============================================================================
// Test query / evaluation types
// ============================================================================

export interface TestQuery {
  query: string;
  relevant: string[]; // e.g. ["quran_2_255", "hadith_bukhari_1"]
  category: string; // e.g. "quran_exact", "quran_thematic", "hadith_exact"
  difficulty: string; // "easy", "medium", "hard"
  language: string; // "ar", "en"
  notes?: string;
}

export interface EvaluationResult {
  query: string;
  category: string;
  difficulty: string;
  language: string;
  relevant: string[];
  retrieved: string[];
  retrieved_scores: number[];
  recall_at_1: number;
  recall_at_5: number;
  recall_at_10: number;
  recall_at_20: number;
  reciprocal_rank: number;
  ndcg_at_10: number;
  precision_at_5: number;
  precision_at_10: number;
  first_relevant_rank: number | null;
  hit: boolean; // Whether any relevant doc was found in top 20
}

export interface CategoryMetrics {
  count: number;
  recall_at_1: number;
  recall_at_5: number;
  recall_at_10: number;
  recall_at_20: number;
  mrr: number;
  ndcg_at_10: number;
  precision_at_5: number;
  precision_at_10: number;
  hit_rate: number;
}

export interface AggregateMetrics {
  total_queries: number;
  recall_at_1: number;
  recall_at_5: number;
  recall_at_10: number;
  recall_at_20: number;
  mrr: number;
  ndcg_at_10: number;
  precision_at_5: number;
  precision_at_10: number;
  hit_rate: number;
  avg_first_relevant_rank: number;
  by_category: Record<string, CategoryMetrics>;
  by_language: Record<string, CategoryMetrics>;
  by_difficulty: Record<string, CategoryMetrics>;
}

// ============================================================================
// Benchmark result types
// ============================================================================

export interface TechniqueResult {
  technique_id: string;
  technique_name: string;
  aggregate: AggregateMetrics;
  results: EvaluationResult[];
}

export interface BenchmarkReport {
  timestamp: string;
  test_set: string;
  total_queries: number;
  techniques: TechniqueResult[];
}
