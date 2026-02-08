/**
 * Evaluate Embedding Model Precision
 *
 * Computes precision-focused metrics against a gold standard evaluation set.
 * Supports both Gemini and BGE-M3 embeddings.
 *
 * Metrics computed:
 * - Precision@K: Proportion of retrieved results that are relevant
 * - Recall@K: Proportion of relevant results that are retrieved
 * - MRR (Mean Reciprocal Rank): Average of 1/rank of first relevant result
 * - NDCG@K: Normalized Discounted Cumulative Gain
 * - False Positive Rate: Rate of irrelevant results in top K
 *
 * Output:
 * - Summary metrics by category
 * - Detailed error analysis
 * - Failed queries for targeted training
 *
 * Usage:
 *   bun run training/scripts/evaluate-precision.ts [options]
 *
 * Options:
 *   --model=<type>       Model: gemini | bge-m3 (default: gemini)
 *   --k=<n>              Top K for evaluation (default: 5)
 *   --gold-file=<file>   Gold standard file (default: gold_standard_evaluation.jsonl)
 *   --output=<file>      Output report file (default: evaluation_report.json)
 *   --verbose            Show detailed output for each query
 *   --category=<cat>     Only evaluate specific category
 */

import "dotenv/config";
import * as fs from "fs";
import * as path from "path";
import { qdrant, QDRANT_QURAN_ENRICHED_COLLECTION, QDRANT_HADITH_COLLECTION } from "../../lib/qdrant";
import { generateEmbedding } from "../../lib/embeddings";

// Types
interface RelevantDoc {
  id: string;
  text?: string;
  relevance: number; // 1-3, where 3 is most relevant
}

interface GoldStandardQuery {
  query: string;
  relevant: RelevantDoc[];
  not_relevant?: string[];
  category: string;
  difficulty: "easy" | "medium" | "hard";
  notes?: string;
}

interface SearchResult {
  id: string;
  score: number;
  text?: string;
}

interface QueryResult {
  query: string;
  category: string;
  difficulty: string;
  retrieved: SearchResult[];
  relevant_retrieved: number;
  total_relevant: number;
  precision_at_k: number;
  recall_at_k: number;
  reciprocal_rank: number;
  ndcg_at_k: number;
  false_positives: string[];
  missed_relevant: string[];
  first_relevant_rank: number | null;
  success: boolean;
}

interface CategoryMetrics {
  count: number;
  precision_at_k: number;
  recall_at_k: number;
  mrr: number;
  ndcg_at_k: number;
  false_positive_rate: number;
  success_rate: number;
}

interface EvaluationReport {
  model: string;
  k: number;
  timestamp: string;
  total_queries: number;
  overall_metrics: {
    precision_at_k: number;
    recall_at_k: number;
    mrr: number;
    ndcg_at_k: number;
    false_positive_rate: number;
    success_rate: number;
  };
  by_category: Record<string, CategoryMetrics>;
  by_difficulty: Record<string, CategoryMetrics>;
  failed_queries: QueryResult[];
  all_results: QueryResult[];
}

// Configuration
const DATA_DIR = path.join(__dirname, "../data");
const DEFAULT_K = 5;
const BGE_COLLECTIONS = {
  quran: "quran_ayahs_enriched_bge",
  hadith: "sunnah_hadiths_bge",
};

// Parse arguments
function parseArgs() {
  const args = process.argv.slice(2);
  const options: Record<string, string | boolean> = {};

  for (const arg of args) {
    if (arg.startsWith("--")) {
      const [key, value] = arg.slice(2).split("=");
      options[key] = value ?? true;
    }
  }

  return {
    model: (options["model"] as string) || "gemini",
    k: parseInt(options["k"] as string) || DEFAULT_K,
    goldFile: (options["gold-file"] as string) || path.join(DATA_DIR, "gold_standard_evaluation.jsonl"),
    output: (options["output"] as string) || path.join(DATA_DIR, "evaluation_report.json"),
    verbose: options["verbose"] === true,
    category: options["category"] as string | undefined,
  };
}

/**
 * Load gold standard evaluation set
 */
function loadGoldStandard(filePath: string): GoldStandardQuery[] {
  if (!fs.existsSync(filePath)) {
    throw new Error(`Gold standard file not found: ${filePath}`);
  }

  const content = fs.readFileSync(filePath, "utf-8");
  const lines = content.trim().split("\n").filter(Boolean);

  return lines.map((line, i) => {
    try {
      return JSON.parse(line) as GoldStandardQuery;
    } catch (e) {
      throw new Error(`Invalid JSON at line ${i + 1}: ${e}`);
    }
  });
}

/**
 * Determine which collection to search based on query ID patterns
 */
function getCollection(model: string, queryRelevant: RelevantDoc[]): string {
  // Check if this is a Quran or Hadith query based on relevant IDs
  const hasQuran = queryRelevant.some((r) => r.id.startsWith("quran_"));
  const hasHadith = queryRelevant.some((r) => r.id.startsWith("hadith_"));

  if (model === "bge-m3") {
    // For mixed queries, default to hadith (larger collection)
    if (hasQuran && !hasHadith) return BGE_COLLECTIONS.quran;
    if (hasHadith && !hasQuran) return BGE_COLLECTIONS.hadith;
    return BGE_COLLECTIONS.hadith;
  } else {
    if (hasQuran && !hasHadith) return QDRANT_QURAN_ENRICHED_COLLECTION;
    if (hasHadith && !hasQuran) return QDRANT_HADITH_COLLECTION;
    return QDRANT_HADITH_COLLECTION;
  }
}

/**
 * Extract ID from Qdrant payload
 */
function extractIdFromPayload(payload: Record<string, unknown>, collection: string): string {
  // Quran collection
  if (collection.includes("quran")) {
    const surahNumber = payload.surahNumber || payload.surah_number;
    const ayahNumber = payload.ayahNumber || payload.ayah_number;
    if (surahNumber && ayahNumber) {
      return `quran_${surahNumber}_${ayahNumber}`;
    }
  }

  // Hadith collection
  if (collection.includes("hadith") || collection.includes("sunnah")) {
    const collectionSlug = payload.collectionSlug || payload.collection_slug || payload.collection;
    const hadithNumber = payload.hadithNumber || payload.hadith_number;
    if (collectionSlug && hadithNumber) {
      return `hadith_${collectionSlug}_${hadithNumber}`;
    }
    // Nawawi special case
    if (payload.bookSlug === "nawawi40" || payload.book_slug === "nawawi40") {
      return `hadith_nawawi_${hadithNumber}`;
    }
  }

  // Fallback to UUID
  return String(payload.id || "unknown");
}

/**
 * Search for a query using the specified model
 */
async function searchQuery(
  query: string,
  model: string,
  collection: string,
  limit: number
): Promise<SearchResult[]> {
  try {
    const embedding = await generateEmbedding(query, model as "gemini" | "bge-m3");

    const results = await qdrant.search(collection, {
      vector: embedding,
      limit: limit * 2, // Get extra for filtering
      with_payload: true,
    });

    return results.map((r) => ({
      id: extractIdFromPayload(r.payload as Record<string, unknown>, collection),
      score: r.score,
      text: (r.payload as Record<string, unknown>).textPlain as string ||
            (r.payload as Record<string, unknown>).text_plain as string ||
            (r.payload as Record<string, unknown>).textArabic as string ||
            undefined,
    })).slice(0, limit);
  } catch (error) {
    console.error(`Search error for query "${query.substring(0, 50)}...":`, error);
    return [];
  }
}

/**
 * Calculate NDCG@K
 */
function calculateNDCG(retrieved: SearchResult[], relevant: RelevantDoc[], k: number): number {
  const relevanceMap = new Map(relevant.map((r) => [r.id, r.relevance]));

  // DCG
  let dcg = 0;
  for (let i = 0; i < Math.min(k, retrieved.length); i++) {
    const rel = relevanceMap.get(retrieved[i].id) || 0;
    dcg += rel / Math.log2(i + 2); // i+2 because log2(1) = 0
  }

  // Ideal DCG (sorted by relevance)
  const idealRels = relevant.map((r) => r.relevance).sort((a, b) => b - a);
  let idcg = 0;
  for (let i = 0; i < Math.min(k, idealRels.length); i++) {
    idcg += idealRels[i] / Math.log2(i + 2);
  }

  return idcg > 0 ? dcg / idcg : 0;
}

/**
 * Evaluate a single query
 */
async function evaluateQuery(
  goldQuery: GoldStandardQuery,
  model: string,
  k: number
): Promise<QueryResult> {
  const collection = getCollection(model, goldQuery.relevant);
  const retrieved = await searchQuery(goldQuery.query, model, collection, k);

  const relevantIds = new Set(goldQuery.relevant.map((r) => r.id));
  const notRelevantIds = new Set(goldQuery.not_relevant || []);

  // Count relevant in retrieved
  let relevantRetrieved = 0;
  let firstRelevantRank: number | null = null;
  const falsePositives: string[] = [];
  const retrievedIds = new Set<string>();

  for (let i = 0; i < retrieved.length; i++) {
    const result = retrieved[i];
    retrievedIds.add(result.id);

    if (relevantIds.has(result.id)) {
      relevantRetrieved++;
      if (firstRelevantRank === null) {
        firstRelevantRank = i + 1;
      }
    } else if (notRelevantIds.has(result.id)) {
      // Explicit false positive
      falsePositives.push(result.id);
    }
  }

  // Find missed relevant
  const missedRelevant = goldQuery.relevant
    .filter((r) => !retrievedIds.has(r.id))
    .map((r) => r.id);

  // Calculate metrics
  const precisionAtK = retrieved.length > 0 ? relevantRetrieved / retrieved.length : 0;
  const recallAtK = goldQuery.relevant.length > 0 ? relevantRetrieved / goldQuery.relevant.length : 0;
  const reciprocalRank = firstRelevantRank ? 1 / firstRelevantRank : 0;
  const ndcgAtK = calculateNDCG(retrieved, goldQuery.relevant, k);

  // Success = at least one relevant in top K
  const success = relevantRetrieved > 0;

  return {
    query: goldQuery.query,
    category: goldQuery.category,
    difficulty: goldQuery.difficulty,
    retrieved,
    relevant_retrieved: relevantRetrieved,
    total_relevant: goldQuery.relevant.length,
    precision_at_k: precisionAtK,
    recall_at_k: recallAtK,
    reciprocal_rank: reciprocalRank,
    ndcg_at_k: ndcgAtK,
    false_positives: falsePositives,
    missed_relevant: missedRelevant,
    first_relevant_rank: firstRelevantRank,
    success,
  };
}

/**
 * Aggregate metrics for a group of results
 */
function aggregateMetrics(results: QueryResult[]): CategoryMetrics {
  if (results.length === 0) {
    return {
      count: 0,
      precision_at_k: 0,
      recall_at_k: 0,
      mrr: 0,
      ndcg_at_k: 0,
      false_positive_rate: 0,
      success_rate: 0,
    };
  }

  const count = results.length;
  const precision = results.reduce((sum, r) => sum + r.precision_at_k, 0) / count;
  const recall = results.reduce((sum, r) => sum + r.recall_at_k, 0) / count;
  const mrr = results.reduce((sum, r) => sum + r.reciprocal_rank, 0) / count;
  const ndcg = results.reduce((sum, r) => sum + r.ndcg_at_k, 0) / count;
  const fp = results.reduce((sum, r) => sum + r.false_positives.length, 0) /
             results.reduce((sum, r) => sum + r.retrieved.length, 0);
  const success = results.filter((r) => r.success).length / count;

  return {
    count,
    precision_at_k: precision,
    recall_at_k: recall,
    mrr,
    ndcg_at_k: ndcg,
    false_positive_rate: fp,
    success_rate: success,
  };
}

/**
 * Main evaluation function
 */
async function main() {
  const options = parseArgs();

  console.log("=".repeat(60));
  console.log("Precision Evaluation for Embedding Model");
  console.log("=".repeat(60));
  console.log();

  // Load gold standard
  console.log(`Loading gold standard from: ${options.goldFile}`);
  let goldQueries = loadGoldStandard(options.goldFile);
  console.log(`Loaded ${goldQueries.length} evaluation queries\n`);

  // Filter by category if specified
  if (options.category) {
    goldQueries = goldQueries.filter((q) => q.category === options.category);
    console.log(`Filtered to ${goldQueries.length} queries in category: ${options.category}\n`);
  }

  // Configuration summary
  console.log("Configuration:");
  console.log(`  Model: ${options.model}`);
  console.log(`  K: ${options.k}`);
  console.log(`  Queries: ${goldQueries.length}`);
  console.log(`  Output: ${options.output}`);
  console.log();

  // Evaluate each query
  console.log("Evaluating queries...");
  const results: QueryResult[] = [];

  for (let i = 0; i < goldQueries.length; i++) {
    const goldQuery = goldQueries[i];

    if (options.verbose) {
      console.log(`\n[${i + 1}/${goldQueries.length}] "${goldQuery.query.substring(0, 50)}..."`);
    }

    const result = await evaluateQuery(goldQuery, options.model, options.k);
    results.push(result);

    if (options.verbose) {
      console.log(`  P@${options.k}: ${result.precision_at_k.toFixed(3)}, ` +
                  `R@${options.k}: ${result.recall_at_k.toFixed(3)}, ` +
                  `RR: ${result.reciprocal_rank.toFixed(3)}, ` +
                  `Success: ${result.success}`);
      if (result.false_positives.length > 0) {
        console.log(`  False positives: ${result.false_positives.join(", ")}`);
      }
      if (result.missed_relevant.length > 0) {
        console.log(`  Missed: ${result.missed_relevant.slice(0, 3).join(", ")}`);
      }
    } else if ((i + 1) % 20 === 0) {
      console.log(`  Evaluated ${i + 1}/${goldQueries.length} queries...`);
    }

    // Small delay to avoid rate limits
    await new Promise((resolve) => setTimeout(resolve, 50));
  }

  // Aggregate metrics
  console.log("\nAggregating metrics...\n");

  const overallMetrics = aggregateMetrics(results);

  // By category
  const categories = [...new Set(results.map((r) => r.category))];
  const byCategory: Record<string, CategoryMetrics> = {};
  for (const cat of categories) {
    byCategory[cat] = aggregateMetrics(results.filter((r) => r.category === cat));
  }

  // By difficulty
  const difficulties = ["easy", "medium", "hard"];
  const byDifficulty: Record<string, CategoryMetrics> = {};
  for (const diff of difficulties) {
    byDifficulty[diff] = aggregateMetrics(results.filter((r) => r.difficulty === diff));
  }

  // Failed queries (for targeted training)
  const failedQueries = results.filter((r) => !r.success || r.precision_at_k < 0.5);

  // Create report
  const report: EvaluationReport = {
    model: options.model,
    k: options.k,
    timestamp: new Date().toISOString(),
    total_queries: goldQueries.length,
    overall_metrics: {
      precision_at_k: overallMetrics.precision_at_k,
      recall_at_k: overallMetrics.recall_at_k,
      mrr: overallMetrics.mrr,
      ndcg_at_k: overallMetrics.ndcg_at_k,
      false_positive_rate: overallMetrics.false_positive_rate,
      success_rate: overallMetrics.success_rate,
    },
    by_category: byCategory,
    by_difficulty: byDifficulty,
    failed_queries: failedQueries,
    all_results: results,
  };

  // Print summary
  console.log("=".repeat(60));
  console.log("EVALUATION RESULTS");
  console.log("=".repeat(60));
  console.log();

  console.log("Overall Metrics:");
  console.log(`  Precision@${options.k}: ${(overallMetrics.precision_at_k * 100).toFixed(1)}%`);
  console.log(`  Recall@${options.k}: ${(overallMetrics.recall_at_k * 100).toFixed(1)}%`);
  console.log(`  MRR: ${overallMetrics.mrr.toFixed(3)}`);
  console.log(`  NDCG@${options.k}: ${overallMetrics.ndcg_at_k.toFixed(3)}`);
  console.log(`  False Positive Rate: ${(overallMetrics.false_positive_rate * 100).toFixed(1)}%`);
  console.log(`  Success Rate: ${(overallMetrics.success_rate * 100).toFixed(1)}%`);
  console.log();

  console.log("By Category:");
  for (const [cat, metrics] of Object.entries(byCategory)) {
    console.log(`  ${cat} (n=${metrics.count}):`);
    console.log(`    P@${options.k}: ${(metrics.precision_at_k * 100).toFixed(1)}%, ` +
                `MRR: ${metrics.mrr.toFixed(3)}, ` +
                `Success: ${(metrics.success_rate * 100).toFixed(1)}%`);
  }
  console.log();

  console.log("By Difficulty:");
  for (const [diff, metrics] of Object.entries(byDifficulty)) {
    if (metrics.count > 0) {
      console.log(`  ${diff} (n=${metrics.count}):`);
      console.log(`    P@${options.k}: ${(metrics.precision_at_k * 100).toFixed(1)}%, ` +
                  `MRR: ${metrics.mrr.toFixed(3)}, ` +
                  `Success: ${(metrics.success_rate * 100).toFixed(1)}%`);
    }
  }
  console.log();

  console.log(`Failed/Low-Precision Queries: ${failedQueries.length}`);
  if (failedQueries.length > 0 && failedQueries.length <= 10) {
    for (const q of failedQueries) {
      console.log(`  - "${q.query.substring(0, 50)}..." (${q.category}, ${q.difficulty})`);
    }
  }
  console.log();

  // Target metrics comparison
  console.log("Target Comparison:");
  console.log(`  Precision@5:     ${(overallMetrics.precision_at_k * 100).toFixed(1)}% (target: >85%)`);
  console.log(`  MRR:             ${overallMetrics.mrr.toFixed(3)} (target: >0.80)`);
  console.log(`  FP Rate:         ${(overallMetrics.false_positive_rate * 100).toFixed(1)}% (target: <15%)`);
  console.log();

  // Save report
  fs.writeFileSync(options.output, JSON.stringify(report, null, 2));
  console.log(`Full report saved to: ${options.output}`);

  // Return exit code based on targets
  const meetsTargets =
    overallMetrics.precision_at_k >= 0.85 &&
    overallMetrics.mrr >= 0.80 &&
    overallMetrics.false_positive_rate <= 0.15;

  if (meetsTargets) {
    console.log("\n✓ All targets met!");
  } else {
    console.log("\n✗ Some targets not met - see report for details");
  }
}

main().catch((e) => {
  console.error("Evaluation failed:", e);
  process.exit(1);
});
