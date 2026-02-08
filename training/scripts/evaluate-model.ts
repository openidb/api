/**
 * Automated Model Evaluation Script
 *
 * Evaluates embedding models against the gold standard test set.
 * Computes Recall@K, MRR, and other metrics.
 *
 * Features:
 * - Multi-model comparison (Gemini, BGE-M3 base, BGE-M3 fine-tuned)
 * - Breakdown by category (quran_exact, quran_thematic, hadith_exact, etc.)
 * - Breakdown by language (Arabic, English)
 * - Breakdown by difficulty (easy, medium, hard)
 * - JSON output for analysis
 *
 * Usage:
 *   bun run training/scripts/evaluate-model.ts --model=bge-m3
 *   bun run training/scripts/evaluate-model.ts --model=gemini --test-set=gold_standard_v2.jsonl
 *   bun run training/scripts/evaluate-model.ts --compare-all
 */

import "dotenv/config";
import * as fs from "fs";
import * as path from "path";
import { qdrant } from "../../lib/qdrant";
import { generateEmbedding } from "../../lib/embeddings";

// Configuration
const DATA_DIR = path.join(__dirname, "../data");
const DEFAULT_TEST_SET = "gold_standard_v2.jsonl";
const DEFAULT_K_VALUES = [1, 5, 10, 20];

interface TestQuery {
  query: string;
  relevant: string[];
  category: string;
  difficulty: string;
  language: string;
  notes?: string;
}

interface EvaluationResult {
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
  first_relevant_rank: number | null;
}

interface AggregateMetrics {
  total_queries: number;
  recall_at_1: number;
  recall_at_5: number;
  recall_at_10: number;
  recall_at_20: number;
  mrr: number;
  avg_first_relevant_rank: number;
  by_category: Record<string, CategoryMetrics>;
  by_language: Record<string, CategoryMetrics>;
  by_difficulty: Record<string, CategoryMetrics>;
}

interface CategoryMetrics {
  count: number;
  recall_at_1: number;
  recall_at_5: number;
  recall_at_10: number;
  recall_at_20: number;
  mrr: number;
}

// Parse command line arguments
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
    model: (options["model"] as string) || "bge-m3",
    testSet: (options["test-set"] as string) || DEFAULT_TEST_SET,
    collection: options["collection"] as string | undefined,
    compareAll: options["compare-all"] === true,
    outputJson: options["output-json"] as string | undefined,
    verbose: options["verbose"] === true,
    limit: options["limit"] ? parseInt(options["limit"] as string) : undefined,
  };
}

/**
 * Load test queries from JSONL file
 */
function loadTestSet(filepath: string, limit?: number): TestQuery[] {
  const content = fs.readFileSync(filepath, "utf-8");
  const lines = content.trim().split("\n").filter(Boolean);
  const queries = lines.map((line) => JSON.parse(line) as TestQuery);
  return limit ? queries.slice(0, limit) : queries;
}

/**
 * Determine which collection to search based on query category
 */
function getCollection(category: string, model: string): string {
  const isQuran = category.startsWith("quran");

  if (model === "gemini") {
    return isQuran ? "quran_enriched_gemini" : "hadith_gemini";
  } else {
    // BGE-M3 collections
    return isQuran ? "quran_enriched_bge_m3" : "hadith_bge_m3";
  }
}

/**
 * Extract document ID from search result payload
 */
function extractDocId(payload: Record<string, unknown>): string | null {
  // Handle different payload formats
  if (payload.id) return payload.id as string;

  // Quran format: surah_ayah
  if (payload.surahNumber && payload.ayahNumber) {
    return `quran_${payload.surahNumber}_${payload.ayahNumber}`;
  }

  // Hadith format: collection_number
  if (payload.collectionSlug && payload.hadithNumber) {
    return `hadith_${payload.collectionSlug}_${payload.hadithNumber}`;
  }

  if (payload.collection_slug && payload.hadith_number) {
    return `hadith_${payload.collection_slug}_${payload.hadith_number}`;
  }

  return null;
}

/**
 * Search for similar documents
 */
async function search(
  query: string,
  collection: string,
  model: "gemini" | "bge-m3",
  limit: number = 20
): Promise<{ ids: string[]; scores: number[] }> {
  const embedding = await generateEmbedding(query, model);

  const results = await qdrant.search(collection, {
    vector: embedding,
    limit: limit,
    with_payload: true,
  });

  const ids: string[] = [];
  const scores: number[] = [];

  for (const result of results) {
    const id = extractDocId(result.payload as Record<string, unknown>);
    if (id) {
      ids.push(id);
      scores.push(result.score);
    }
  }

  return { ids, scores };
}

/**
 * Calculate recall at K
 */
function recallAtK(relevant: string[], retrieved: string[], k: number): number {
  const retrievedAtK = new Set(retrieved.slice(0, k));
  const relevantSet = new Set(relevant);

  let found = 0;
  for (const rel of relevantSet) {
    if (retrievedAtK.has(rel)) {
      found++;
    }
  }

  return found / relevantSet.size;
}

/**
 * Calculate reciprocal rank (for MRR)
 */
function reciprocalRank(relevant: string[], retrieved: string[]): number {
  const relevantSet = new Set(relevant);

  for (let i = 0; i < retrieved.length; i++) {
    if (relevantSet.has(retrieved[i])) {
      return 1 / (i + 1);
    }
  }

  return 0;
}

/**
 * Find rank of first relevant result
 */
function firstRelevantRank(relevant: string[], retrieved: string[]): number | null {
  const relevantSet = new Set(relevant);

  for (let i = 0; i < retrieved.length; i++) {
    if (relevantSet.has(retrieved[i])) {
      return i + 1;
    }
  }

  return null;
}

/**
 * Evaluate a single model
 */
async function evaluateModel(
  model: "gemini" | "bge-m3",
  testQueries: TestQuery[],
  collectionOverride?: string,
  verbose: boolean = false
): Promise<{ results: EvaluationResult[]; aggregate: AggregateMetrics }> {
  console.log(`\nEvaluating model: ${model}`);
  console.log(`Test queries: ${testQueries.length}`);
  console.log();

  const results: EvaluationResult[] = [];

  for (let i = 0; i < testQueries.length; i++) {
    const tq = testQueries[i];

    try {
      const collection = collectionOverride || getCollection(tq.category, model);
      const { ids, scores } = await search(tq.query, collection, model, 20);

      const result: EvaluationResult = {
        query: tq.query,
        category: tq.category,
        difficulty: tq.difficulty,
        language: tq.language,
        relevant: tq.relevant,
        retrieved: ids,
        retrieved_scores: scores,
        recall_at_1: recallAtK(tq.relevant, ids, 1),
        recall_at_5: recallAtK(tq.relevant, ids, 5),
        recall_at_10: recallAtK(tq.relevant, ids, 10),
        recall_at_20: recallAtK(tq.relevant, ids, 20),
        reciprocal_rank: reciprocalRank(tq.relevant, ids),
        first_relevant_rank: firstRelevantRank(tq.relevant, ids),
      };

      results.push(result);

      if (verbose) {
        const status = result.recall_at_10 > 0 ? "✓" : "✗";
        console.log(`  ${status} [${tq.category}] ${tq.query.substring(0, 40)}... R@10=${result.recall_at_10.toFixed(2)}`);
      }
    } catch (error) {
      console.error(`  Error evaluating: ${tq.query.substring(0, 40)}...`, error);
      continue;
    }

    // Progress
    if ((i + 1) % 20 === 0) {
      console.log(`  Processed ${i + 1}/${testQueries.length} queries...`);
    }
  }

  // Aggregate metrics
  const aggregate = calculateAggregateMetrics(results);

  return { results, aggregate };
}

/**
 * Calculate aggregate metrics from individual results
 */
function calculateAggregateMetrics(results: EvaluationResult[]): AggregateMetrics {
  const byCategory: Record<string, EvaluationResult[]> = {};
  const byLanguage: Record<string, EvaluationResult[]> = {};
  const byDifficulty: Record<string, EvaluationResult[]> = {};

  for (const r of results) {
    if (!byCategory[r.category]) byCategory[r.category] = [];
    byCategory[r.category].push(r);

    if (!byLanguage[r.language]) byLanguage[r.language] = [];
    byLanguage[r.language].push(r);

    if (!byDifficulty[r.difficulty]) byDifficulty[r.difficulty] = [];
    byDifficulty[r.difficulty].push(r);
  }

  const calcGroupMetrics = (group: EvaluationResult[]): CategoryMetrics => ({
    count: group.length,
    recall_at_1: group.reduce((sum, r) => sum + r.recall_at_1, 0) / group.length,
    recall_at_5: group.reduce((sum, r) => sum + r.recall_at_5, 0) / group.length,
    recall_at_10: group.reduce((sum, r) => sum + r.recall_at_10, 0) / group.length,
    recall_at_20: group.reduce((sum, r) => sum + r.recall_at_20, 0) / group.length,
    mrr: group.reduce((sum, r) => sum + r.reciprocal_rank, 0) / group.length,
  });

  const firstRanks = results
    .map((r) => r.first_relevant_rank)
    .filter((r): r is number => r !== null);

  return {
    total_queries: results.length,
    recall_at_1: results.reduce((sum, r) => sum + r.recall_at_1, 0) / results.length,
    recall_at_5: results.reduce((sum, r) => sum + r.recall_at_5, 0) / results.length,
    recall_at_10: results.reduce((sum, r) => sum + r.recall_at_10, 0) / results.length,
    recall_at_20: results.reduce((sum, r) => sum + r.recall_at_20, 0) / results.length,
    mrr: results.reduce((sum, r) => sum + r.reciprocal_rank, 0) / results.length,
    avg_first_relevant_rank: firstRanks.length > 0
      ? firstRanks.reduce((a, b) => a + b, 0) / firstRanks.length
      : 0,
    by_category: Object.fromEntries(
      Object.entries(byCategory).map(([k, v]) => [k, calcGroupMetrics(v)])
    ),
    by_language: Object.fromEntries(
      Object.entries(byLanguage).map(([k, v]) => [k, calcGroupMetrics(v)])
    ),
    by_difficulty: Object.fromEntries(
      Object.entries(byDifficulty).map(([k, v]) => [k, calcGroupMetrics(v)])
    ),
  };
}

/**
 * Print metrics in a formatted table
 */
function printMetrics(modelName: string, metrics: AggregateMetrics) {
  console.log("\n" + "=".repeat(60));
  console.log(`EVALUATION RESULTS: ${modelName}`);
  console.log("=".repeat(60));

  console.log("\n📊 Overall Metrics:");
  console.log(`  Total queries: ${metrics.total_queries}`);
  console.log(`  Recall@1:  ${(metrics.recall_at_1 * 100).toFixed(1)}%`);
  console.log(`  Recall@5:  ${(metrics.recall_at_5 * 100).toFixed(1)}%`);
  console.log(`  Recall@10: ${(metrics.recall_at_10 * 100).toFixed(1)}%`);
  console.log(`  Recall@20: ${(metrics.recall_at_20 * 100).toFixed(1)}%`);
  console.log(`  MRR:       ${metrics.mrr.toFixed(3)}`);
  console.log(`  Avg First Relevant Rank: ${metrics.avg_first_relevant_rank.toFixed(1)}`);

  console.log("\n📚 By Category:");
  const sortedCategories = Object.entries(metrics.by_category)
    .sort((a, b) => b[1].recall_at_10 - a[1].recall_at_10);

  for (const [category, m] of sortedCategories) {
    console.log(`  ${category.padEnd(20)} R@10=${(m.recall_at_10 * 100).toFixed(1).padStart(5)}%  MRR=${m.mrr.toFixed(3)}  (n=${m.count})`);
  }

  console.log("\n🌍 By Language:");
  for (const [lang, m] of Object.entries(metrics.by_language)) {
    console.log(`  ${lang.padEnd(10)} R@10=${(m.recall_at_10 * 100).toFixed(1).padStart(5)}%  MRR=${m.mrr.toFixed(3)}  (n=${m.count})`);
  }

  console.log("\n📈 By Difficulty:");
  for (const [diff, m] of Object.entries(metrics.by_difficulty)) {
    console.log(`  ${diff.padEnd(10)} R@10=${(m.recall_at_10 * 100).toFixed(1).padStart(5)}%  MRR=${m.mrr.toFixed(3)}  (n=${m.count})`);
  }
}

/**
 * Compare multiple models
 */
function printComparison(
  results: Record<string, AggregateMetrics>
) {
  console.log("\n" + "=".repeat(60));
  console.log("MODEL COMPARISON");
  console.log("=".repeat(60));

  const models = Object.keys(results);

  console.log("\n📊 Overall:");
  console.log("Model".padEnd(15) + "R@1".padStart(8) + "R@5".padStart(8) + "R@10".padStart(8) + "MRR".padStart(8));
  console.log("-".repeat(47));

  for (const model of models) {
    const m = results[model];
    console.log(
      model.padEnd(15) +
      `${(m.recall_at_1 * 100).toFixed(1)}%`.padStart(8) +
      `${(m.recall_at_5 * 100).toFixed(1)}%`.padStart(8) +
      `${(m.recall_at_10 * 100).toFixed(1)}%`.padStart(8) +
      m.mrr.toFixed(3).padStart(8)
    );
  }

  // Find best model
  const bestModel = models.reduce((best, model) =>
    results[model].recall_at_10 > results[best].recall_at_10 ? model : best
  );

  console.log(`\n🏆 Best model: ${bestModel} (R@10: ${(results[bestModel].recall_at_10 * 100).toFixed(1)}%)`);
}

async function main() {
  const options = parseArgs();

  console.log("=".repeat(60));
  console.log("Embedding Model Evaluation");
  console.log("=".repeat(60));

  // Load test set
  const testSetPath = path.join(DATA_DIR, options.testSet);
  if (!fs.existsSync(testSetPath)) {
    console.error(`ERROR: Test set not found: ${testSetPath}`);
    process.exit(1);
  }

  const testQueries = loadTestSet(testSetPath, options.limit);
  console.log(`\nLoaded ${testQueries.length} test queries from ${options.testSet}`);

  // Evaluate
  if (options.compareAll) {
    // Compare all models
    const allResults: Record<string, AggregateMetrics> = {};

    for (const model of ["gemini", "bge-m3"] as const) {
      try {
        const { aggregate } = await evaluateModel(
          model,
          testQueries,
          options.collection,
          options.verbose
        );
        allResults[model] = aggregate;
        printMetrics(model, aggregate);
      } catch (error) {
        console.error(`Error evaluating ${model}:`, error);
      }
    }

    if (Object.keys(allResults).length > 1) {
      printComparison(allResults);
    }

    // Save to JSON
    if (options.outputJson) {
      fs.writeFileSync(options.outputJson, JSON.stringify(allResults, null, 2));
      console.log(`\nResults saved to: ${options.outputJson}`);
    }
  } else {
    // Single model evaluation
    const model = options.model as "gemini" | "bge-m3";
    const { results, aggregate } = await evaluateModel(
      model,
      testQueries,
      options.collection,
      options.verbose
    );

    printMetrics(model, aggregate);

    // Save detailed results
    if (options.outputJson) {
      const output = {
        model,
        testSet: options.testSet,
        timestamp: new Date().toISOString(),
        aggregate,
        results,
      };
      fs.writeFileSync(options.outputJson, JSON.stringify(output, null, 2));
      console.log(`\nResults saved to: ${options.outputJson}`);
    }
  }

  console.log("\n" + "=".repeat(60));
  console.log("Evaluation complete!");
  console.log("=".repeat(60));
}

main().catch((e) => {
  console.error("Failed:", e);
  process.exit(1);
});
