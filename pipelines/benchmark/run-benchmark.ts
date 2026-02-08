/**
 * Run benchmark: evaluate each technique against gold standard queries.
 *
 * For each technique + query:
 *   1. Embed query via Gemini
 *   2. Search the technique's Qdrant collection
 *   3. Compute metrics (Recall@K, MRR, NDCG, Precision, Hit Rate)
 *
 * Usage:
 *   bun run scripts/benchmark-techniques/run-benchmark.ts [--techniques=...] [--test-set=gold_standard_v2.jsonl] [--output=results.json] [--verbose] [--limit=N]
 */

import "../env";
import * as fs from "fs";
import * as path from "path";
import { qdrant } from "../../src/qdrant";
import { generateEmbedding } from "../../src/embeddings/gemini";
import { getTechniques, getCollectionNames } from "./registry";
import { computeQueryMetrics, calculateAggregateMetrics } from "./utils/metrics";
import type {
  TestQuery,
  EvaluationResult,
  TechniqueResult,
  BenchmarkReport,
} from "./types";

// Parse args
const args = process.argv.slice(2);
const techniquesArg = args.find((a) => a.startsWith("--techniques="));
const techniqueIds = techniquesArg?.split("=")[1]?.split(",");
const testSetArg = args.find((a) => a.startsWith("--test-set="));
const testSetFile = testSetArg?.split("=")[1] || "gold_standard_v2.jsonl";
const outputArg = args.find((a) => a.startsWith("--output="));
const outputFile = outputArg?.split("=")[1] || "results/benchmark-results.json";
const verboseFlag = args.includes("--verbose");
const limitArg = args.find((a) => a.startsWith("--limit="));
const queryLimit = limitArg ? parseInt(limitArg.split("=")[1], 10) : undefined;

const SEARCH_LIMIT = 20;

/**
 * Load test queries from JSONL file.
 * Supports both v1 (relevant as objects with .id) and v2 (relevant as strings) format.
 */
function loadTestQueries(filepath: string, limit?: number): TestQuery[] {
  const content = fs.readFileSync(filepath, "utf-8");
  const lines = content.trim().split("\n").filter(Boolean);
  const queries = lines.map((line) => {
    const raw = JSON.parse(line);

    // Normalize relevant field: v1 has objects, v2 has strings
    let relevant: string[];
    if (raw.relevant && raw.relevant.length > 0 && typeof raw.relevant[0] === "object") {
      relevant = raw.relevant.map((r: { id: string }) => r.id);
    } else {
      relevant = raw.relevant || [];
    }

    return {
      query: raw.query,
      relevant,
      category: raw.category || "unknown",
      difficulty: raw.difficulty || "medium",
      language: raw.language || (isArabic(raw.query) ? "ar" : "en"),
      notes: raw.notes,
    } as TestQuery;
  });
  return limit ? queries.slice(0, limit) : queries;
}

function isArabic(text: string): boolean {
  return /[\u0600-\u06FF]/.test(text);
}

/**
 * Extract document ID from search result payload.
 */
function extractDocId(payload: Record<string, unknown>): string | null {
  // Quran format
  if (payload.surahNumber && payload.ayahNumber) {
    return `quran_${payload.surahNumber}_${payload.ayahNumber}`;
  }
  // Hadith format
  if (payload.collectionSlug && payload.hadithNumber) {
    return `hadith_${payload.collectionSlug}_${payload.hadithNumber}`;
  }
  return null;
}

/**
 * Determine content type from query category.
 */
function getContentType(category: string): "quran" | "hadith" {
  return category.startsWith("quran") ? "quran" : "hadith";
}

/**
 * Search a benchmark collection for a query.
 */
async function searchCollection(
  query: string,
  collectionName: string
): Promise<{ ids: string[]; scores: number[] }> {
  const embedding = await generateEmbedding(query);

  const results = await qdrant.search(collectionName, {
    vector: embedding,
    limit: SEARCH_LIMIT,
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
 * Evaluate a single technique against all test queries.
 */
async function evaluateTechnique(
  techniqueId: string,
  techniqueName: string,
  testQueries: TestQuery[]
): Promise<TechniqueResult> {
  console.log(`\n--- Evaluating: ${techniqueName} (${techniqueId}) ---`);

  const collections = getCollectionNames(techniqueId);
  const results: EvaluationResult[] = [];
  let errors = 0;

  for (let i = 0; i < testQueries.length; i++) {
    const tq = testQueries[i];

    try {
      const contentType = getContentType(tq.category);
      const collectionName =
        contentType === "quran" ? collections.quran : collections.hadith;

      const { ids, scores } = await searchCollection(tq.query, collectionName);
      const metrics = computeQueryMetrics(tq.relevant, ids, scores);

      results.push({
        query: tq.query,
        category: tq.category,
        difficulty: tq.difficulty,
        language: tq.language,
        relevant: tq.relevant,
        retrieved: ids,
        retrieved_scores: scores,
        ...metrics,
      });

      if (verboseFlag) {
        const status = metrics.recall_at_10 > 0 ? "+" : "-";
        console.log(
          `  ${status} [${tq.category}] ${tq.query.substring(0, 50)}... R@10=${metrics.recall_at_10.toFixed(2)} MRR=${metrics.reciprocal_rank.toFixed(2)}`
        );
      }
    } catch (error) {
      errors++;
      if (verboseFlag) {
        console.error(`  ! Error: ${tq.query.substring(0, 40)}...`, error);
      }
    }

    // Progress
    if (!verboseFlag && (i + 1) % 20 === 0) {
      console.log(`  Processed ${i + 1}/${testQueries.length} queries...`);
    }
  }

  if (errors > 0) {
    console.log(`  ${errors} queries failed`);
  }

  const aggregate = calculateAggregateMetrics(results);

  // Print summary
  console.log(
    `  Results: R@1=${(aggregate.recall_at_1 * 100).toFixed(1)}% R@5=${(aggregate.recall_at_5 * 100).toFixed(1)}% R@10=${(aggregate.recall_at_10 * 100).toFixed(1)}% MRR=${aggregate.mrr.toFixed(3)} NDCG@10=${aggregate.ndcg_at_10.toFixed(3)}`
  );

  return {
    technique_id: techniqueId,
    technique_name: techniqueName,
    aggregate,
    results,
  };
}

async function main() {
  const techniques = getTechniques(techniqueIds);

  console.log("=".repeat(60));
  console.log("Retrieval Techniques Benchmark");
  console.log("=".repeat(60));
  console.log(`Techniques: ${techniques.map((t) => t.id).join(", ")}`);
  console.log(`Test set: ${testSetFile}`);
  console.log(`Verbose: ${verboseFlag}`);

  // Load test queries
  const dataDir = path.join(__dirname, "../../training/data");
  const testSetPath = path.join(dataDir, testSetFile);

  if (!fs.existsSync(testSetPath)) {
    console.error(`Test set not found: ${testSetPath}`);
    process.exit(1);
  }

  const testQueries = loadTestQueries(testSetPath, queryLimit);
  console.log(`Loaded ${testQueries.length} test queries`);

  const quranQueries = testQueries.filter((q) =>
    q.category.startsWith("quran")
  ).length;
  const hadithQueries = testQueries.length - quranQueries;
  console.log(`  Quran: ${quranQueries}, Hadith: ${hadithQueries}`);
  console.log();

  // Evaluate each technique
  const techniqueResults: TechniqueResult[] = [];

  for (const technique of techniques) {
    const result = await evaluateTechnique(
      technique.id,
      technique.name,
      testQueries
    );
    techniqueResults.push(result);
  }

  // Build report
  const report: BenchmarkReport = {
    timestamp: new Date().toISOString(),
    test_set: testSetFile,
    total_queries: testQueries.length,
    techniques: techniqueResults,
  };

  // Save results
  const outputPath = path.join(__dirname, outputFile);
  const outputDir = path.dirname(outputPath);
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }
  fs.writeFileSync(outputPath, JSON.stringify(report, null, 2));
  console.log(`\nResults saved to: ${outputPath}`);

  // Print comparison table
  printComparison(techniqueResults);
}

function printComparison(results: TechniqueResult[]) {
  console.log("\n" + "=".repeat(80));
  console.log("TECHNIQUE COMPARISON");
  console.log("=".repeat(80));

  const header =
    "Technique".padEnd(20) +
    "R@1".padStart(7) +
    "R@5".padStart(7) +
    "R@10".padStart(7) +
    "R@20".padStart(7) +
    "MRR".padStart(7) +
    "NDCG".padStart(7) +
    "P@5".padStart(7) +
    "Hit%".padStart(7);
  console.log(header);
  console.log("-".repeat(80));

  for (const r of results) {
    const m = r.aggregate;
    console.log(
      r.technique_id.padEnd(20) +
        `${(m.recall_at_1 * 100).toFixed(1)}%`.padStart(7) +
        `${(m.recall_at_5 * 100).toFixed(1)}%`.padStart(7) +
        `${(m.recall_at_10 * 100).toFixed(1)}%`.padStart(7) +
        `${(m.recall_at_20 * 100).toFixed(1)}%`.padStart(7) +
        m.mrr.toFixed(3).padStart(7) +
        m.ndcg_at_10.toFixed(3).padStart(7) +
        `${(m.precision_at_5 * 100).toFixed(1)}%`.padStart(7) +
        `${(m.hit_rate * 100).toFixed(1)}%`.padStart(7)
    );
  }

  // Find best technique per metric
  const metrics = [
    { key: "recall_at_10" as const, label: "Recall@10" },
    { key: "mrr" as const, label: "MRR" },
    { key: "ndcg_at_10" as const, label: "NDCG@10" },
    { key: "hit_rate" as const, label: "Hit Rate" },
  ];

  console.log("\nBest per metric:");
  for (const metric of metrics) {
    const best = results.reduce((a, b) =>
      a.aggregate[metric.key] > b.aggregate[metric.key] ? a : b
    );
    console.log(
      `  ${metric.label.padEnd(12)} ${best.technique_id} (${(best.aggregate[metric.key] * 100).toFixed(1)}%)`
    );
  }
}

main().catch((e) => {
  console.error("Benchmark failed:", e);
  process.exit(1);
});
