/**
 * Analyze Errors from Precision Evaluation
 *
 * Reads the evaluation report and categorizes failures into actionable
 * error types with recommendations for targeted training data.
 *
 * Error Categories:
 * 1. Similar vocab, wrong topic - Same Arabic root word, different context
 * 2. Cross-reference confusion - Hadith quoting Quran or vice versa
 * 3. Short query fails - Very short queries that miss relevant results
 * 4. MSA vs Classical - Modern Standard Arabic vs Classical Quranic Arabic
 * 5. Transliteration issues - English transliteration not matching Arabic
 * 6. Adjacent passage confusion - Retrieving nearby ayahs/hadiths instead
 * 7. Source confusion - Quran query returning hadith or vice versa
 *
 * Output:
 * - Categorized error analysis
 * - Targeted training recommendations
 * - JSONL file with suggested training pairs
 *
 * Usage:
 *   bun run training/scripts/analyze-errors.ts [options]
 *
 * Options:
 *   --report=<file>     Evaluation report JSON (default: evaluation_report.json)
 *   --output=<file>     Output analysis JSON (default: error_analysis.json)
 *   --training=<file>   Output targeted training pairs (default: targeted_training.jsonl)
 *   --verbose           Show detailed analysis
 */

import * as fs from "fs";
import * as path from "path";

// Types
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

interface EvaluationReport {
  model: string;
  k: number;
  timestamp: string;
  total_queries: number;
  overall_metrics: Record<string, number>;
  by_category: Record<string, Record<string, number>>;
  by_difficulty: Record<string, Record<string, number>>;
  failed_queries: QueryResult[];
  all_results: QueryResult[];
}

interface ErrorCategory {
  name: string;
  description: string;
  queries: QueryResult[];
  training_recommendations: TrainingRecommendation[];
}

interface TrainingRecommendation {
  query: string;
  positive_id: string;
  negative_ids: string[];
  rationale: string;
}

interface TrainingPair {
  query: string;
  pos: string[];
  neg: string[];
  source: string;
  error_category: string;
}

interface ErrorAnalysis {
  timestamp: string;
  model: string;
  total_failures: number;
  categories: ErrorCategory[];
  summary: {
    category: string;
    count: number;
    percentage: number;
  }[];
  training_pairs_generated: number;
}

// Configuration
const DATA_DIR = path.join(__dirname, "../data");

// Arabic text analysis helpers
function isArabic(text: string): boolean {
  return /[\u0600-\u06FF]/.test(text);
}

function isShortQuery(query: string): boolean {
  const words = query.trim().split(/\s+/);
  return words.length <= 2 || query.length <= 15;
}

function isTransliteration(query: string): boolean {
  // Common transliteration patterns
  const patterns = [
    /^surah/i, /^ayat/i, /^hadith/i, /^bukhari/i, /^muslim/i,
    /kursi/i, /fatiha/i, /ikhlas/i, /kahf/i, /yasin/i,
    /salah/i, /zakah/i, /sabr/i, /tawbah/i, /taqwa/i,
    /niyyah/i, /ihsan/i, /iman/i
  ];
  return patterns.some((p) => p.test(query));
}

function hasSourceSpecificKeyword(query: string): { quran: boolean; hadith: boolean } {
  const quranKeywords = /quran|surah|ayah|verse|quranic|آية|سورة/i;
  const hadithKeywords = /hadith|narrat|prophet|صلى الله عليه|حديث|النبي/i;
  return {
    quran: quranKeywords.test(query),
    hadith: hadithKeywords.test(query),
  };
}

function isAdjacentPassage(targetId: string, retrievedId: string): boolean {
  // Check if IDs are adjacent (e.g., quran_2_255 vs quran_2_254)
  const targetMatch = targetId.match(/^(quran|hadith)_(\d+)_(\d+)$/);
  const retrievedMatch = retrievedId.match(/^(quran|hadith)_(\d+)_(\d+)$/);

  if (!targetMatch || !retrievedMatch) return false;
  if (targetMatch[1] !== retrievedMatch[1]) return false; // Different type
  if (targetMatch[2] !== retrievedMatch[2]) return false; // Different surah/book

  const targetNum = parseInt(targetMatch[3]);
  const retrievedNum = parseInt(retrievedMatch[3]);
  return Math.abs(targetNum - retrievedNum) <= 2;
}

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
    report: (options["report"] as string) || path.join(DATA_DIR, "evaluation_report.json"),
    output: (options["output"] as string) || path.join(DATA_DIR, "error_analysis.json"),
    training: (options["training"] as string) || path.join(DATA_DIR, "targeted_training.jsonl"),
    verbose: options["verbose"] === true,
  };
}

/**
 * Categorize a single failed query
 */
function categorizeError(result: QueryResult): string[] {
  const categories: string[] = [];

  // Check for short query
  if (isShortQuery(result.query)) {
    categories.push("short_query");
  }

  // Check for transliteration
  if (isTransliteration(result.query) && !isArabic(result.query)) {
    categories.push("transliteration");
  }

  // Check for source confusion
  const sourceKeywords = hasSourceSpecificKeyword(result.query);
  if (sourceKeywords.quran && result.retrieved.some((r) => r.id.startsWith("hadith_"))) {
    categories.push("source_confusion");
  }
  if (sourceKeywords.hadith && result.retrieved.some((r) => r.id.startsWith("quran_"))) {
    categories.push("source_confusion");
  }

  // Check for adjacent passage confusion
  const missedRelevant = result.missed_relevant || [];
  for (const missed of missedRelevant) {
    if (result.retrieved.some((r) => isAdjacentPassage(missed, r.id))) {
      categories.push("adjacent_passage");
      break;
    }
  }

  // Check for MSA vs Classical (Arabic queries missing classical text)
  if (isArabic(result.query) && missedRelevant.some((id) => id.startsWith("quran_"))) {
    categories.push("msa_classical");
  }

  // Check for similar vocab wrong topic (false positives present)
  if (result.false_positives.length > 0) {
    categories.push("similar_vocab_wrong_topic");
  }

  // Cross-reference confusion (looking for Quran but getting hadith that quotes it)
  if (result.category === "cross_collection") {
    categories.push("cross_reference");
  }

  // Default category if no specific pattern matched
  if (categories.length === 0) {
    categories.push("general_recall_failure");
  }

  return categories;
}

/**
 * Generate training recommendations for an error
 */
function generateTrainingRecommendation(
  result: QueryResult,
  errorCategory: string
): TrainingRecommendation | null {
  const missedRelevant = result.missed_relevant || [];
  if (missedRelevant.length === 0) return null;

  const positiveId = missedRelevant[0];
  let negativeIds: string[] = [];
  let rationale = "";

  switch (errorCategory) {
    case "short_query":
      // Add the incorrectly retrieved results as hard negatives
      negativeIds = result.retrieved.slice(0, 3).map((r) => r.id);
      rationale = "Short query needs keyword-specific training to disambiguate";
      break;

    case "transliteration":
      negativeIds = result.retrieved.slice(0, 3).map((r) => r.id);
      rationale = "Transliteration needs explicit mapping to Arabic text";
      break;

    case "source_confusion":
      // Negatives are from wrong source
      negativeIds = result.retrieved
        .filter((r) => {
          if (positiveId.startsWith("quran_")) return r.id.startsWith("hadith_");
          return r.id.startsWith("quran_");
        })
        .slice(0, 3)
        .map((r) => r.id);
      rationale = "Source-specific query needs to distinguish Quran from Hadith";
      break;

    case "adjacent_passage":
      negativeIds = result.retrieved
        .filter((r) => isAdjacentPassage(positiveId, r.id))
        .slice(0, 3)
        .map((r) => r.id);
      rationale = "Query needs to distinguish between adjacent passages";
      break;

    case "msa_classical":
      negativeIds = result.retrieved.slice(0, 3).map((r) => r.id);
      rationale = "MSA query needs bridging to classical Arabic text";
      break;

    case "similar_vocab_wrong_topic":
      negativeIds = result.false_positives.slice(0, 3);
      rationale = "Similar vocabulary but different meaning needs hard negative training";
      break;

    case "cross_reference":
      negativeIds = result.retrieved.slice(0, 3).map((r) => r.id);
      rationale = "Cross-reference query needs to handle citations properly";
      break;

    default:
      negativeIds = result.retrieved.slice(0, 3).map((r) => r.id);
      rationale = "General recall improvement through hard negative training";
  }

  return {
    query: result.query,
    positive_id: positiveId,
    negative_ids: negativeIds,
    rationale,
  };
}

/**
 * Analyze all errors and categorize them
 */
function analyzeErrors(report: EvaluationReport, verbose: boolean): ErrorAnalysis {
  const failedQueries = report.all_results.filter(
    (r) => !r.success || r.precision_at_k < 0.5
  );

  // Initialize category buckets
  const categoryBuckets: Record<string, QueryResult[]> = {
    short_query: [],
    transliteration: [],
    source_confusion: [],
    adjacent_passage: [],
    msa_classical: [],
    similar_vocab_wrong_topic: [],
    cross_reference: [],
    general_recall_failure: [],
  };

  // Categorize each failure
  for (const result of failedQueries) {
    const categories = categorizeError(result);
    for (const cat of categories) {
      if (categoryBuckets[cat]) {
        categoryBuckets[cat].push(result);
      }
    }
  }

  // Generate training recommendations
  const categories: ErrorCategory[] = [];
  const allTrainingPairs: TrainingPair[] = [];

  const categoryDescriptions: Record<string, string> = {
    short_query: "Very short queries (1-2 words) that lack context for disambiguation",
    transliteration: "English transliteration not matching Arabic text",
    source_confusion: "Quran queries returning hadith or vice versa",
    adjacent_passage: "Retrieving nearby ayahs/hadiths instead of the target",
    msa_classical: "Modern Standard Arabic queries missing Classical Quranic text",
    similar_vocab_wrong_topic: "Same Arabic vocabulary but different semantic meaning",
    cross_reference: "Confusion when hadith quotes Quran or discusses same topic",
    general_recall_failure: "General recall failures not fitting other categories",
  };

  for (const [catName, catResults] of Object.entries(categoryBuckets)) {
    if (catResults.length === 0) continue;

    const recommendations: TrainingRecommendation[] = [];
    for (const result of catResults.slice(0, 20)) { // Limit to top 20 per category
      const rec = generateTrainingRecommendation(result, catName);
      if (rec) {
        recommendations.push(rec);

        // Create training pair
        allTrainingPairs.push({
          query: rec.query,
          pos: [rec.positive_id], // ID only, actual text to be filled later
          neg: rec.negative_ids,
          source: "targeted_training",
          error_category: catName,
        });
      }
    }

    categories.push({
      name: catName,
      description: categoryDescriptions[catName] || catName,
      queries: catResults,
      training_recommendations: recommendations,
    });

    if (verbose) {
      console.log(`\n${catName} (${catResults.length} failures):`);
      console.log(`  ${categoryDescriptions[catName]}`);
      for (const result of catResults.slice(0, 3)) {
        console.log(`  - "${result.query.substring(0, 50)}..."`);
      }
    }
  }

  // Summary statistics
  const summary = Object.entries(categoryBuckets)
    .filter(([_, results]) => results.length > 0)
    .map(([category, results]) => ({
      category,
      count: results.length,
      percentage: (results.length / Math.max(failedQueries.length, 1)) * 100,
    }))
    .sort((a, b) => b.count - a.count);

  return {
    timestamp: new Date().toISOString(),
    model: report.model,
    total_failures: failedQueries.length,
    categories,
    summary,
    training_pairs_generated: allTrainingPairs.length,
  };
}

/**
 * Main function
 */
async function main() {
  const options = parseArgs();

  console.log("=".repeat(60));
  console.log("Error Analysis for Embedding Evaluation");
  console.log("=".repeat(60));
  console.log();

  // Load evaluation report
  if (!fs.existsSync(options.report)) {
    console.error(`Error: Evaluation report not found: ${options.report}`);
    console.error("Run evaluate-precision.ts first to generate the report.");
    process.exit(1);
  }

  console.log(`Loading evaluation report: ${options.report}`);
  const report: EvaluationReport = JSON.parse(fs.readFileSync(options.report, "utf-8"));
  console.log(`Model: ${report.model}`);
  console.log(`Total queries: ${report.total_queries}`);
  console.log();

  // Analyze errors
  console.log("Analyzing failures...");
  const analysis = analyzeErrors(report, options.verbose);

  // Print summary
  console.log("\n" + "=".repeat(60));
  console.log("ERROR ANALYSIS SUMMARY");
  console.log("=".repeat(60));
  console.log();

  console.log(`Total failures analyzed: ${analysis.total_failures}`);
  console.log();

  console.log("Failures by Category:");
  for (const item of analysis.summary) {
    const bar = "█".repeat(Math.ceil(item.percentage / 5));
    console.log(`  ${item.category.padEnd(25)} ${item.count.toString().padStart(3)} (${item.percentage.toFixed(1).padStart(5)}%) ${bar}`);
  }
  console.log();

  // Print top recommendations
  console.log("Training Recommendations:");
  for (const category of analysis.categories.slice(0, 5)) {
    if (category.training_recommendations.length > 0) {
      console.log(`\n  ${category.name}:`);
      console.log(`    ${category.description}`);
      console.log(`    Recommendations: ${category.training_recommendations.length} training pairs`);

      const sample = category.training_recommendations[0];
      if (sample) {
        console.log(`    Example:`);
        console.log(`      Query: "${sample.query.substring(0, 40)}..."`);
        console.log(`      Positive: ${sample.positive_id}`);
        console.log(`      Negatives: ${sample.negative_ids.slice(0, 2).join(", ")}`);
      }
    }
  }
  console.log();

  // Save analysis
  fs.writeFileSync(options.output, JSON.stringify(analysis, null, 2));
  console.log(`Analysis saved to: ${options.output}`);

  // Generate targeted training pairs
  const trainingPairs: TrainingPair[] = [];
  for (const category of analysis.categories) {
    for (const rec of category.training_recommendations) {
      trainingPairs.push({
        query: rec.query,
        pos: [rec.positive_id],
        neg: rec.negative_ids,
        source: "targeted_training",
        error_category: category.name,
      });
    }
  }

  if (trainingPairs.length > 0) {
    const trainingLines = trainingPairs.map((p) => JSON.stringify(p)).join("\n") + "\n";
    fs.writeFileSync(options.training, trainingLines);
    console.log(`Training pairs saved to: ${options.training} (${trainingPairs.length} pairs)`);
    console.log();
    console.log("Next steps:");
    console.log("  1. Review targeted_training.jsonl");
    console.log("  2. Replace IDs with actual text from database");
    console.log("  3. Add to training data and retrain");
  }

  // Action items
  console.log("\n" + "=".repeat(60));
  console.log("RECOMMENDED ACTIONS");
  console.log("=".repeat(60));

  const actions: string[] = [];

  if (analysis.summary.find((s) => s.category === "short_query" && s.count > 5)) {
    actions.push("Add keyword-specific training pairs for common short queries");
  }
  if (analysis.summary.find((s) => s.category === "transliteration" && s.count > 5)) {
    actions.push("Add explicit transliteration→Arabic mapping pairs");
  }
  if (analysis.summary.find((s) => s.category === "source_confusion" && s.count > 5)) {
    actions.push("Add source-specific training with cross-source hard negatives");
  }
  if (analysis.summary.find((s) => s.category === "msa_classical" && s.count > 5)) {
    actions.push("Add MSA paraphrase pairs that map to classical Arabic");
  }
  if (analysis.summary.find((s) => s.category === "adjacent_passage" && s.count > 5)) {
    actions.push("Add adjacent passages as hard negatives for discriminative training");
  }

  if (actions.length === 0) {
    actions.push("No major error patterns detected - model performing well");
  }

  for (let i = 0; i < actions.length; i++) {
    console.log(`${i + 1}. ${actions[i]}`);
  }
  console.log();
}

main().catch((e) => {
  console.error("Analysis failed:", e);
  process.exit(1);
});
