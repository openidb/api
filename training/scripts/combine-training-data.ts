/**
 * Combine and Validate Training Data
 *
 * Merges all JSONL training files with:
 * - Deduplication of queries
 * - Source tagging for analysis
 * - Data quality validation
 * - Shuffling for training
 * - Statistics export
 *
 * Input files:
 * - quran_pairs.jsonl (or quran_pairs_negatives.jsonl)
 * - hadith_pairs.jsonl (or hadith_pairs_negatives.jsonl)
 * - synthetic_queries.jsonl
 * - arabic_paraphrases.jsonl
 *
 * Output: combined_training.jsonl
 *
 * Usage:
 *   bun run training/scripts/combine-training-data.ts [options]
 *
 * Options:
 *   --output=<file>       Output file (default: combined_training.jsonl)
 *   --stats-only          Only show statistics, don't write output
 *   --no-shuffle          Don't shuffle the output
 *   --dedupe-threshold=<n> Similarity threshold for deduplication (default: 0.95)
 *   --min-query-len=<n>   Minimum query length (default: 10)
 *   --max-query-len=<n>   Maximum query length (default: 1000)
 *   --validate            Run detailed validation checks
 */

import * as fs from "fs";
import * as path from "path";

// Configuration
const DATA_DIR = path.join(__dirname, "../data");

interface TrainingPair {
  query: string;
  pos: string[];
  neg: string[];
  source?: string;
  query_type?: string;
  language?: string;
  pair_type?: string;
  passage_id?: string;
}

interface DatasetStats {
  totalPairs: number;
  uniqueQueries: number;
  uniquePositives: number;
  withNegatives: number;
  avgNegativesPerPair: number;
  bySource: Record<string, number>;
  byQueryType: Record<string, number>;
  byLanguage: Record<string, number>;
  queryLengthStats: {
    min: number;
    max: number;
    avg: number;
  };
  positiveLengthStats: {
    min: number;
    max: number;
    avg: number;
  };
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
    output: (options["output"] as string) || path.join(DATA_DIR, "combined_training.jsonl"),
    statsOnly: options["stats-only"] === true,
    noShuffle: options["no-shuffle"] === true,
    dedupeThreshold: parseFloat(options["dedupe-threshold"] as string) || 0.95,
    minQueryLen: parseInt(options["min-query-len"] as string) || 10,
    maxQueryLen: parseInt(options["max-query-len"] as string) || 1000,
    validate: options["validate"] === true,
  };
}

/**
 * Load training pairs from a JSONL file
 */
function loadJsonlFile(filePath: string): TrainingPair[] {
  if (!fs.existsSync(filePath)) {
    return [];
  }
  const content = fs.readFileSync(filePath, "utf-8");
  const lines = content.trim().split("\n").filter(Boolean);
  return lines.map((line) => {
    try {
      return JSON.parse(line) as TrainingPair;
    } catch {
      console.warn(`Warning: Invalid JSON line in ${filePath}`);
      return null;
    }
  }).filter((p): p is TrainingPair => p !== null);
}

/**
 * Normalize text for deduplication comparison
 */
function normalizeForComparison(text: string): string {
  return text
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ")
    .replace(/[^\w\s\u0600-\u06FF]/g, ""); // Keep alphanumeric and Arabic
}

/**
 * Calculate Jaccard similarity between two strings
 */
function jaccardSimilarity(str1: string, str2: string): number {
  const norm1 = normalizeForComparison(str1);
  const norm2 = normalizeForComparison(str2);

  const words1 = new Set(norm1.split(" ").filter(Boolean));
  const words2 = new Set(norm2.split(" ").filter(Boolean));

  if (words1.size === 0 || words2.size === 0) return 0;

  const intersection = Array.from(words1).filter((w) => words2.has(w)).length;
  const union = words1.size + words2.size - intersection;

  return intersection / union;
}

/**
 * Detect language of a query (simple heuristic)
 */
function detectLanguage(text: string): string {
  const arabicChars = (text.match(/[\u0600-\u06FF]/g) || []).length;
  const totalChars = text.replace(/\s/g, "").length;

  if (totalChars === 0) return "unknown";

  const arabicRatio = arabicChars / totalChars;

  if (arabicRatio > 0.5) return "ar";
  if (arabicRatio > 0.1) return "mixed";
  return "en";
}

/**
 * Validate a training pair
 */
function validatePair(
  pair: TrainingPair,
  minLen: number,
  maxLen: number
): { valid: boolean; issues: string[] } {
  const issues: string[] = [];

  // Query validation
  if (!pair.query || typeof pair.query !== "string") {
    issues.push("Missing or invalid query");
  } else {
    if (pair.query.length < minLen) {
      issues.push(`Query too short (${pair.query.length} < ${minLen})`);
    }
    if (pair.query.length > maxLen) {
      issues.push(`Query too long (${pair.query.length} > ${maxLen})`);
    }
  }

  // Positive validation
  if (!pair.pos || !Array.isArray(pair.pos) || pair.pos.length === 0) {
    issues.push("Missing or empty pos array");
  } else {
    for (const pos of pair.pos) {
      if (!pos || typeof pos !== "string" || pos.length < 10) {
        issues.push("Invalid positive text");
      }
    }
  }

  // Negative validation (optional)
  if (pair.neg && !Array.isArray(pair.neg)) {
    issues.push("Invalid neg array");
  }

  return {
    valid: issues.length === 0,
    issues,
  };
}

/**
 * Deduplicate pairs by query similarity
 */
function deduplicatePairs(
  pairs: TrainingPair[],
  threshold: number
): TrainingPair[] {
  console.log(`Deduplicating ${pairs.length} pairs (threshold: ${threshold})...`);

  const result: TrainingPair[] = [];
  const seenQueries: string[] = [];

  for (const pair of pairs) {
    const normalized = normalizeForComparison(pair.query);

    // Check for exact duplicates first (fast)
    if (seenQueries.includes(normalized)) {
      continue;
    }

    // Check for near-duplicates (slower, only if threshold < 1)
    if (threshold < 1.0) {
      let isDuplicate = false;
      for (const seen of seenQueries.slice(-1000)) { // Check last 1000 for efficiency
        if (jaccardSimilarity(normalized, seen) > threshold) {
          isDuplicate = true;
          break;
        }
      }
      if (isDuplicate) continue;
    }

    seenQueries.push(normalized);
    result.push(pair);
  }

  console.log(`  Removed ${pairs.length - result.length} duplicates`);
  return result;
}

/**
 * Shuffle array in place using Fisher-Yates
 */
function shuffleArray<T>(array: T[]): T[] {
  const result = [...array];
  for (let i = result.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [result[i], result[j]] = [result[j], result[i]];
  }
  return result;
}

/**
 * Calculate dataset statistics
 */
function calculateStats(pairs: TrainingPair[]): DatasetStats {
  const uniqueQueries = new Set(pairs.map((p) => normalizeForComparison(p.query)));
  const uniquePositives = new Set(pairs.flatMap((p) => p.pos.map(normalizeForComparison)));

  const withNegatives = pairs.filter((p) => p.neg && p.neg.length > 0).length;
  const totalNegatives = pairs.reduce((sum, p) => sum + (p.neg?.length || 0), 0);

  const bySource: Record<string, number> = {};
  const byQueryType: Record<string, number> = {};
  const byLanguage: Record<string, number> = {};

  let totalQueryLen = 0;
  let totalPosLen = 0;
  let minQueryLen = Infinity;
  let maxQueryLen = 0;
  let minPosLen = Infinity;
  let maxPosLen = 0;

  for (const pair of pairs) {
    // Source
    const source = pair.source || "original";
    bySource[source] = (bySource[source] || 0) + 1;

    // Query type
    const queryType = pair.query_type || pair.pair_type || "translation";
    byQueryType[queryType] = (byQueryType[queryType] || 0) + 1;

    // Language
    const language = pair.language || detectLanguage(pair.query);
    byLanguage[language] = (byLanguage[language] || 0) + 1;

    // Length stats
    const queryLen = pair.query.length;
    totalQueryLen += queryLen;
    minQueryLen = Math.min(minQueryLen, queryLen);
    maxQueryLen = Math.max(maxQueryLen, queryLen);

    for (const pos of pair.pos) {
      const posLen = pos.length;
      totalPosLen += posLen;
      minPosLen = Math.min(minPosLen, posLen);
      maxPosLen = Math.max(maxPosLen, posLen);
    }
  }

  return {
    totalPairs: pairs.length,
    uniqueQueries: uniqueQueries.size,
    uniquePositives: uniquePositives.size,
    withNegatives,
    avgNegativesPerPair: withNegatives > 0 ? totalNegatives / withNegatives : 0,
    bySource,
    byQueryType,
    byLanguage,
    queryLengthStats: {
      min: minQueryLen === Infinity ? 0 : minQueryLen,
      max: maxQueryLen,
      avg: pairs.length > 0 ? totalQueryLen / pairs.length : 0,
    },
    positiveLengthStats: {
      min: minPosLen === Infinity ? 0 : minPosLen,
      max: maxPosLen,
      avg: pairs.length > 0 ? totalPosLen / pairs.reduce((sum, p) => sum + p.pos.length, 0) : 0,
    },
  };
}

/**
 * Print statistics report
 */
function printStats(stats: DatasetStats): void {
  console.log("\n" + "=".repeat(60));
  console.log("DATASET STATISTICS");
  console.log("=".repeat(60));

  console.log("\nOverview:");
  console.log(`  Total pairs: ${stats.totalPairs.toLocaleString()}`);
  console.log(`  Unique queries: ${stats.uniqueQueries.toLocaleString()}`);
  console.log(`  Unique positives: ${stats.uniquePositives.toLocaleString()}`);
  console.log(`  Pairs with negatives: ${stats.withNegatives.toLocaleString()} (${((stats.withNegatives / stats.totalPairs) * 100).toFixed(1)}%)`);
  console.log(`  Avg negatives per pair: ${stats.avgNegativesPerPair.toFixed(2)}`);

  console.log("\nBy Source:");
  for (const [source, count] of Object.entries(stats.bySource).sort((a, b) => b[1] - a[1])) {
    const pct = ((count / stats.totalPairs) * 100).toFixed(1);
    console.log(`  ${source}: ${count.toLocaleString()} (${pct}%)`);
  }

  console.log("\nBy Query Type:");
  for (const [type, count] of Object.entries(stats.byQueryType).sort((a, b) => b[1] - a[1])) {
    const pct = ((count / stats.totalPairs) * 100).toFixed(1);
    console.log(`  ${type}: ${count.toLocaleString()} (${pct}%)`);
  }

  console.log("\nBy Language:");
  for (const [lang, count] of Object.entries(stats.byLanguage).sort((a, b) => b[1] - a[1])) {
    const pct = ((count / stats.totalPairs) * 100).toFixed(1);
    console.log(`  ${lang}: ${count.toLocaleString()} (${pct}%)`);
  }

  console.log("\nQuery Length:");
  console.log(`  Min: ${stats.queryLengthStats.min} chars`);
  console.log(`  Max: ${stats.queryLengthStats.max} chars`);
  console.log(`  Avg: ${stats.queryLengthStats.avg.toFixed(0)} chars`);

  console.log("\nPositive Length:");
  console.log(`  Min: ${stats.positiveLengthStats.min} chars`);
  console.log(`  Max: ${stats.positiveLengthStats.max} chars`);
  console.log(`  Avg: ${stats.positiveLengthStats.avg.toFixed(0)} chars`);
}

/**
 * Write training pairs to JSONL file
 */
function writeJsonl(pairs: TrainingPair[], outputPath: string): void {
  const lines = pairs.map((p) => JSON.stringify(p));
  fs.writeFileSync(outputPath, lines.join("\n") + "\n");
}

async function main() {
  const options = parseArgs();

  console.log("=".repeat(60));
  console.log("Training Data Combination and Validation");
  console.log("=".repeat(60));
  console.log();

  // Define input files in priority order (negatives files preferred)
  const inputFiles = [
    // Original pairs (with or without hard negatives)
    { path: path.join(DATA_DIR, "quran_pairs_negatives.jsonl"), source: "quran_original", fallback: path.join(DATA_DIR, "quran_pairs.jsonl") },
    { path: path.join(DATA_DIR, "hadith_pairs_negatives.jsonl"), source: "hadith_original", fallback: path.join(DATA_DIR, "hadith_pairs.jsonl") },
    // Synthetic queries
    { path: path.join(DATA_DIR, "synthetic_queries.jsonl"), source: "synthetic" },
    // Arabic paraphrases
    { path: path.join(DATA_DIR, "arabic_paraphrases.jsonl"), source: "paraphrase" },
  ];

  // Load all files
  console.log("Loading training data files...");
  let allPairs: TrainingPair[] = [];

  for (const file of inputFiles) {
    let filePath = file.path;

    // Try fallback if primary doesn't exist
    if (!fs.existsSync(filePath) && file.fallback) {
      filePath = file.fallback;
    }

    if (!fs.existsSync(filePath)) {
      console.log(`  [SKIP] ${path.basename(file.path)} - not found`);
      continue;
    }

    const pairs = loadJsonlFile(filePath);
    // Tag source if not already tagged
    pairs.forEach((p) => {
      if (!p.source) p.source = file.source;
    });
    allPairs.push(...pairs);
    console.log(`  [OK] ${path.basename(filePath)}: ${pairs.length.toLocaleString()} pairs`);
  }

  console.log(`\nTotal loaded: ${allPairs.length.toLocaleString()} pairs`);

  if (allPairs.length === 0) {
    console.error("No training data found. Please generate training pairs first.");
    process.exit(1);
  }

  // Validation
  if (options.validate) {
    console.log("\nValidating pairs...");
    let invalidCount = 0;
    const issueCount: Record<string, number> = {};

    for (const pair of allPairs) {
      const { valid, issues } = validatePair(pair, options.minQueryLen, options.maxQueryLen);
      if (!valid) {
        invalidCount++;
        for (const issue of issues) {
          issueCount[issue] = (issueCount[issue] || 0) + 1;
        }
      }
    }

    if (invalidCount > 0) {
      console.log(`  Found ${invalidCount} invalid pairs:`);
      for (const [issue, count] of Object.entries(issueCount).sort((a, b) => b[1] - a[1])) {
        console.log(`    - ${issue}: ${count}`);
      }
    } else {
      console.log("  All pairs valid!");
    }

    // Filter invalid pairs
    allPairs = allPairs.filter((p) => validatePair(p, options.minQueryLen, options.maxQueryLen).valid);
    console.log(`  Kept ${allPairs.length.toLocaleString()} valid pairs`);
  }

  // Deduplication
  allPairs = deduplicatePairs(allPairs, options.dedupeThreshold);

  // Shuffle
  if (!options.noShuffle) {
    console.log("Shuffling pairs...");
    allPairs = shuffleArray(allPairs);
  }

  // Calculate and print statistics
  const stats = calculateStats(allPairs);
  printStats(stats);

  // Write output
  if (!options.statsOnly) {
    console.log(`\nWriting ${allPairs.length.toLocaleString()} pairs to ${options.output}...`);
    writeJsonl(allPairs, options.output);

    // Also write stats to JSON
    const statsPath = options.output.replace(".jsonl", "_stats.json");
    fs.writeFileSync(statsPath, JSON.stringify(stats, null, 2));
    console.log(`Statistics written to ${statsPath}`);
  }

  console.log("\nDone!");
}

main().catch((e) => {
  console.error("Failed:", e);
  process.exit(1);
});
