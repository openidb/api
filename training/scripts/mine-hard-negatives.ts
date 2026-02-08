/**
 * Mine Hard Negatives for Training Data (Precision-Optimized)
 *
 * For each (query, positive) pair in existing training data:
 * 1. Embed the query using Gemini embeddings
 * 2. Search Qdrant for top-N similar passages
 * 3. Apply similarity threshold filtering:
 *    - 0.90-1.00: EXCLUDE (likely duplicate/relevant)
 *    - 0.85-0.90: Flag for MANUAL REVIEW
 *    - 0.65-0.85: IDEAL HARD NEGATIVES ← Target zone
 *    - 0.50-0.65: SEMI-HARD (useful for diversity)
 *    - < 0.50: TOO EASY (skip)
 * 4. Filter out actual positives and same-hadith-different-collection pairs
 * 5. Output negatives with confidence scores
 *
 * Hard negatives are semantically similar but incorrect passages,
 * which help the model learn fine-grained distinctions.
 *
 * Output format (JSONL):
 * {"query": "...", "pos": ["correct"], "neg": ["similar but wrong 1", ...], "neg_scores": [0.78, ...], "needs_review": false}
 *
 * Usage:
 *   bun run training/scripts/mine-hard-negatives.ts [options]
 *
 * Options:
 *   --input=<file>        Input JSONL file (default: all pairs)
 *   --output=<file>       Output file (default: auto-named with _negatives suffix)
 *   --source=<type>       Source type: quran | hadith | all (default: all)
 *   --negatives=<n>       Number of hard negatives per query (default: 3)
 *   --search-limit=<n>    Number of candidates to search (default: 50)
 *   --min-sim=<f>         Minimum similarity threshold (default: 0.50)
 *   --max-sim=<f>         Maximum similarity threshold (default: 0.85)
 *   --review-threshold=<f> Flag for review above this (default: 0.80)
 *   --dry-run             Show what would be done without writing
 *   --export-review       Export borderline cases to separate file for review
 */

import "dotenv/config";
import * as fs from "fs";
import * as path from "path";
import { qdrant, QDRANT_QURAN_ENRICHED_COLLECTION, QDRANT_HADITH_COLLECTION } from "../../lib/qdrant";
import { generateEmbedding } from "../../lib/embeddings";

// Configuration
const DATA_DIR = path.join(__dirname, "../data");
const DEFAULT_NEGATIVES = 3;
const DEFAULT_SEARCH_LIMIT = 50;
const BATCH_SIZE = 100; // Process queries in batches for progress reporting
const CONCURRENCY = 50; // Number of parallel requests
const PROGRESS_INTERVAL = 500; // Log progress every N pairs

// Similarity thresholds (precision-optimized)
const DEFAULT_MIN_SIMILARITY = 0.50;  // Too easy below this
const DEFAULT_MAX_SIMILARITY = 0.85;  // Likely relevant above this
const DEFAULT_REVIEW_THRESHOLD = 0.80; // Flag for manual review above this

interface TrainingPair {
  query: string;
  pos: string[];
  neg: string[];
  neg_scores?: number[];
  needs_review?: boolean;
  review_candidates?: { text: string; score: number }[];
  source?: string;
}

interface QdrantPayload {
  text?: string;
  textPlain?: string;
  textArabic?: string;
  collectionSlug?: string;
  collection_slug?: string;
  hadithNumber?: string;
  hadith_number?: string;
  [key: string]: unknown;
}

interface NegativeCandidate {
  text: string;
  score: number;
  collectionSlug?: string;
  hadithNumber?: string;
}

interface MiningStats {
  total: number;
  withNegatives: number;
  needsReview: number;
  avgNegatives: number;
  avgScore: number;
  excludedTooSimilar: number;
  excludedTooEasy: number;
  excludedSameHadith: number;
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
    input: options["input"] as string | undefined,
    output: options["output"] as string | undefined,
    source: (options["source"] as string) || "all",
    negatives: parseInt(options["negatives"] as string) || DEFAULT_NEGATIVES,
    searchLimit: parseInt(options["search-limit"] as string) || DEFAULT_SEARCH_LIMIT,
    minSim: parseFloat(options["min-sim"] as string) || DEFAULT_MIN_SIMILARITY,
    maxSim: parseFloat(options["max-sim"] as string) || DEFAULT_MAX_SIMILARITY,
    reviewThreshold: parseFloat(options["review-threshold"] as string) || DEFAULT_REVIEW_THRESHOLD,
    dryRun: options["dry-run"] === true,
    exportReview: options["export-review"] === true,
  };
}

/**
 * Load training pairs from a JSONL file
 */
function loadTrainingPairs(filePath: string): TrainingPair[] {
  const content = fs.readFileSync(filePath, "utf-8");
  const lines = content.trim().split("\n").filter(Boolean);
  return lines.map((line) => JSON.parse(line) as TrainingPair);
}

/**
 * Normalize text for comparison (remove extra whitespace, normalize Arabic)
 */
function normalizeText(text: string): string {
  return text
    .trim()
    .replace(/\s+/g, " ")
    .replace(/[\u064B-\u0652]/g, "") // Remove Arabic diacritics for comparison
    .toLowerCase();
}

/**
 * Check if two texts are semantically the same (used to filter out actual positives)
 */
function isSamePassage(text1: string, text2: string): boolean {
  const norm1 = normalizeText(text1);
  const norm2 = normalizeText(text2);

  // Exact match after normalization
  if (norm1 === norm2) return true;

  // High overlap (Jaccard similarity)
  const words1 = new Set(norm1.split(" "));
  const words2 = new Set(norm2.split(" "));
  const intersection = Array.from(words1).filter((w) => words2.has(w)).length;
  const union = words1.size + words2.size - intersection;
  const jaccard = intersection / union;

  // If more than 80% overlap, consider it the same
  return jaccard > 0.8;
}

/**
 * Check if two hadiths are the same hadith from different collections
 * (e.g., same hadith in Bukhari and Muslim - should NOT be hard negative)
 */
function isSameHadithDifferentCollection(
  candidate: NegativeCandidate,
  pair: TrainingPair
): boolean {
  // Only check for hadith pairs
  if (pair.source !== "hadith") return false;
  if (!candidate.collectionSlug || !candidate.hadithNumber) return false;

  // Check if the positive text contains similar content
  // This is a heuristic - same hadith in different collections often has very high text overlap
  for (const pos of pair.pos) {
    const posNorm = normalizeText(pos);
    const candNorm = normalizeText(candidate.text);

    // Very high Jaccard similarity suggests same hadith
    const posWords = new Set(posNorm.split(" ").filter((w) => w.length > 2));
    const candWords = new Set(candNorm.split(" ").filter((w) => w.length > 2));

    const intersection = Array.from(posWords).filter((w) => candWords.has(w)).length;
    const minSize = Math.min(posWords.size, candWords.size);

    // If 70%+ of the smaller set's words appear in both, likely same hadith
    if (minSize > 0 && intersection / minSize > 0.7) {
      return true;
    }
  }

  return false;
}

/**
 * Extract text and metadata from Qdrant search result payload
 */
function extractFromPayload(payload: QdrantPayload): NegativeCandidate | null {
  const text = (
    payload.textPlain ||
    payload.textArabic ||
    payload.text ||
    null
  ) as string | null;

  if (!text) return null;

  return {
    text,
    score: 0, // Will be set by caller
    collectionSlug: (payload.collectionSlug || payload.collection_slug) as string | undefined,
    hadithNumber: (payload.hadithNumber || payload.hadith_number) as string | undefined,
  };
}

/**
 * Process a single pair to find hard negatives
 */
async function processSinglePair(
  pair: TrainingPair,
  collection: string,
  numNegatives: number,
  searchLimit: number,
  minSim: number,
  maxSim: number,
  reviewThreshold: number
): Promise<{ pair: TrainingPair; excluded: { tooSimilar: number; tooEasy: number; sameHadith: number }; needsReview: boolean }> {
  const excluded = { tooSimilar: 0, tooEasy: 0, sameHadith: 0 };

  try {
    // Generate embedding for the query using Gemini
    const queryEmbedding = await generateEmbedding(pair.query, "gemini");

    // Search for similar passages
    const searchResults = await qdrant.search(collection, {
      vector: queryEmbedding,
      limit: searchLimit,
      with_payload: true,
      score_threshold: minSim,
    });

    // Filter and categorize candidates
    const negatives: string[] = [];
    const negScores: number[] = [];
    const reviewCandidates: { text: string; score: number }[] = [];
    let needsReview = false;

    for (const result of searchResults) {
      if (negatives.length >= numNegatives) break;

      const candidate = extractFromPayload(result.payload as QdrantPayload);
      if (!candidate) continue;

      candidate.score = result.score;

      if (candidate.score > 0.90) {
        excluded.tooSimilar++;
        continue;
      }

      if (candidate.score < minSim) {
        excluded.tooEasy++;
        continue;
      }

      const isPositive = pair.pos.some((pos) => isSamePassage(pos, candidate.text));
      if (isPositive) continue;

      if (isSameHadithDifferentCollection(candidate, pair)) {
        excluded.sameHadith++;
        continue;
      }

      const isDuplicate = negatives.some((neg) => isSamePassage(neg, candidate.text));
      if (isDuplicate) continue;

      if (candidate.score > reviewThreshold && candidate.score <= maxSim) {
        reviewCandidates.push({ text: candidate.text, score: candidate.score });
        needsReview = true;
      }

      if (candidate.score <= maxSim) {
        negatives.push(candidate.text);
        negScores.push(candidate.score);
      }
    }

    const resultPair: TrainingPair = {
      ...pair,
      neg: negatives,
      neg_scores: negScores,
    };

    if (needsReview) {
      resultPair.needs_review = true;
      resultPair.review_candidates = reviewCandidates;
    }

    return { pair: resultPair, excluded, needsReview };
  } catch (error) {
    // Keep the original pair without negatives on error
    return { pair: { ...pair, neg: [], neg_scores: [] }, excluded, needsReview: false };
  }
}

/**
 * Mine hard negatives for a batch of training pairs IN PARALLEL
 */
async function mineHardNegativesForPairs(
  pairs: TrainingPair[],
  collection: string,
  numNegatives: number,
  searchLimit: number,
  minSim: number,
  maxSim: number,
  reviewThreshold: number,
  stats: MiningStats
): Promise<TrainingPair[]> {
  // Process all pairs in parallel with controlled concurrency
  const chunks: TrainingPair[][] = [];
  for (let i = 0; i < pairs.length; i += CONCURRENCY) {
    chunks.push(pairs.slice(i, i + CONCURRENCY));
  }

  const results: TrainingPair[] = [];

  for (const chunk of chunks) {
    const chunkResults = await Promise.all(
      chunk.map((pair) =>
        processSinglePair(pair, collection, numNegatives, searchLimit, minSim, maxSim, reviewThreshold)
      )
    );

    for (const result of chunkResults) {
      results.push(result.pair);
      stats.excludedTooSimilar += result.excluded.tooSimilar;
      stats.excludedTooEasy += result.excluded.tooEasy;
      stats.excludedSameHadith += result.excluded.sameHadith;
      if (result.needsReview) stats.needsReview++;
      if (result.pair.neg.length > 0) stats.withNegatives++;
    }
  }

  return results;
}

/**
 * Determine which Qdrant collection to use based on source type
 * Uses existing Gemini-embedded collections for mining
 */
function getCollection(source: string): string {
  switch (source) {
    case "quran":
      return QDRANT_QURAN_ENRICHED_COLLECTION;
    case "hadith":
      return QDRANT_HADITH_COLLECTION;
    default:
      return QDRANT_HADITH_COLLECTION;
  }
}

/**
 * Write training pairs to JSONL file
 */
function writeTrainingPairs(pairs: TrainingPair[], outputPath: string): void {
  const lines = pairs.map((p) => JSON.stringify(p));
  fs.writeFileSync(outputPath, lines.join("\n") + "\n");
}

/**
 * Export borderline cases for manual review
 */
function exportReviewCases(pairs: TrainingPair[], outputPath: string): void {
  const reviewPairs = pairs.filter((p) => p.needs_review && p.review_candidates);

  const reviewData = reviewPairs.map((p) => ({
    query: p.query,
    positive: p.pos[0],
    review_candidates: p.review_candidates,
    source: p.source,
    decision: null, // To be filled by reviewer: "relevant" | "not_relevant"
  }));

  fs.writeFileSync(outputPath, JSON.stringify(reviewData, null, 2));
}

async function main() {
  const options = parseArgs();

  console.log("=".repeat(60));
  console.log("Hard Negative Mining for BGE-M3 Training (Precision-Optimized)");
  console.log("=".repeat(60));
  console.log();

  // Check OpenRouter API key for Gemini embeddings
  if (!process.env.OPENROUTER_API_KEY) {
    console.error("ERROR: OPENROUTER_API_KEY not set.");
    console.error("Hard negative mining uses Gemini embeddings to search existing collections.");
    process.exit(1);
  }
  console.log("Using Gemini embeddings for hard negative mining.\n");

  // Determine input files
  let inputFiles: string[] = [];
  if (options.input) {
    inputFiles = [options.input];
  } else if (options.source === "quran") {
    inputFiles = [path.join(DATA_DIR, "quran_pairs.jsonl")];
  } else if (options.source === "hadith") {
    inputFiles = [path.join(DATA_DIR, "hadith_pairs.jsonl")];
  } else {
    inputFiles = [
      path.join(DATA_DIR, "quran_pairs.jsonl"),
      path.join(DATA_DIR, "hadith_pairs.jsonl"),
    ];
  }

  // Load all training pairs
  console.log("Loading training pairs...");
  const allPairs: TrainingPair[] = [];
  for (const file of inputFiles) {
    if (!fs.existsSync(file)) {
      console.warn(`Warning: File not found: ${file}`);
      continue;
    }
    const pairs = loadTrainingPairs(file);
    const source = path.basename(file).includes("quran") ? "quran" : "hadith";
    pairs.forEach((p) => (p.source = source));
    allPairs.push(...pairs);
    console.log(`  Loaded ${pairs.length} pairs from ${path.basename(file)}`);
  }
  console.log(`Total pairs to process: ${allPairs.length}\n`);

  if (allPairs.length === 0) {
    console.error("No training pairs found. Please run the pair generation scripts first.");
    process.exit(1);
  }

  // Configuration summary
  console.log("Configuration:");
  console.log(`  Hard negatives per query: ${options.negatives}`);
  console.log(`  Search candidates: ${options.searchLimit}`);
  console.log(`  Similarity thresholds:`);
  console.log(`    Min (too easy below): ${options.minSim}`);
  console.log(`    Max (excluded above): ${options.maxSim}`);
  console.log(`    Review threshold: ${options.reviewThreshold}`);
  console.log(`  Dry run: ${options.dryRun}`);
  console.log(`  Export review: ${options.exportReview}`);
  console.log();

  console.log("Similarity zones:");
  console.log("  0.90 - 1.00: EXCLUDE (likely duplicate/relevant)");
  console.log(`  ${options.reviewThreshold.toFixed(2)} - 0.90: MANUAL REVIEW REQUIRED`);
  console.log(`  ${options.minSim.toFixed(2)} - ${options.reviewThreshold.toFixed(2)}: IDEAL HARD NEGATIVES`);
  console.log(`  < ${options.minSim.toFixed(2)}: TOO EASY (in-batch sufficient)`);
  console.log();

  if (options.dryRun) {
    console.log("DRY RUN - No files will be written.");
    console.log(`Would process ${allPairs.length} pairs.`);
    console.log(`Expected output: ${allPairs.length} pairs with up to ${options.negatives} negatives each.`);
    return;
  }

  // Initialize stats
  const stats: MiningStats = {
    total: 0,
    withNegatives: 0,
    needsReview: 0,
    avgNegatives: 0,
    avgScore: 0,
    excludedTooSimilar: 0,
    excludedTooEasy: 0,
    excludedSameHadith: 0,
  };

  // Process pairs by source
  const quranPairs = allPairs.filter((p) => p.source === "quran");
  const hadithPairs = allPairs.filter((p) => p.source === "hadith");

  const processedPairs: TrainingPair[] = [];
  let totalProcessed = 0;

  // Process Quran pairs
  if (quranPairs.length > 0) {
    console.log(`Processing ${quranPairs.length} Quran pairs (concurrency: ${CONCURRENCY})...`);
    const collection = QDRANT_QURAN_ENRICHED_COLLECTION;

    for (let i = 0; i < quranPairs.length; i += BATCH_SIZE) {
      const batch = quranPairs.slice(i, i + BATCH_SIZE);
      const results = await mineHardNegativesForPairs(
        batch,
        collection,
        options.negatives,
        options.searchLimit,
        options.minSim,
        options.maxSim,
        options.reviewThreshold,
        stats
      );
      processedPairs.push(...results);
      totalProcessed += batch.length;

      if (totalProcessed % PROGRESS_INTERVAL === 0 || i + BATCH_SIZE >= quranPairs.length) {
        const pct = ((totalProcessed / allPairs.length) * 100).toFixed(1);
        console.log(`  Processed ${totalProcessed}/${allPairs.length} pairs (${pct}%)...`);
      }
    }
  }

  // Process Hadith pairs
  if (hadithPairs.length > 0) {
    console.log(`Processing ${hadithPairs.length} Hadith pairs (concurrency: ${CONCURRENCY})...`);
    const collection = QDRANT_HADITH_COLLECTION;

    for (let i = 0; i < hadithPairs.length; i += BATCH_SIZE) {
      const batch = hadithPairs.slice(i, i + BATCH_SIZE);
      const results = await mineHardNegativesForPairs(
        batch,
        collection,
        options.negatives,
        options.searchLimit,
        options.minSim,
        options.maxSim,
        options.reviewThreshold,
        stats
      );
      processedPairs.push(...results);
      totalProcessed += batch.length;

      if (totalProcessed % PROGRESS_INTERVAL === 0 || i + BATCH_SIZE >= hadithPairs.length) {
        const pct = ((totalProcessed / allPairs.length) * 100).toFixed(1);
        console.log(`  Processed ${totalProcessed}/${allPairs.length} pairs (${pct}%)...`);
      }
    }
  }

  // Calculate final stats
  stats.total = processedPairs.length;
  const totalNegatives = processedPairs.reduce((sum, p) => sum + p.neg.length, 0);
  stats.avgNegatives = totalNegatives / stats.total;

  const allScores = processedPairs.flatMap((p) => p.neg_scores || []);
  stats.avgScore = allScores.length > 0
    ? allScores.reduce((a, b) => a + b, 0) / allScores.length
    : 0;

  console.log();
  console.log("=".repeat(60));
  console.log("MINING COMPLETE - STATISTICS");
  console.log("=".repeat(60));
  console.log();
  console.log(`Total pairs processed: ${stats.total}`);
  console.log(`Pairs with hard negatives: ${stats.withNegatives} (${((stats.withNegatives / stats.total) * 100).toFixed(1)}%)`);
  console.log(`Pairs needing manual review: ${stats.needsReview}`);
  console.log(`Average negatives per pair: ${stats.avgNegatives.toFixed(2)}`);
  console.log(`Average negative score: ${stats.avgScore.toFixed(3)}`);
  console.log();
  console.log("Exclusions:");
  console.log(`  Too similar (>0.90): ${stats.excludedTooSimilar}`);
  console.log(`  Too easy (<${options.minSim}): ${stats.excludedTooEasy}`);
  console.log(`  Same hadith different collection: ${stats.excludedSameHadith}`);
  console.log();

  // Write output files
  const outputDir = DATA_DIR;
  if (options.output) {
    writeTrainingPairs(processedPairs, options.output);
    console.log(`Written all pairs to: ${options.output}`);
  } else {
    // Write separate files for quran and hadith
    const quranWithNegs = processedPairs.filter((p) => p.source === "quran");
    const hadithWithNegs = processedPairs.filter((p) => p.source === "hadith");

    if (quranWithNegs.length > 0) {
      const quranOutput = path.join(outputDir, "quran_pairs_negatives.jsonl");
      writeTrainingPairs(quranWithNegs, quranOutput);
      console.log(`Written ${quranWithNegs.length} Quran pairs to: ${quranOutput}`);
    }

    if (hadithWithNegs.length > 0) {
      const hadithOutput = path.join(outputDir, "hadith_pairs_negatives.jsonl");
      writeTrainingPairs(hadithWithNegs, hadithOutput);
      console.log(`Written ${hadithWithNegs.length} Hadith pairs to: ${hadithOutput}`);
    }
  }

  // Export review cases if requested
  if (options.exportReview && stats.needsReview > 0) {
    const reviewOutput = path.join(outputDir, "borderline_review.json");
    exportReviewCases(processedPairs, reviewOutput);
    console.log(`\nExported ${stats.needsReview} borderline cases to: ${reviewOutput}`);
    console.log("Please review these cases and mark as 'relevant' or 'not_relevant'.");
  }

  // Show sample output
  console.log();
  console.log("Sample output:");
  console.log("-".repeat(60));
  const sample = processedPairs.find((p) => p.neg.length >= 2 && p.neg_scores);
  if (sample) {
    console.log(`Query: ${sample.query.substring(0, 100)}...`);
    console.log(`Positive: ${sample.pos[0].substring(0, 80)}...`);
    console.log(`Negatives (${sample.neg.length}):`);
    sample.neg.slice(0, 2).forEach((neg, i) => {
      const score = sample.neg_scores?.[i] || 0;
      console.log(`  ${i + 1}. [${score.toFixed(3)}] ${neg.substring(0, 60)}...`);
    });
    if (sample.needs_review) {
      console.log(`  ⚠️  Needs manual review (borderline cases detected)`);
    }
  }

  // Score distribution summary
  console.log();
  console.log("Score distribution:");
  const scoreRanges = [
    { min: 0.80, max: 0.85, label: "0.80-0.85 (borderline)" },
    { min: 0.70, max: 0.80, label: "0.70-0.80 (ideal)" },
    { min: 0.60, max: 0.70, label: "0.60-0.70 (good)" },
    { min: 0.50, max: 0.60, label: "0.50-0.60 (semi-hard)" },
  ];

  for (const range of scoreRanges) {
    const count = allScores.filter((s) => s >= range.min && s < range.max).length;
    const pct = allScores.length > 0 ? ((count / allScores.length) * 100).toFixed(1) : "0.0";
    const bar = "█".repeat(Math.ceil(parseFloat(pct) / 5));
    console.log(`  ${range.label}: ${count.toString().padStart(6)} (${pct.padStart(5)}%) ${bar}`);
  }
}

main().catch((e) => {
  console.error("Failed:", e);
  process.exit(1);
});
