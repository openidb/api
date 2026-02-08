/**
 * Validate Training Data Quality
 *
 * Performs quality checks on training data:
 * - Query/positive length validation
 * - Language detection and distribution
 * - Duplicate detection
 * - Sample review for manual inspection
 * - Hard negative quality check
 *
 * Usage:
 *   bun run training/scripts/validate-training-data.ts [options]
 *
 * Options:
 *   --input=<file>      Input JSONL file to validate
 *   --sample=<n>        Number of samples to show (default: 20)
 *   --check-negatives   Perform detailed negative validation
 *   --export-issues     Export problematic pairs to issues.jsonl
 */

import * as fs from "fs";
import * as path from "path";

// Configuration
const DATA_DIR = path.join(__dirname, "../data");
const MIN_QUERY_LENGTH = 10;
const MAX_QUERY_LENGTH = 1000;
const MIN_POSITIVE_LENGTH = 20;
const MAX_POSITIVE_LENGTH = 5000;

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

interface ValidationIssue {
  lineNumber: number;
  pair: TrainingPair;
  issues: string[];
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
    input: (options["input"] as string) || path.join(DATA_DIR, "combined_training.jsonl"),
    sample: parseInt(options["sample"] as string) || 20,
    checkNegatives: options["check-negatives"] === true,
    exportIssues: options["export-issues"] === true,
  };
}

/**
 * Detect language of text (simple heuristic)
 */
function detectLanguage(text: string): "ar" | "en" | "mixed" | "unknown" {
  const arabicChars = (text.match(/[\u0600-\u06FF]/g) || []).length;
  const latinChars = (text.match(/[a-zA-Z]/g) || []).length;
  const totalChars = text.replace(/\s/g, "").length;

  if (totalChars === 0) return "unknown";

  const arabicRatio = arabicChars / totalChars;
  const latinRatio = latinChars / totalChars;

  if (arabicRatio > 0.7) return "ar";
  if (latinRatio > 0.7) return "en";
  if (arabicRatio > 0.2 && latinRatio > 0.2) return "mixed";
  if (arabicRatio > latinRatio) return "ar";
  return "en";
}

/**
 * Normalize text for duplicate detection
 */
function normalizeForDupe(text: string): string {
  return text
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ")
    .replace(/[\u064B-\u0652]/g, ""); // Remove Arabic diacritics
}

/**
 * Check Jaccard similarity between two texts
 */
function jaccardSimilarity(text1: string, text2: string): number {
  const words1 = new Set(normalizeForDupe(text1).split(" "));
  const words2 = new Set(normalizeForDupe(text2).split(" "));

  if (words1.size === 0 || words2.size === 0) return 0;

  const intersection = Array.from(words1).filter((w) => words2.has(w)).length;
  const union = words1.size + words2.size - intersection;

  return intersection / union;
}

/**
 * Validate a single training pair
 */
function validatePair(pair: TrainingPair, lineNum: number): ValidationIssue | null {
  const issues: string[] = [];

  // Query validation
  if (!pair.query || typeof pair.query !== "string") {
    issues.push("Missing or invalid query");
  } else {
    if (pair.query.length < MIN_QUERY_LENGTH) {
      issues.push(`Query too short (${pair.query.length} chars)`);
    }
    if (pair.query.length > MAX_QUERY_LENGTH) {
      issues.push(`Query too long (${pair.query.length} chars)`);
    }
    if (pair.query.includes("[DRY RUN]")) {
      issues.push("Dry run placeholder query");
    }
  }

  // Positive validation
  if (!pair.pos || !Array.isArray(pair.pos) || pair.pos.length === 0) {
    issues.push("Missing or empty pos array");
  } else {
    for (let i = 0; i < pair.pos.length; i++) {
      const pos = pair.pos[i];
      if (!pos || typeof pos !== "string") {
        issues.push(`Invalid positive at index ${i}`);
      } else if (pos.length < MIN_POSITIVE_LENGTH) {
        issues.push(`Positive too short (${pos.length} chars)`);
      } else if (pos.length > MAX_POSITIVE_LENGTH) {
        issues.push(`Positive too long (${pos.length} chars)`);
      }
    }
  }

  // Check if query and positive are too similar (potential error)
  if (pair.query && pair.pos?.[0]) {
    const similarity = jaccardSimilarity(pair.query, pair.pos[0]);
    if (similarity > 0.95) {
      issues.push(`Query and positive nearly identical (${(similarity * 100).toFixed(1)}% overlap)`);
    }
  }

  if (issues.length > 0) {
    return { lineNumber: lineNum, pair, issues };
  }

  return null;
}

/**
 * Validate hard negatives quality
 */
function validateNegatives(
  pair: TrainingPair,
  lineNum: number
): ValidationIssue | null {
  const issues: string[] = [];

  if (!pair.neg || pair.neg.length === 0) return null;

  for (let i = 0; i < pair.neg.length; i++) {
    const neg = pair.neg[i];

    // Check if negative is valid
    if (!neg || typeof neg !== "string" || neg.length < 10) {
      issues.push(`Invalid negative at index ${i}`);
      continue;
    }

    // Check if negative is too similar to any positive (false negative)
    for (const pos of pair.pos) {
      const similarity = jaccardSimilarity(neg, pos);
      if (similarity > 0.8) {
        issues.push(
          `Negative ${i} too similar to positive (${(similarity * 100).toFixed(1)}% - potential false negative)`
        );
      }
    }

    // Check if negative is too similar to query (not useful)
    const querySim = jaccardSimilarity(neg, pair.query);
    if (querySim > 0.9) {
      issues.push(`Negative ${i} too similar to query`);
    }
  }

  if (issues.length > 0) {
    return { lineNumber: lineNum, pair, issues };
  }

  return null;
}

async function main() {
  const options = parseArgs();

  console.log("=".repeat(60));
  console.log("Training Data Validation");
  console.log("=".repeat(60));
  console.log();

  if (!fs.existsSync(options.input)) {
    console.error(`Error: File not found: ${options.input}`);
    process.exit(1);
  }

  console.log(`Validating: ${options.input}`);
  console.log();

  // Read and validate
  const content = fs.readFileSync(options.input, "utf-8");
  const lines = content.trim().split("\n");

  const stats = {
    total: 0,
    valid: 0,
    invalid: 0,
    withNegatives: 0,
    bySource: {} as Record<string, number>,
    byLanguage: {} as Record<string, number>,
    byQueryType: {} as Record<string, number>,
    queryLengths: [] as number[],
    posLengths: [] as number[],
    negCounts: [] as number[],
  };

  const issues: ValidationIssue[] = [];
  const negativeIssues: ValidationIssue[] = [];
  const seenQueries = new Map<string, number>(); // query -> line number
  const duplicates: Array<{ lineNum: number; originalLineNum: number; query: string }> = [];

  // Samples for manual review
  const samples: Array<{ lineNum: number; pair: TrainingPair }> = [];
  const sampleIndices = new Set<number>();
  const sampleInterval = Math.max(1, Math.floor(lines.length / options.sample));

  for (let i = 0; i < options.sample; i++) {
    const idx = i * sampleInterval;
    if (idx < lines.length) sampleIndices.add(idx);
  }

  console.log(`Processing ${lines.length} pairs...`);

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!line.trim()) continue;

    let pair: TrainingPair;
    try {
      pair = JSON.parse(line);
    } catch {
      issues.push({
        lineNumber: i + 1,
        pair: { query: "", pos: [], neg: [] },
        issues: ["Invalid JSON"],
      });
      stats.invalid++;
      continue;
    }

    stats.total++;

    // Validate pair
    const pairIssue = validatePair(pair, i + 1);
    if (pairIssue) {
      issues.push(pairIssue);
      stats.invalid++;
    } else {
      stats.valid++;
    }

    // Validate negatives
    if (options.checkNegatives && pair.neg?.length > 0) {
      const negIssue = validateNegatives(pair, i + 1);
      if (negIssue) {
        negativeIssues.push(negIssue);
      }
    }

    // Check for duplicates
    const normalizedQuery = normalizeForDupe(pair.query);
    if (seenQueries.has(normalizedQuery)) {
      duplicates.push({
        lineNum: i + 1,
        originalLineNum: seenQueries.get(normalizedQuery)!,
        query: pair.query.substring(0, 50),
      });
    } else {
      seenQueries.set(normalizedQuery, i + 1);
    }

    // Collect stats
    const source = pair.source || "original";
    stats.bySource[source] = (stats.bySource[source] || 0) + 1;

    const language = pair.language || detectLanguage(pair.query);
    stats.byLanguage[language] = (stats.byLanguage[language] || 0) + 1;

    const queryType = pair.query_type || pair.pair_type || "translation";
    stats.byQueryType[queryType] = (stats.byQueryType[queryType] || 0) + 1;

    if (pair.neg?.length > 0) {
      stats.withNegatives++;
      stats.negCounts.push(pair.neg.length);
    }

    stats.queryLengths.push(pair.query?.length || 0);
    pair.pos?.forEach((p) => stats.posLengths.push(p?.length || 0));

    // Collect samples
    if (sampleIndices.has(i)) {
      samples.push({ lineNum: i + 1, pair });
    }
  }

  // Print validation report
  console.log("\n" + "=".repeat(60));
  console.log("VALIDATION REPORT");
  console.log("=".repeat(60));

  console.log("\n## Overview");
  console.log(`  Total pairs: ${stats.total.toLocaleString()}`);
  console.log(`  Valid pairs: ${stats.valid.toLocaleString()} (${((stats.valid / stats.total) * 100).toFixed(1)}%)`);
  console.log(`  Invalid pairs: ${stats.invalid.toLocaleString()} (${((stats.invalid / stats.total) * 100).toFixed(1)}%)`);
  console.log(`  Duplicate queries: ${duplicates.length.toLocaleString()}`);
  console.log(`  Pairs with hard negatives: ${stats.withNegatives.toLocaleString()}`);

  console.log("\n## By Source");
  for (const [source, count] of Object.entries(stats.bySource).sort((a, b) => b[1] - a[1])) {
    const pct = ((count / stats.total) * 100).toFixed(1);
    console.log(`  ${source}: ${count.toLocaleString()} (${pct}%)`);
  }

  console.log("\n## By Language");
  for (const [lang, count] of Object.entries(stats.byLanguage).sort((a, b) => b[1] - a[1])) {
    const pct = ((count / stats.total) * 100).toFixed(1);
    console.log(`  ${lang}: ${count.toLocaleString()} (${pct}%)`);
  }

  console.log("\n## By Query Type");
  for (const [type, count] of Object.entries(stats.byQueryType).sort((a, b) => b[1] - a[1])) {
    const pct = ((count / stats.total) * 100).toFixed(1);
    console.log(`  ${type}: ${count.toLocaleString()} (${pct}%)`);
  }

  // Length statistics
  const avgQueryLen = stats.queryLengths.reduce((a, b) => a + b, 0) / stats.queryLengths.length;
  const avgPosLen = stats.posLengths.reduce((a, b) => a + b, 0) / stats.posLengths.length;
  const avgNegCount = stats.negCounts.length > 0
    ? stats.negCounts.reduce((a, b) => a + b, 0) / stats.negCounts.length
    : 0;

  console.log("\n## Length Statistics");
  console.log(`  Query length: avg=${avgQueryLen.toFixed(0)}, min=${Math.min(...stats.queryLengths)}, max=${Math.max(...stats.queryLengths)}`);
  console.log(`  Positive length: avg=${avgPosLen.toFixed(0)}, min=${Math.min(...stats.posLengths)}, max=${Math.max(...stats.posLengths)}`);
  if (stats.withNegatives > 0) {
    console.log(`  Negatives per pair: avg=${avgNegCount.toFixed(1)}, min=${Math.min(...stats.negCounts)}, max=${Math.max(...stats.negCounts)}`);
  }

  // Issues summary
  if (issues.length > 0) {
    console.log("\n## Validation Issues (first 10)");
    const issueCounts: Record<string, number> = {};
    for (const issue of issues) {
      for (const msg of issue.issues) {
        issueCounts[msg] = (issueCounts[msg] || 0) + 1;
      }
    }

    for (const [msg, count] of Object.entries(issueCounts).sort((a, b) => b[1] - a[1]).slice(0, 10)) {
      console.log(`  ${msg}: ${count}`);
    }
  }

  if (negativeIssues.length > 0) {
    console.log("\n## Hard Negative Issues (first 10)");
    const negIssueCounts: Record<string, number> = {};
    for (const issue of negativeIssues) {
      for (const msg of issue.issues) {
        negIssueCounts[msg] = (negIssueCounts[msg] || 0) + 1;
      }
    }

    for (const [msg, count] of Object.entries(negIssueCounts).sort((a, b) => b[1] - a[1]).slice(0, 10)) {
      console.log(`  ${msg}: ${count}`);
    }
  }

  // Sample output for manual review
  console.log("\n" + "=".repeat(60));
  console.log("SAMPLE PAIRS FOR REVIEW");
  console.log("=".repeat(60));

  for (const sample of samples.slice(0, 10)) {
    const lang = sample.pair.language || detectLanguage(sample.pair.query);
    console.log(`\n--- Line ${sample.lineNum} [${sample.pair.source || "original"}] [${lang}] ---`);
    console.log(`Query: ${sample.pair.query.substring(0, 80)}${sample.pair.query.length > 80 ? "..." : ""}`);
    console.log(`Positive: ${sample.pair.pos[0]?.substring(0, 60)}${(sample.pair.pos[0]?.length || 0) > 60 ? "..." : ""}`);
    if (sample.pair.neg?.length > 0) {
      console.log(`Negatives: ${sample.pair.neg.length}`);
    }
  }

  // Export issues if requested
  if (options.exportIssues && issues.length > 0) {
    const issuesPath = options.input.replace(".jsonl", "_issues.jsonl");
    const issueLines = issues.map((i) => JSON.stringify(i));
    fs.writeFileSync(issuesPath, issueLines.join("\n") + "\n");
    console.log(`\nIssues exported to: ${issuesPath}`);
  }

  // Final verdict
  console.log("\n" + "=".repeat(60));
  if (stats.invalid === 0 && duplicates.length < stats.total * 0.01) {
    console.log("VALIDATION PASSED");
  } else if (stats.invalid < stats.total * 0.05) {
    console.log("VALIDATION PASSED WITH WARNINGS");
    console.log(`Consider filtering ${stats.invalid} invalid pairs.`);
  } else {
    console.log("VALIDATION FAILED");
    console.log(`Too many invalid pairs (${((stats.invalid / stats.total) * 100).toFixed(1)}%).`);
  }
  console.log("=".repeat(60));
}

main().catch((e) => {
  console.error("Failed:", e);
  process.exit(1);
});
