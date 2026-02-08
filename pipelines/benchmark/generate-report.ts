/**
 * Generate a readable markdown report from benchmark results.
 *
 * Usage:
 *   bun run scripts/benchmark-techniques/generate-report.ts [--input=results/benchmark-results.json]
 */

import * as fs from "fs";
import * as path from "path";
import type { BenchmarkReport, TechniqueResult, CategoryMetrics } from "./types";

const LANGUAGE_NAMES: Record<string, string> = {
  ar: "Arabic",
  en: "English",
  fr: "French",
  id: "Indonesian",
  ur: "Urdu",
  es: "Spanish",
  zh: "Chinese",
  pt: "Portuguese",
  ru: "Russian",
  ja: "Japanese",
  ko: "Korean",
  it: "Italian",
  bn: "Bengali",
};

// Parse args
const args = process.argv.slice(2);
const inputArg = args.find((a) => a.startsWith("--input="));
const inputFile = inputArg?.split("=")[1] || "results/benchmark-results.json";

function pct(n: number): string {
  return `${(n * 100).toFixed(1)}%`;
}

function generateMarkdown(report: BenchmarkReport): string {
  const lines: string[] = [];

  lines.push("# Retrieval Techniques Benchmark Report");
  lines.push("");
  lines.push(`**Date**: ${new Date(report.timestamp).toLocaleDateString()}`);
  lines.push(`**Test Set**: ${report.test_set}`);
  lines.push(`**Total Queries**: ${report.total_queries}`);
  lines.push(`**Techniques Tested**: ${report.techniques.length}`);
  lines.push("");

  // Overall comparison table
  lines.push("## Overall Comparison");
  lines.push("");
  lines.push(
    "| Technique | R@1 | R@5 | R@10 | R@20 | MRR | NDCG@10 | P@5 | P@10 | Hit Rate |"
  );
  lines.push(
    "|-----------|-----|-----|------|------|-----|---------|-----|------|----------|"
  );

  // Sort by R@10 descending
  const sorted = [...report.techniques].sort(
    (a, b) => b.aggregate.recall_at_10 - a.aggregate.recall_at_10
  );

  for (const t of sorted) {
    const m = t.aggregate;
    lines.push(
      `| **${t.technique_name}** | ${pct(m.recall_at_1)} | ${pct(m.recall_at_5)} | ${pct(m.recall_at_10)} | ${pct(m.recall_at_20)} | ${m.mrr.toFixed(3)} | ${m.ndcg_at_10.toFixed(3)} | ${pct(m.precision_at_5)} | ${pct(m.precision_at_10)} | ${pct(m.hit_rate)} |`
    );
  }

  // Winner per metric
  lines.push("");
  lines.push("## Winners by Metric");
  lines.push("");

  const metricKeys: Array<{
    key: keyof typeof sorted[0]["aggregate"];
    label: string;
  }> = [
    { key: "recall_at_1", label: "Recall@1" },
    { key: "recall_at_5", label: "Recall@5" },
    { key: "recall_at_10", label: "Recall@10" },
    { key: "recall_at_20", label: "Recall@20" },
    { key: "mrr", label: "MRR" },
    { key: "ndcg_at_10", label: "NDCG@10" },
    { key: "precision_at_5", label: "Precision@5" },
    { key: "precision_at_10", label: "Precision@10" },
    { key: "hit_rate", label: "Hit Rate" },
  ];

  lines.push("| Metric | Winner | Score | Runner-up | Score |");
  lines.push("|--------|--------|-------|-----------|-------|");

  for (const { key, label } of metricKeys) {
    const ranked = [...sorted].sort(
      (a, b) =>
        (b.aggregate[key] as number) - (a.aggregate[key] as number)
    );
    const winner = ranked[0];
    const runnerUp = ranked[1];
    const winnerScore =
      typeof winner.aggregate[key] === "number"
        ? key === "mrr" || key === "ndcg_at_10"
          ? (winner.aggregate[key] as number).toFixed(3)
          : pct(winner.aggregate[key] as number)
        : "-";
    const runnerUpScore = runnerUp
      ? typeof runnerUp.aggregate[key] === "number"
        ? key === "mrr" || key === "ndcg_at_10"
          ? (runnerUp.aggregate[key] as number).toFixed(3)
          : pct(runnerUp.aggregate[key] as number)
        : "-"
      : "-";

    lines.push(
      `| ${label} | **${winner.technique_name}** | ${winnerScore} | ${runnerUp?.technique_name || "-"} | ${runnerUpScore} |`
    );
  }

  // Breakdown by category
  lines.push("");
  lines.push("## Breakdown by Category");
  lines.push("");

  const allCategories = new Set<string>();
  for (const t of report.techniques) {
    Object.keys(t.aggregate.by_category).forEach((c) => allCategories.add(c));
  }

  for (const category of [...allCategories].sort()) {
    lines.push(`### ${category}`);
    lines.push("");
    lines.push("| Technique | R@10 | MRR | NDCG@10 | Hit Rate | N |");
    lines.push("|-----------|------|-----|---------|----------|---|");

    const catResults = report.techniques
      .filter((t) => t.aggregate.by_category[category])
      .map((t) => ({
        name: t.technique_name,
        metrics: t.aggregate.by_category[category],
      }))
      .sort((a, b) => b.metrics.recall_at_10 - a.metrics.recall_at_10);

    for (const { name, metrics: m } of catResults) {
      lines.push(
        `| ${name} | ${pct(m.recall_at_10)} | ${m.mrr.toFixed(3)} | ${m.ndcg_at_10.toFixed(3)} | ${pct(m.hit_rate)} | ${m.count} |`
      );
    }

    lines.push("");
  }

  // Breakdown by language
  lines.push("## Breakdown by Language");
  lines.push("");

  const allLanguages = new Set<string>();
  for (const t of report.techniques) {
    Object.keys(t.aggregate.by_language).forEach((l) => allLanguages.add(l));
  }

  for (const lang of [...allLanguages].sort()) {
    lines.push(`### ${LANGUAGE_NAMES[lang] || lang.toUpperCase()}`);
    lines.push("");
    lines.push("| Technique | R@10 | MRR | NDCG@10 | Hit Rate | N |");
    lines.push("|-----------|------|-----|---------|----------|---|");

    const langResults = report.techniques
      .filter((t) => t.aggregate.by_language[lang])
      .map((t) => ({
        name: t.technique_name,
        metrics: t.aggregate.by_language[lang],
      }))
      .sort((a, b) => b.metrics.recall_at_10 - a.metrics.recall_at_10);

    for (const { name, metrics: m } of langResults) {
      lines.push(
        `| ${name} | ${pct(m.recall_at_10)} | ${m.mrr.toFixed(3)} | ${m.ndcg_at_10.toFixed(3)} | ${pct(m.hit_rate)} | ${m.count} |`
      );
    }

    lines.push("");
  }

  // Breakdown by difficulty
  lines.push("## Breakdown by Difficulty");
  lines.push("");

  for (const diff of ["easy", "medium", "hard"]) {
    const hasDiff = report.techniques.some(
      (t) => t.aggregate.by_difficulty[diff]
    );
    if (!hasDiff) continue;

    lines.push(`### ${diff.charAt(0).toUpperCase() + diff.slice(1)}`);
    lines.push("");
    lines.push("| Technique | R@10 | MRR | NDCG@10 | Hit Rate | N |");
    lines.push("|-----------|------|-----|---------|----------|---|");

    const diffResults = report.techniques
      .filter((t) => t.aggregate.by_difficulty[diff])
      .map((t) => ({
        name: t.technique_name,
        metrics: t.aggregate.by_difficulty[diff],
      }))
      .sort((a, b) => b.metrics.recall_at_10 - a.metrics.recall_at_10);

    for (const { name, metrics: m } of diffResults) {
      lines.push(
        `| ${name} | ${pct(m.recall_at_10)} | ${m.mrr.toFixed(3)} | ${m.ndcg_at_10.toFixed(3)} | ${pct(m.hit_rate)} | ${m.count} |`
      );
    }

    lines.push("");
  }

  // Delta vs baseline
  const baseline = report.techniques.find(
    (t) => t.technique_id === "baseline"
  );
  if (baseline) {
    lines.push("## Delta vs Baseline");
    lines.push("");
    lines.push("| Technique | R@10 | MRR | NDCG@10 | Hit Rate |");
    lines.push("|-----------|------|-----|---------|----------|");

    for (const t of sorted) {
      if (t.technique_id === "baseline") continue;
      const deltaR10 = t.aggregate.recall_at_10 - baseline.aggregate.recall_at_10;
      const deltaMRR = t.aggregate.mrr - baseline.aggregate.mrr;
      const deltaNDCG = t.aggregate.ndcg_at_10 - baseline.aggregate.ndcg_at_10;
      const deltaHit = t.aggregate.hit_rate - baseline.aggregate.hit_rate;

      const fmt = (d: number, isPercent = true) => {
        const sign = d >= 0 ? "+" : "";
        return isPercent
          ? `${sign}${(d * 100).toFixed(1)}pp`
          : `${sign}${d.toFixed(3)}`;
      };

      lines.push(
        `| ${t.technique_name} | ${fmt(deltaR10)} | ${fmt(deltaMRR, false)} | ${fmt(deltaNDCG, false)} | ${fmt(deltaHit)} |`
      );
    }
  }

  lines.push("");
  lines.push("---");
  lines.push(
    `*Generated at ${new Date(report.timestamp).toISOString()} by benchmark-techniques*`
  );

  return lines.join("\n");
}

function main() {
  const inputPath = path.join(__dirname, inputFile);

  if (!fs.existsSync(inputPath)) {
    console.error(`Results file not found: ${inputPath}`);
    console.error("Run the benchmark first:");
    console.error("  bun run scripts/benchmark-techniques/run-benchmark.ts");
    process.exit(1);
  }

  const report = JSON.parse(
    fs.readFileSync(inputPath, "utf-8")
  ) as BenchmarkReport;

  console.log(`Loaded results: ${report.techniques.length} techniques, ${report.total_queries} queries`);

  // Generate markdown report
  const markdown = generateMarkdown(report);
  const reportPath = path.join(__dirname, "results/report.md");
  fs.writeFileSync(reportPath, markdown);
  console.log(`Markdown report saved: ${reportPath}`);

  // Also save a summary JSON
  const summary = report.techniques.map((t) => ({
    id: t.technique_id,
    name: t.technique_name,
    ...t.aggregate,
  }));
  const summaryPath = path.join(__dirname, "results/report.json");
  fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));
  console.log(`Summary JSON saved: ${summaryPath}`);

  // Print the comparison table to console
  console.log("\n" + markdown.split("\n").slice(0, 30).join("\n"));
  console.log("\n... (full report saved to file)");
}

main();
