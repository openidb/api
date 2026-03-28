/**
 * Gemini vs Nemotron Benchmark
 *
 * Compares retrieval quality between:
 *   A) Gemini 3072d (baseline)
 *   B) Nemotron 2048d
 *
 * Searches only Quran and Bukhari hadith collections (the ones we embedded with Nemotron).
 * For Gemini, uses the existing full collections but filters to comparable results.
 *
 * Usage: bun run pipelines/benchmark/benchmark-nemotron.ts
 */

import "../env";
import {
  qdrant,
  QDRANT_QURAN_COLLECTION,
  QDRANT_HADITH_COLLECTION,
  QDRANT_QURAN_NEMOTRON_COLLECTION,
  QDRANT_HADITH_NEMOTRON_COLLECTION,
} from "../../src/qdrant";
import { generateEmbedding, normalizeArabicText } from "../../src/embeddings";
import { generateNemotronEmbedding } from "../../src/embeddings/nemotron";
import fs from "fs";
import path from "path";

// --- Test Queries ---

const TEST_QUERIES = [
  // Arabic topic queries
  "الصلاة",
  "التوبة",
  "أحكام الصيام",
  "الزكاة وأحكامها",
  "فضل الصدقة",
  "بر الوالدين",
  "أركان الإسلام",
  "الصبر على البلاء",
  "التوكل على الله",
  "الإيمان بالقدر",
  "أحكام البيع والشراء",
  "فضل العلم وطلبه",
  "صلاة الجماعة",
  "فضل قراءة القرآن",
  // Specific verse lookups
  "آية الكرسي",
  "سورة الفاتحة",
  "سورة الإخلاص",
  "قل هو الله أحد",
  // English queries
  "patience in Islam",
  "story of Moses",
  "prayer times",
  "charity and generosity",
  "forgiveness and mercy",
  "fasting rules Ramadan",
  "prophet Muhammad biography",
  // Cross-lingual + question queries
  "ما حكم الربا",
  "كيف نصلي صلاة الاستخارة",
  "ما هي أركان الصلاة",
  "ما فضل الدعاء",
  "حديث إنما الأعمال بالنيات",
  "قصة يوسف عليه السلام",
  "معنى الإحسان",
  "حقوق الجار في الإسلام",
  // Hadith-specific
  "حديث جبريل",
  "أحاديث عن الصبر",
  "سنن النبي في الطعام",
  "حديث من غشنا فليس منا",
  "فضل الصلاة على النبي",
  // Quran-specific
  "آيات عن الرحمة",
  "الآيات المتعلقة بالصبر",
  "قصة موسى في القرآن",
  "آيات الأحكام",
  "سورة البقرة",
];

// --- Types ---

interface SearchConfig {
  label: string;
  collections: { quran: string; hadith: string };
  generateEmbedding: (text: string) => Promise<number[]>;
}

interface SearchResult {
  id: string;
  score: number;
  text: string;
  payload: Record<string, unknown>;
}

interface QueryResult {
  query: string;
  config: string;
  embeddingMs: number;
  searchMs: number;
  totalMs: number;
  quranResults: SearchResult[];
  hadithResults: SearchResult[];
}

// --- Search ---

const TOP_N = 10;

async function searchCollection(
  collection: string,
  embedding: number[],
  limit: number,
  filter?: Record<string, unknown>,
): Promise<SearchResult[]> {
  try {
    const results = await qdrant.search(collection, {
      vector: embedding,
      limit,
      with_payload: true,
      score_threshold: 0.15,
      filter: filter || undefined,
    });

    return results.map((r) => {
      const payload = r.payload as Record<string, unknown>;
      const text =
        (payload.text as string) ||
        (payload.textPlain as string) ||
        "";
      return {
        id: String(r.id),
        score: r.score,
        text: text.slice(0, 200),
        payload,
      };
    });
  } catch (err) {
    console.warn(`  Search failed on ${collection}:`, (err as Error).message);
    return [];
  }
}

async function runQuery(query: string, config: SearchConfig): Promise<QueryResult> {
  const normalizedQuery = normalizeArabicText(query);

  // Embedding
  const embStart = Date.now();
  const embedding = await config.generateEmbedding(normalizedQuery);
  const embeddingMs = Date.now() - embStart;

  // Search Quran + Hadith in parallel
  const searchStart = Date.now();

  // For Gemini, filter hadith to only Bukhari for fair comparison
  const hadithFilter =
    config.label.includes("Gemini")
      ? { must: [{ key: "collectionSlug", match: { value: "bukhari" } }] }
      : undefined;

  const [quranResults, hadithResults] = await Promise.all([
    searchCollection(config.collections.quran, embedding, TOP_N),
    searchCollection(config.collections.hadith, embedding, TOP_N, hadithFilter),
  ]);
  const searchMs = Date.now() - searchStart;

  return {
    query,
    config: config.label,
    embeddingMs,
    searchMs,
    totalMs: embeddingMs + searchMs,
    quranResults: quranResults.slice(0, TOP_N),
    hadithResults: hadithResults.slice(0, TOP_N),
  };
}

// --- Metrics ---

function jaccard(setA: Set<string>, setB: Set<string>): number {
  if (setA.size === 0 && setB.size === 0) return 1;
  const intersection = new Set([...setA].filter((x) => setB.has(x)));
  const union = new Set([...setA, ...setB]);
  return intersection.size / union.size;
}

function getTopIds(result: QueryResult): Set<string> {
  return new Set([
    ...result.quranResults.map((r) => `q:${r.id}`),
    ...result.hadithResults.map((r) => `h:${r.id}`),
  ]);
}

function avgScore(results: SearchResult[]): number {
  if (results.length === 0) return 0;
  return results.reduce((s, r) => s + r.score, 0) / results.length;
}

// --- Display helpers ---

function formatTopResult(r: SearchResult, type: string): string {
  const payload = r.payload;
  if (type === "quran") {
    return `${payload.surahNameArabic} ${payload.surahNumber}:${payload.ayahNumber} (${r.score.toFixed(3)})`;
  }
  return `${payload.collectionNameArabic} #${payload.hadithNumber} (${r.score.toFixed(3)})`;
}

// --- Main ---

async function main() {
  console.log("=== Gemini vs Nemotron Retrieval Benchmark ===\n");
  console.log("Comparing: Quran (all ayahs) + Bukhari hadiths only\n");

  // Verify collections exist
  const collections = await qdrant.getCollections();
  const collectionNames = collections.collections.map((c) => c.name);

  const required = [
    QDRANT_QURAN_COLLECTION,
    QDRANT_HADITH_COLLECTION,
    QDRANT_QURAN_NEMOTRON_COLLECTION,
    QDRANT_HADITH_NEMOTRON_COLLECTION,
  ];

  for (const c of required) {
    if (!collectionNames.includes(c)) {
      console.error(`Missing required collection: ${c}`);
      if (c.includes("nemotron")) {
        console.error("Run: bun run pipelines/embed/generate-nemotron-embeddings.ts");
      }
      process.exit(1);
    }
  }

  // Print collection sizes
  for (const c of required) {
    try {
      const info = await qdrant.getCollection(c);
      console.log(`  ${c}: ${info.points_count} points`);
    } catch {}
  }
  console.log();

  // Define configs
  const configs: SearchConfig[] = [
    {
      label: "A: Gemini 3072d",
      collections: {
        quran: QDRANT_QURAN_COLLECTION,
        hadith: QDRANT_HADITH_COLLECTION,
      },
      generateEmbedding: (text: string) => generateEmbedding(text),
    },
    {
      label: "B: Nemotron 2048d",
      collections: {
        quran: QDRANT_QURAN_NEMOTRON_COLLECTION,
        hadith: QDRANT_HADITH_NEMOTRON_COLLECTION,
      },
      generateEmbedding: (text: string) => generateNemotronEmbedding(text),
    },
  ];

  // Run benchmark
  const allResults: QueryResult[] = [];
  const queryComparisons: Array<{
    query: string;
    geminiQuranTop: string;
    nemotronQuranTop: string;
    geminiHadithTop: string;
    nemotronHadithTop: string;
    quranOverlap: number;
    hadithOverlap: number;
  }> = [];

  for (let qi = 0; qi < TEST_QUERIES.length; qi++) {
    const query = TEST_QUERIES[qi];
    console.log(`[${qi + 1}/${TEST_QUERIES.length}] "${query}"`);

    const results: QueryResult[] = [];
    for (const config of configs) {
      const result = await runQuery(query, config);
      allResults.push(result);
      results.push(result);
      console.log(
        `  ${config.label}: emb=${result.embeddingMs}ms search=${result.searchMs}ms | q=${result.quranResults.length} (avg ${avgScore(result.quranResults).toFixed(3)}) h=${result.hadithResults.length} (avg ${avgScore(result.hadithResults).toFixed(3)})`,
      );
    }

    // Compare top results
    const gemini = results[0];
    const nemotron = results[1];

    const quranIdsGemini = new Set(gemini.quranResults.map((r) => r.id));
    const quranIdsNemotron = new Set(nemotron.quranResults.map((r) => r.id));
    const hadithIdsGemini = new Set(gemini.hadithResults.map((r) => r.id));
    const hadithIdsNemotron = new Set(nemotron.hadithResults.map((r) => r.id));

    queryComparisons.push({
      query,
      geminiQuranTop: gemini.quranResults[0]
        ? formatTopResult(gemini.quranResults[0], "quran")
        : "(none)",
      nemotronQuranTop: nemotron.quranResults[0]
        ? formatTopResult(nemotron.quranResults[0], "quran")
        : "(none)",
      geminiHadithTop: gemini.hadithResults[0]
        ? formatTopResult(gemini.hadithResults[0], "hadith")
        : "(none)",
      nemotronHadithTop: nemotron.hadithResults[0]
        ? formatTopResult(nemotron.hadithResults[0], "hadith")
        : "(none)",
      quranOverlap: jaccard(quranIdsGemini, quranIdsNemotron),
      hadithOverlap: jaccard(hadithIdsGemini, hadithIdsNemotron),
    });
  }

  // --- Summary ---

  console.log("\n" + "=".repeat(80));
  console.log("LATENCY SUMMARY");
  console.log("=".repeat(80));

  for (const config of configs) {
    const configResults = allResults.filter((r) => r.config === config.label);
    const avgEmb = configResults.reduce((s, r) => s + r.embeddingMs, 0) / configResults.length;
    const avgSearch = configResults.reduce((s, r) => s + r.searchMs, 0) / configResults.length;
    const avgTotal = configResults.reduce((s, r) => s + r.totalMs, 0) / configResults.length;
    const p95Total = configResults.map((r) => r.totalMs).sort((a, b) => a - b)[
      Math.floor(configResults.length * 0.95)
    ];

    console.log(`\n${config.label}:`);
    console.log(`  Avg embedding:  ${avgEmb.toFixed(0)}ms`);
    console.log(`  Avg search:     ${avgSearch.toFixed(0)}ms`);
    console.log(`  Avg total:      ${avgTotal.toFixed(0)}ms`);
    console.log(`  P95 total:      ${p95Total}ms`);
  }

  // --- Score Distribution ---

  console.log("\n" + "=".repeat(80));
  console.log("SCORE DISTRIBUTION");
  console.log("=".repeat(80));

  for (const config of configs) {
    const configResults = allResults.filter((r) => r.config === config.label);

    const quranScores = configResults.flatMap((r) => r.quranResults.map((q) => q.score));
    const hadithScores = configResults.flatMap((r) => r.hadithResults.map((h) => h.score));

    const avg = (arr: number[]) => (arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0);
    const median = (arr: number[]) => {
      if (!arr.length) return 0;
      const sorted = [...arr].sort((a, b) => a - b);
      const mid = Math.floor(sorted.length / 2);
      return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
    };

    console.log(`\n${config.label}:`);
    console.log(`  Quran:  avg=${avg(quranScores).toFixed(3)} median=${median(quranScores).toFixed(3)} count=${quranScores.length}`);
    console.log(`  Hadith: avg=${avg(hadithScores).toFixed(3)} median=${median(hadithScores).toFixed(3)} count=${hadithScores.length}`);
  }

  // --- Overlap Analysis ---

  console.log("\n" + "=".repeat(80));
  console.log("RESULT OVERLAP (Jaccard similarity)");
  console.log("=".repeat(80));

  const avgQuranOverlap =
    queryComparisons.reduce((s, c) => s + c.quranOverlap, 0) / queryComparisons.length;
  const avgHadithOverlap =
    queryComparisons.reduce((s, c) => s + c.hadithOverlap, 0) / queryComparisons.length;
  const avgOverall =
    queryComparisons.reduce((s, c) => s + (c.quranOverlap + c.hadithOverlap) / 2, 0) /
    queryComparisons.length;

  console.log(`\n  Quran top-10 overlap:  ${(avgQuranOverlap * 100).toFixed(1)}%`);
  console.log(`  Hadith top-10 overlap: ${(avgHadithOverlap * 100).toFixed(1)}%`);
  console.log(`  Overall overlap:       ${(avgOverall * 100).toFixed(1)}%`);

  // --- Top-1 Comparison ---

  console.log("\n" + "=".repeat(80));
  console.log("TOP-1 RESULT COMPARISON");
  console.log("=".repeat(80));

  let sameQuranTop1 = 0;
  let sameHadithTop1 = 0;

  for (const c of queryComparisons) {
    const geminiQuranId = allResults.find(
      (r) => r.config === configs[0].label && r.query === c.query,
    )?.quranResults[0]?.id;
    const nemotronQuranId = allResults.find(
      (r) => r.config === configs[1].label && r.query === c.query,
    )?.quranResults[0]?.id;
    if (geminiQuranId && geminiQuranId === nemotronQuranId) sameQuranTop1++;

    const geminiHadithId = allResults.find(
      (r) => r.config === configs[0].label && r.query === c.query,
    )?.hadithResults[0]?.id;
    const nemotronHadithId = allResults.find(
      (r) => r.config === configs[1].label && r.query === c.query,
    )?.hadithResults[0]?.id;
    if (geminiHadithId && geminiHadithId === nemotronHadithId) sameHadithTop1++;
  }

  console.log(
    `\n  Same Quran top-1:  ${sameQuranTop1}/${TEST_QUERIES.length} (${((sameQuranTop1 / TEST_QUERIES.length) * 100).toFixed(1)}%)`,
  );
  console.log(
    `  Same Hadith top-1: ${sameHadithTop1}/${TEST_QUERIES.length} (${((sameHadithTop1 / TEST_QUERIES.length) * 100).toFixed(1)}%)`,
  );

  // --- Detailed per-query comparison ---

  console.log("\n" + "=".repeat(80));
  console.log("PER-QUERY TOP RESULTS");
  console.log("=".repeat(80));

  for (const c of queryComparisons) {
    console.log(`\n"${c.query}":`);
    console.log(`  Quran  - Gemini: ${c.geminiQuranTop}`);
    console.log(`           Nemotron: ${c.nemotronQuranTop}`);
    console.log(`           Overlap: ${(c.quranOverlap * 100).toFixed(0)}%`);
    console.log(`  Hadith - Gemini: ${c.geminiHadithTop}`);
    console.log(`           Nemotron: ${c.nemotronHadithTop}`);
    console.log(`           Overlap: ${(c.hadithOverlap * 100).toFixed(0)}%`);
  }

  // Save JSON report
  const reportPath = path.join(process.cwd(), "benchmark-nemotron-report.json");
  const report = {
    timestamp: new Date().toISOString(),
    queryCount: TEST_QUERIES.length,
    configs: configs.map((c) => c.label),
    summary: {
      avgQuranOverlap,
      avgHadithOverlap,
      avgOverall,
      sameQuranTop1: `${sameQuranTop1}/${TEST_QUERIES.length}`,
      sameHadithTop1: `${sameHadithTop1}/${TEST_QUERIES.length}`,
    },
    queryComparisons,
    results: allResults,
  };
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
  console.log(`\nReport saved to: ${reportPath}`);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
