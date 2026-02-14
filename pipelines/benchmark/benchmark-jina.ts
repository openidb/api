/**
 * Jina vs Gemini Benchmark
 *
 * Compares 4 configurations:
 *   A) Gemini 3072d + no reranker (baseline)
 *   B) Gemini 3072d + jina-reranker-v3
 *   C) Jina v3 1024d + no reranker
 *   D) Jina v3 1024d + jina-reranker-v3
 *
 * Usage: bun run pipelines/benchmark/benchmark-jina.ts
 */

import "../env";
import {
  qdrant,
  QDRANT_QURAN_COLLECTION,
  QDRANT_HADITH_COLLECTION,
  QDRANT_COLLECTION,
  QDRANT_QURAN_JINA_COLLECTION,
  QDRANT_HADITH_JINA_COLLECTION,
  QDRANT_PAGES_JINA_COLLECTION,
} from "../../src/qdrant";
import { generateEmbedding, normalizeArabicText } from "../../src/embeddings";
import { generateJinaEmbedding } from "../../src/embeddings/jina";
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
  "أحكام الطلاق",
  "صلاة الجماعة",
  "فضل قراءة القرآن",
  // Specific verse lookups
  "آية الكرسي",
  "سورة الفاتحة",
  "سورة الإخلاص",
  "آية النور",
  "قل هو الله أحد",
  // English queries
  "patience in Islam",
  "story of Moses",
  "prayer times",
  "rights of women in Islam",
  "charity and generosity",
  "forgiveness and mercy",
  "fasting rules Ramadan",
  "pilgrimage Hajj",
  "prophet Muhammad biography",
  "Islamic finance",
  // Cross-lingual + question queries
  "ما حكم الربا",
  "كيف نصلي صلاة الاستخارة",
  "ما هي أركان الصلاة",
  "هل يجوز الجمع بين الصلاتين",
  "ما فضل الدعاء",
  "حديث إنما الأعمال بالنيات",
  "قصة يوسف عليه السلام",
  "معنى الإحسان",
  "الفرق بين السنة والبدعة",
  "حقوق الجار في الإسلام",
  // Hadith-specific
  "حديث جبريل",
  "أحاديث عن الصبر",
  "سنن النبي في الطعام",
  "حديث من غشنا فليس منا",
  "فضل الصلاة على النبي",
  // Book-specific topics
  "أصول الفقه",
  "علم الكلام",
  "النحو والصرف",
  "تفسير ابن كثير",
  "فقه المعاملات",
];

// --- Types ---

interface SearchConfig {
  label: string;
  embeddingModel: "gemini" | "jina";
  reranker: "none" | "jina-reranker-v3";
  collections: { quran: string; hadith: string; pages: string };
  generateEmbedding: (text: string) => Promise<number[]>;
}

interface QueryResult {
  query: string;
  config: string;
  embeddingMs: number;
  searchMs: number;
  rerankMs: number;
  totalMs: number;
  quranResults: Array<{ id: string; score: number; text: string }>;
  hadithResults: Array<{ id: string; score: number; text: string }>;
  bookResults: Array<{ id: string; score: number; text: string }>;
}

// --- Jina Reranker ---

async function jinaRerank(
  query: string,
  documents: Array<{ id: string; text: string; score: number; type: string }>,
  topN: number,
): Promise<Array<{ id: string; text: string; score: number; type: string }>> {
  const apiKey = process.env.JINA_API_KEY;
  if (!apiKey) throw new Error("JINA_API_KEY not set");

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 10000);

  try {
    const response = await fetch("https://api.jina.ai/v1/rerank", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: "jina-reranker-v3",
        query,
        documents: documents.map((d) => d.text),
        top_n: topN,
      }),
      signal: controller.signal,
    });

    if (!response.ok) throw new Error(`Jina rerank error: ${response.status}`);
    const data = await response.json();

    return (data.results as Array<{ index: number; relevance_score: number }>).map((r) => ({
      ...documents[r.index],
      score: r.relevance_score,
    }));
  } finally {
    clearTimeout(timeoutId);
  }
}

// --- Search ---

const TOP_N = 10;

async function searchCollection(
  collection: string,
  embedding: number[],
  limit: number,
): Promise<Array<{ id: string; score: number; text: string }>> {
  try {
    const results = await qdrant.search(collection, {
      vector: embedding,
      limit,
      with_payload: true,
      score_threshold: 0.2,
    });

    return results.map((r) => {
      const payload = r.payload as Record<string, unknown>;
      const text =
        (payload.embeddedText as string) ||
        (payload.text as string) ||
        (payload.textSnippet as string) ||
        (payload.textPlain as string) ||
        "";
      const id = String(r.id);
      return { id, score: r.score, text: text.slice(0, 200) };
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

  // Search
  const searchStart = Date.now();
  const [quranResults, hadithResults, bookResults] = await Promise.all([
    searchCollection(config.collections.quran, embedding, TOP_N),
    searchCollection(config.collections.hadith, embedding, TOP_N),
    searchCollection(config.collections.pages, embedding, TOP_N),
  ]);
  const searchMs = Date.now() - searchStart;

  // Rerank
  let rerankMs = 0;
  let finalQuran = quranResults;
  let finalHadith = hadithResults;
  let finalBooks = bookResults;

  if (config.reranker === "jina-reranker-v3") {
    const rerankStart = Date.now();
    const allDocs = [
      ...quranResults.map((r) => ({ ...r, type: "quran" })),
      ...hadithResults.map((r) => ({ ...r, type: "hadith" })),
      ...bookResults.map((r) => ({ ...r, type: "book" })),
    ];

    if (allDocs.length > 0) {
      try {
        const reranked = await jinaRerank(query, allDocs, Math.min(allDocs.length, 30));
        finalQuran = reranked.filter((r) => r.type === "quran").slice(0, TOP_N);
        finalHadith = reranked.filter((r) => r.type === "hadith").slice(0, TOP_N);
        finalBooks = reranked.filter((r) => r.type === "book").slice(0, TOP_N);
      } catch (err) {
        console.warn(`  Rerank failed for "${query}":`, (err as Error).message);
      }
    }
    rerankMs = Date.now() - rerankStart;
  }

  return {
    query,
    config: config.label,
    embeddingMs,
    searchMs,
    rerankMs,
    totalMs: embeddingMs + searchMs + rerankMs,
    quranResults: finalQuran.slice(0, 5),
    hadithResults: finalHadith.slice(0, 5),
    bookResults: finalBooks.slice(0, 5),
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
    ...result.bookResults.map((r) => `b:${r.id}`),
  ]);
}

// --- Main ---

async function main() {
  console.log("=== Jina vs Gemini Benchmark ===\n");

  // Verify collections exist
  const collections = await qdrant.getCollections();
  const collectionNames = collections.collections.map((c) => c.name);

  const requiredCollections = [
    QDRANT_QURAN_COLLECTION,
    QDRANT_HADITH_COLLECTION,
    QDRANT_COLLECTION,
  ];
  const jinaCollections = [
    QDRANT_QURAN_JINA_COLLECTION,
    QDRANT_HADITH_JINA_COLLECTION,
    QDRANT_PAGES_JINA_COLLECTION,
  ];

  for (const c of requiredCollections) {
    if (!collectionNames.includes(c)) {
      console.error(`Missing required collection: ${c}`);
      process.exit(1);
    }
  }

  const hasJinaCollections = jinaCollections.every((c) => collectionNames.includes(c));
  if (!hasJinaCollections) {
    console.warn("Jina collections not found. Skipping configs C and D.");
    console.warn("Run: bun run pipelines/embed/generate-jina-embeddings.ts\n");
  }

  // Define configs
  const configs: SearchConfig[] = [
    {
      label: "A: Gemini (baseline)",
      embeddingModel: "gemini",
      reranker: "none",
      collections: {
        quran: QDRANT_QURAN_COLLECTION,
        hadith: QDRANT_HADITH_COLLECTION,
        pages: QDRANT_COLLECTION,
      },
      generateEmbedding: (text: string) => generateEmbedding(text),
    },
    {
      label: "B: Gemini + Jina Reranker",
      embeddingModel: "gemini",
      reranker: "jina-reranker-v3",
      collections: {
        quran: QDRANT_QURAN_COLLECTION,
        hadith: QDRANT_HADITH_COLLECTION,
        pages: QDRANT_COLLECTION,
      },
      generateEmbedding: (text: string) => generateEmbedding(text),
    },
  ];

  if (hasJinaCollections) {
    configs.push(
      {
        label: "C: Jina v3",
        embeddingModel: "jina",
        reranker: "none",
        collections: {
          quran: QDRANT_QURAN_JINA_COLLECTION,
          hadith: QDRANT_HADITH_JINA_COLLECTION,
          pages: QDRANT_PAGES_JINA_COLLECTION,
        },
        generateEmbedding: (text: string) => generateJinaEmbedding(text, "retrieval.query"),
      },
      {
        label: "D: Jina v3 + Jina Reranker",
        embeddingModel: "jina",
        reranker: "jina-reranker-v3",
        collections: {
          quran: QDRANT_QURAN_JINA_COLLECTION,
          hadith: QDRANT_HADITH_JINA_COLLECTION,
          pages: QDRANT_PAGES_JINA_COLLECTION,
        },
        generateEmbedding: (text: string) => generateJinaEmbedding(text, "retrieval.query"),
      },
    );
  }

  // Run benchmark
  const allResults: QueryResult[] = [];

  for (let qi = 0; qi < TEST_QUERIES.length; qi++) {
    const query = TEST_QUERIES[qi];
    console.log(`[${qi + 1}/${TEST_QUERIES.length}] "${query}"`);

    for (const config of configs) {
      const result = await runQuery(query, config);
      allResults.push(result);
      console.log(
        `  ${config.label}: emb=${result.embeddingMs}ms search=${result.searchMs}ms rerank=${result.rerankMs}ms total=${result.totalMs}ms | q=${result.quranResults.length} h=${result.hadithResults.length} b=${result.bookResults.length}`
      );
    }
  }

  // --- Summary ---

  console.log("\n" + "=".repeat(80));
  console.log("SUMMARY");
  console.log("=".repeat(80));

  for (const config of configs) {
    const configResults = allResults.filter((r) => r.config === config.label);
    const avgEmb = configResults.reduce((s, r) => s + r.embeddingMs, 0) / configResults.length;
    const avgSearch = configResults.reduce((s, r) => s + r.searchMs, 0) / configResults.length;
    const avgRerank = configResults.reduce((s, r) => s + r.rerankMs, 0) / configResults.length;
    const avgTotal = configResults.reduce((s, r) => s + r.totalMs, 0) / configResults.length;
    const p95Total = configResults.map((r) => r.totalMs).sort((a, b) => a - b)[
      Math.floor(configResults.length * 0.95)
    ];

    console.log(`\n${config.label}:`);
    console.log(`  Avg embedding:  ${avgEmb.toFixed(0)}ms`);
    console.log(`  Avg search:     ${avgSearch.toFixed(0)}ms`);
    console.log(`  Avg rerank:     ${avgRerank.toFixed(0)}ms`);
    console.log(`  Avg total:      ${avgTotal.toFixed(0)}ms`);
    console.log(`  P95 total:      ${p95Total}ms`);
  }

  // Overlap analysis
  if (configs.length >= 2) {
    console.log("\n" + "-".repeat(40));
    console.log("RESULT OVERLAP (Jaccard similarity, top-10)");
    console.log("-".repeat(40));

    for (let i = 0; i < configs.length; i++) {
      for (let j = i + 1; j < configs.length; j++) {
        const iResults = allResults.filter((r) => r.config === configs[i].label);
        const jResults = allResults.filter((r) => r.config === configs[j].label);

        let totalJaccard = 0;
        for (let q = 0; q < TEST_QUERIES.length; q++) {
          const setI = getTopIds(iResults[q]);
          const setJ = getTopIds(jResults[q]);
          totalJaccard += jaccard(setI, setJ);
        }
        const avgJaccard = totalJaccard / TEST_QUERIES.length;
        console.log(
          `  ${configs[i].label} vs ${configs[j].label}: ${(avgJaccard * 100).toFixed(1)}%`
        );
      }
    }
  }

  // Save JSON report
  const reportPath = path.join(process.cwd(), "benchmark-jina-report.json");
  const report = {
    timestamp: new Date().toISOString(),
    queryCount: TEST_QUERIES.length,
    configs: configs.map((c) => c.label),
    results: allResults,
  };
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
  console.log(`\nReport saved to: ${reportPath}`);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
