/**
 * Index data into Qdrant for each benchmark technique.
 *
 * For each technique: load data -> technique.prepareText() -> embed via Gemini -> upsert to Qdrant.
 *
 * Usage:
 *   bun run scripts/benchmark-techniques/index-data.ts [--techniques=baseline,stopword,...] [--batch-size=32] [--force]
 */

import "../env";
import crypto from "crypto";
import { qdrant } from "../../src/qdrant";
import { generateEmbeddings } from "../../src/embeddings/gemini";
import { prisma } from "../../src/db";
import { getTechniques, getCollectionNames } from "./registry";
import { loadQuranAyahs, loadHadithSubset } from "./utils/data-loader";
import { flushPendingEnrichments } from "./techniques/contextual-enrichment";
import type { RetrievalTechnique, QuranAyahData, HadithData } from "./types";

// Parse args
const args = process.argv.slice(2);
const forceFlag = args.includes("--force");
const batchSizeArg = args.find((a) => a.startsWith("--batch-size="));
const BATCH_SIZE = batchSizeArg ? parseInt(batchSizeArg.split("=")[1], 10) : 32;
const techniquesArg = args.find((a) => a.startsWith("--techniques="));
const techniqueIds = techniquesArg?.split("=")[1]?.split(",");

function generatePointId(prefix: string, ...parts: (string | number)[]): string {
  const input = `${prefix}_${parts.join("_")}`;
  return crypto.createHash("md5").update(input).digest("hex");
}

async function getExistingPointIds(collection: string): Promise<Set<string>> {
  if (forceFlag) return new Set();

  try {
    const ids = new Set<string>();
    let offset: string | undefined;

    while (true) {
      const result = await qdrant.scroll(collection, {
        limit: 1000,
        offset,
        with_payload: false,
        with_vector: false,
      });

      for (const point of result.points) {
        ids.add(point.id as string);
      }

      if (!result.next_page_offset) break;
      offset = result.next_page_offset as string;
    }

    return ids;
  } catch {
    return new Set();
  }
}

async function indexQuran(
  technique: RetrievalTechnique,
  ayahs: QuranAyahData[]
): Promise<number> {
  const { quran: collectionName } = getCollectionNames(technique.id);
  const existingIds = await getExistingPointIds(collectionName);
  console.log(`  [${collectionName}] ${existingIds.size} existing points`);

  let processed = 0;
  let skipped = 0;

  // For contextual enrichment: first pass to queue all items, then flush
  if (technique.id === "contextual") {
    // First pass: queue items that need LLM enrichment
    for (const ayah of ayahs) {
      await technique.prepareQuranText(ayah);
    }
    // Flush: generate all LLM enrichments in batches
    await flushPendingEnrichments();
  }

  // Process in batches
  for (let i = 0; i < ayahs.length; i += BATCH_SIZE) {
    const batch = ayahs.slice(i, i + BATCH_SIZE);

    // Prepare texts
    const prepared: Array<{ ayah: QuranAyahData; text: string; pointId: string }> = [];

    for (const ayah of batch) {
      const pointId = generatePointId(
        "bench_quran",
        technique.id,
        ayah.surahNumber,
        ayah.ayahNumber
      );

      if (existingIds.has(pointId)) {
        skipped++;
        continue;
      }

      const text = await technique.prepareQuranText(ayah);
      if (text) {
        prepared.push({ ayah, text, pointId });
      }
    }

    if (prepared.length === 0) continue;

    // Generate embeddings
    const texts = prepared.map((p) => p.text);
    const embeddings = await generateEmbeddings(texts);

    // Build points
    const points = prepared.map((p, idx) => ({
      id: p.pointId,
      vector: embeddings[idx],
      payload: {
        surahNumber: p.ayah.surahNumber,
        ayahNumber: p.ayah.ayahNumber,
        surahNameArabic: p.ayah.surahNameArabic,
        surahNameEnglish: p.ayah.surahNameEnglish,
        text: p.ayah.textUthmani,
        textPlain: p.ayah.textPlain,
        juzNumber: p.ayah.juzNumber,
        pageNumber: p.ayah.pageNumber,
      },
    }));

    await qdrant.upsert(collectionName, { wait: true, points });
    processed += points.length;

    if ((i + BATCH_SIZE) % (BATCH_SIZE * 10) === 0 || i + BATCH_SIZE >= ayahs.length) {
      console.log(
        `  [${collectionName}] ${processed} indexed, ${skipped} skipped (${Math.round(((i + BATCH_SIZE) / ayahs.length) * 100)}%)`
      );
    }

    // Rate limiting
    await new Promise((r) => setTimeout(r, 50));
  }

  return processed;
}

async function indexHadith(
  technique: RetrievalTechnique,
  hadiths: HadithData[]
): Promise<number> {
  const { hadith: collectionName } = getCollectionNames(technique.id);
  const existingIds = await getExistingPointIds(collectionName);
  console.log(`  [${collectionName}] ${existingIds.size} existing points`);

  let processed = 0;
  let skipped = 0;

  // For contextual enrichment: first pass to queue, then flush
  if (technique.id === "contextual") {
    for (const hadith of hadiths) {
      await technique.prepareHadithText(hadith);
    }
    await flushPendingEnrichments();
  }

  for (let i = 0; i < hadiths.length; i += BATCH_SIZE) {
    const batch = hadiths.slice(i, i + BATCH_SIZE);
    const prepared: Array<{ hadith: HadithData; text: string; pointId: string }> = [];

    for (const hadith of batch) {
      const pointId = generatePointId(
        "bench_hadith",
        technique.id,
        hadith.collectionSlug,
        hadith.hadithNumber
      );

      if (existingIds.has(pointId)) {
        skipped++;
        continue;
      }

      const text = await technique.prepareHadithText(hadith);
      if (text) {
        prepared.push({ hadith, text, pointId });
      }
    }

    if (prepared.length === 0) continue;

    const texts = prepared.map((p) => p.text);
    const embeddings = await generateEmbeddings(texts);

    const points = prepared.map((p, idx) => ({
      id: p.pointId,
      vector: embeddings[idx],
      payload: {
        collectionSlug: p.hadith.collectionSlug,
        collectionNameArabic: p.hadith.collectionNameArabic,
        hadithNumber: p.hadith.hadithNumber,
        text: p.hadith.textArabic,
        textPlain: p.hadith.textPlain,
        chapterArabic: p.hadith.chapterArabic,
        chapterEnglish: p.hadith.chapterEnglish,
      },
    }));

    await qdrant.upsert(collectionName, { wait: true, points });
    processed += points.length;

    if ((i + BATCH_SIZE) % (BATCH_SIZE * 10) === 0 || i + BATCH_SIZE >= hadiths.length) {
      console.log(
        `  [${collectionName}] ${processed} indexed, ${skipped} skipped (${Math.round(((i + BATCH_SIZE) / hadiths.length) * 100)}%)`
      );
    }

    await new Promise((r) => setTimeout(r, 50));
  }

  return processed;
}

async function main() {
  const techniques = getTechniques(techniqueIds);

  console.log("=".repeat(60));
  console.log("Benchmark Data Indexing");
  console.log("=".repeat(60));
  console.log(`Techniques: ${techniques.map((t) => t.id).join(", ")}`);
  console.log(`Batch size: ${BATCH_SIZE}`);
  console.log(`Force: ${forceFlag}`);
  console.log();

  // Load data once
  const quranAyahs = await loadQuranAyahs();
  const hadiths = await loadHadithSubset();
  console.log();

  const summary: Array<{ technique: string; quran: number; hadith: number }> = [];

  for (const technique of techniques) {
    console.log(`\n--- ${technique.name} (${technique.id}) ---`);
    console.log(`  ${technique.description}`);

    const quranCount = await indexQuran(technique, quranAyahs);
    const hadithCount = await indexHadith(technique, hadiths);

    summary.push({
      technique: technique.id,
      quran: quranCount,
      hadith: hadithCount,
    });
  }

  console.log("\n" + "=".repeat(60));
  console.log("INDEXING SUMMARY");
  console.log("=".repeat(60));
  for (const s of summary) {
    console.log(`  ${s.technique.padEnd(15)} Quran: ${s.quran}, Hadith: ${s.hadith}`);
  }

  // Verify collections
  console.log("\nCollection point counts:");
  for (const technique of techniques) {
    const { quran, hadith } = getCollectionNames(technique.id);
    try {
      const qInfo = await qdrant.getCollection(quran);
      const hInfo = await qdrant.getCollection(hadith);
      console.log(`  ${technique.id.padEnd(15)} Quran: ${qInfo.points_count}, Hadith: ${hInfo.points_count}`);
    } catch {
      console.log(`  ${technique.id.padEnd(15)} (error reading collection info)`);
    }
  }

  console.log("\nDone!");
}

main()
  .catch((e) => {
    console.error("Indexing failed:", e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
