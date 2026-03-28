/**
 * Generate Nemotron Embeddings Script
 *
 * Populates Qdrant collections with Nemotron embeddings (2048d) for Quran and Bukhari only.
 * Uses nvidia/llama-nemotron-embed-vl-1b-v2:free via OpenRouter.
 *
 * Free tier: 20 req/min, 50-1000 req/day — built-in rate limiting handles this.
 *
 * Usage: bun run pipelines/embed/generate-nemotron-embeddings.ts [options]
 *
 * Options:
 *   --collection=quran|hadith|all   Which collection(s) to process (default: all)
 *   --force                         Recreate collections even if they exist
 *   --batch-size=N                  Documents per embedding API call (default: 20)
 */

import "../env";
import { prisma } from "../../src/db";
import {
  qdrant,
  QDRANT_QURAN_NEMOTRON_COLLECTION,
  QDRANT_HADITH_NEMOTRON_COLLECTION,
} from "../../src/qdrant";
import { NEMOTRON_EMBEDDING_DIMENSIONS } from "../../src/constants";
import { generateNemotronEmbeddings } from "../../src/embeddings/nemotron";
import { normalizeArabicText, truncateForEmbedding } from "../../src/embeddings/gemini";
import { generateHadithSourceUrl } from "../../src/utils/source-urls";
import crypto from "crypto";

const forceFlag = process.argv.includes("--force");
const collectionArg = process.argv.find((arg) => arg.startsWith("--collection="));
const collectionFilter = collectionArg ? collectionArg.split("=")[1] : "all";
const batchSizeArg = process.argv.find((arg) => arg.startsWith("--batch-size="));
const BATCH_SIZE = batchSizeArg ? parseInt(batchSizeArg.split("=")[1], 10) : 20;

// No concurrency for free tier — rate limits are tight (20 req/min)
const CONCURRENCY = 1;

// --- Point ID generation (must match Gemini/Jina scripts for consistent IDs) ---

function generateAyahPointId(surahNumber: number, ayahNumber: number): string {
  const input = `ayah_${surahNumber}_${ayahNumber}`;
  return crypto.createHash("md5").update(input).digest("hex");
}

function generateHadithPointId(collectionSlug: string, hadithNumber: string): string {
  const input = `hadith_${collectionSlug}_${hadithNumber}`;
  return crypto.createHash("md5").update(input).digest("hex");
}

// --- Collection helpers ---

async function ensureCollection(
  name: string,
  payloadIndexes?: Array<{ field: string; schema: "keyword" | "integer" | "float" | "bool" | "text" }>,
): Promise<void> {
  const collections = await qdrant.getCollections();
  const exists = collections.collections.some((c) => c.name === name);

  if (exists && forceFlag) {
    console.log(`  Deleting existing collection: ${name}`);
    await qdrant.deleteCollection(name);
  }

  if (!exists || forceFlag) {
    console.log(`  Creating collection: ${name} (${NEMOTRON_EMBEDDING_DIMENSIONS}d, cosine)`);
    await qdrant.createCollection(name, {
      vectors: {
        size: NEMOTRON_EMBEDDING_DIMENSIONS,
        distance: "Cosine",
      },
      optimizers_config: {
        indexing_threshold: 10000,
      },
    });

    if (payloadIndexes) {
      for (const idx of payloadIndexes) {
        await qdrant.createPayloadIndex(name, {
          field_name: idx.field,
          field_schema: idx.schema,
        });
      }
      console.log(`  Created ${payloadIndexes.length} payload indexes`);
    }
  } else {
    console.log(`  Collection already exists: ${name}`);
  }
}

async function getExistingIds(collection: string): Promise<Set<string>> {
  if (forceFlag) return new Set();

  const ids = new Set<string>();
  let offset: string | number | null = null;
  try {
    while (true) {
      const result = await qdrant.scroll(collection, {
        limit: 1000,
        offset: offset ?? undefined,
        with_payload: false,
        with_vector: false,
      });
      for (const point of result.points) {
        ids.add(String(point.id));
      }
      if (!result.next_page_offset) break;
      offset = result.next_page_offset as string;
    }
  } catch {
    // Collection might not exist yet
  }
  return ids;
}

// --- Quran embeddings ---

async function generateQuranEmbeddings(): Promise<void> {
  console.log("\n" + "=".repeat(60));
  console.log("QURAN AYAH EMBEDDINGS (Nemotron)");
  console.log("=".repeat(60));

  await ensureCollection(QDRANT_QURAN_NEMOTRON_COLLECTION, [
    { field: "surahNumber", schema: "integer" },
    { field: "ayahNumber", schema: "integer" },
  ]);

  const existingIds = await getExistingIds(QDRANT_QURAN_NEMOTRON_COLLECTION);
  console.log(`  Existing points: ${existingIds.size}`);

  const totalAyahs = await prisma.ayah.count();
  console.log(`  Total ayahs in DB: ${totalAyahs}`);

  if (totalAyahs === 0) {
    console.log("  No ayahs found. Run import-quran.ts first.");
    return;
  }

  // Load English translations (Mustafa Khattab)
  console.log("  Loading English translations (Mustafa Khattab)...");
  const allTranslations = await prisma.ayahTranslation.findMany({
    where: { editionId: "eng-mustafakhattaba" },
    select: { surahNumber: true, ayahNumber: true, text: true },
  });
  const translationMap = new Map<string, string>();
  for (const t of allTranslations) {
    translationMap.set(`${t.surahNumber}:${t.ayahNumber}`, t.text);
  }
  console.log(`  Loaded ${translationMap.size} translations`);

  let processed = 0;
  let skipped = 0;
  let batchesDone = 0;

  // Fetch all ayahs in pages
  let dbOffset = 0;
  const FETCH_SIZE = BATCH_SIZE * 10;

  while (dbOffset < totalAyahs) {
    const ayahs = await prisma.ayah.findMany({
      skip: dbOffset,
      take: FETCH_SIZE,
      orderBy: [{ surahId: "asc" }, { ayahNumber: "asc" }],
      select: {
        ayahNumber: true,
        textUthmani: true,
        textPlain: true,
        juzNumber: true,
        pageNumber: true,
        surah: { select: { number: true, nameArabic: true, nameEnglish: true } },
      },
    });

    if (ayahs.length === 0) break;

    const toProcess = ayahs.filter((a) => {
      const id = generateAyahPointId(a.surah.number, a.ayahNumber);
      if (existingIds.has(id)) {
        skipped++;
        return false;
      }
      return true;
    });

    // Process in batches
    for (let i = 0; i < toProcess.length; i += BATCH_SIZE) {
      const batch = toProcess.slice(i, i + BATCH_SIZE);

      const texts = batch.map((ayah) => {
        const metadata = `سورة ${ayah.surah.nameArabic}، آية ${ayah.ayahNumber}:`;
        const normalized = normalizeArabicText(ayah.textPlain);
        const parts = [metadata, normalized];
        const translation = translationMap.get(`${ayah.surah.number}:${ayah.ayahNumber}`);
        if (translation) {
          parts.push(` ||| ${translation}`);
        }
        return truncateForEmbedding(parts.join("\n"));
      });

      const embeddings = await generateNemotronEmbeddings(texts);

      const points = batch.map((ayah, idx) => ({
        id: generateAyahPointId(ayah.surah.number, ayah.ayahNumber),
        vector: embeddings[idx],
        payload: {
          surahNumber: ayah.surah.number,
          ayahNumber: ayah.ayahNumber,
          surahNameArabic: ayah.surah.nameArabic,
          surahNameEnglish: ayah.surah.nameEnglish,
          text: ayah.textUthmani,
          textPlain: ayah.textPlain,
          juzNumber: ayah.juzNumber,
          pageNumber: ayah.pageNumber,
        },
      }));

      await qdrant.upsert(QDRANT_QURAN_NEMOTRON_COLLECTION, { wait: true, points });
      processed += points.length;
      batchesDone++;

      if (batchesDone % 5 === 0) {
        console.log(`  Progress: ${processed} embedded, ${skipped} skipped (batch ${batchesDone})`);
      }
    }

    dbOffset += ayahs.length;
  }

  console.log(`  Done: ${processed} new, ${skipped} skipped`);
  try {
    const info = await qdrant.getCollection(QDRANT_QURAN_NEMOTRON_COLLECTION);
    console.log(`  Collection points: ${info.points_count}`);
  } catch {}
}

// --- Hadith embeddings (Bukhari only) ---

async function generateHadithEmbeddings(): Promise<void> {
  console.log("\n" + "=".repeat(60));
  console.log("HADITH EMBEDDINGS - BUKHARI ONLY (Nemotron)");
  console.log("=".repeat(60));

  await ensureCollection(QDRANT_HADITH_NEMOTRON_COLLECTION, [
    { field: "collectionSlug", schema: "keyword" },
    { field: "bookNumber", schema: "integer" },
    { field: "hadithNumber", schema: "keyword" },
  ]);

  const existingIds = await getExistingIds(QDRANT_HADITH_NEMOTRON_COLLECTION);
  console.log(`  Existing points: ${existingIds.size}`);

  // Only Bukhari
  const totalHadiths = await prisma.hadith.count({
    where: { book: { collection: { slug: "bukhari" } } },
  });
  console.log(`  Total Bukhari hadiths in DB: ${totalHadiths}`);

  if (totalHadiths === 0) {
    console.log("  No Bukhari hadiths found.");
    return;
  }

  let processed = 0;
  let skipped = 0;
  let batchesDone = 0;
  let dbOffset = 0;
  const FETCH_SIZE = BATCH_SIZE * 10;

  while (dbOffset < totalHadiths) {
    const hadiths = await prisma.hadith.findMany({
      where: { book: { collection: { slug: "bukhari" } } },
      skip: dbOffset,
      take: FETCH_SIZE,
      orderBy: [{ bookId: "asc" }, { hadithNumber: "asc" }],
      select: {
        hadithNumber: true,
        textArabic: true,
        textPlain: true,
        chapterArabic: true,
        chapterEnglish: true,
        sourceBookId: true,
        sourcePageStart: true,
        numberInCollection: true,
        book: {
          select: {
            bookNumber: true,
            nameArabic: true,
            nameEnglish: true,
            collection: {
              select: { slug: true, nameArabic: true, nameEnglish: true },
            },
          },
        },
      },
    });

    if (hadiths.length === 0) break;

    const toProcess = hadiths.filter((h) => {
      const id = generateHadithPointId(h.book.collection.slug, h.hadithNumber);
      if (existingIds.has(id)) {
        skipped++;
        return false;
      }
      return true;
    });

    for (let i = 0; i < toProcess.length; i += BATCH_SIZE) {
      const batch = toProcess.slice(i, i + BATCH_SIZE);

      const texts = batch.map((hadith) => {
        const metadataParts = [hadith.book.collection.nameArabic];
        if (hadith.chapterArabic) metadataParts.push(hadith.chapterArabic);
        const metadata = `${metadataParts.join("، ")}:`;
        const normalized = normalizeArabicText(hadith.textPlain);
        return truncateForEmbedding(`${metadata}\n${normalized}`);
      });

      const embeddings = await generateNemotronEmbeddings(texts);

      const points = batch.map((hadith, idx) => {
        const slug = hadith.book.collection.slug;
        return {
          id: generateHadithPointId(slug, hadith.hadithNumber),
          vector: embeddings[idx],
          payload: {
            collectionSlug: slug,
            collectionNameArabic: hadith.book.collection.nameArabic,
            collectionNameEnglish: hadith.book.collection.nameEnglish,
            bookNumber: hadith.book.bookNumber,
            bookNameArabic: hadith.book.nameArabic,
            bookNameEnglish: hadith.book.nameEnglish,
            hadithNumber: hadith.hadithNumber,
            text: hadith.textArabic,
            textPlain: hadith.textPlain,
            chapterArabic: hadith.chapterArabic,
            chapterEnglish: hadith.chapterEnglish,
            sourceUrl: generateHadithSourceUrl(
              slug,
              hadith.hadithNumber,
              hadith.book.bookNumber,
              hadith.numberInCollection,
              hadith.sourceBookId,
              hadith.sourcePageStart,
            ),
          },
        };
      });

      await qdrant.upsert(QDRANT_HADITH_NEMOTRON_COLLECTION, { wait: true, points });
      processed += points.length;
      batchesDone++;

      if (batchesDone % 5 === 0) {
        console.log(`  Progress: ${processed} embedded, ${skipped} skipped (batch ${batchesDone})`);
      }
    }

    dbOffset += hadiths.length;
  }

  console.log(`  Done: ${processed} new, ${skipped} skipped`);
  try {
    const info = await qdrant.getCollection(QDRANT_HADITH_NEMOTRON_COLLECTION);
    console.log(`  Collection points: ${info.points_count}`);
  } catch {}
}

// --- Main ---

async function main() {
  console.log("=== Nemotron Embedding Generation ===");
  console.log(`Dimensions: ${NEMOTRON_EMBEDDING_DIMENSIONS}`);
  console.log(`Batch size: ${BATCH_SIZE}`);
  console.log(`Force: ${forceFlag}`);
  console.log(`Collections: ${collectionFilter}`);
  console.log(`Note: Free tier rate limits (20 req/min) — be patient\n`);

  const toProcess =
    collectionFilter === "all" ? ["quran", "hadith"] : [collectionFilter];

  for (const key of toProcess) {
    if (key === "quran") await generateQuranEmbeddings();
    else if (key === "hadith") await generateHadithEmbeddings();
    else console.error(`Unknown collection: ${key}`);
  }

  console.log("\n=== Complete ===");
}

main()
  .catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
