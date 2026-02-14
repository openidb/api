/**
 * Generate Jina v3 Embeddings Script
 *
 * Populates separate Qdrant collections with Jina embeddings-v3 (1024d).
 * Reads from the database (same as the Gemini script) and constructs identical
 * embedding text: metadata + original Arabic + English translation (for Quran).
 *
 * Usage: bun run pipelines/embed/generate-jina-embeddings.ts [options]
 *
 * Options:
 *   --collection=quran|hadith|pages|all   Which collection(s) to process (default: all)
 *   --force                                Recreate collections even if they exist
 *   --batch-size=N                         Documents per embedding API call (default: 100)
 */

import "../env";
import { prisma } from "../../src/db";
import {
  qdrant,
  QDRANT_QURAN_JINA_COLLECTION,
  QDRANT_HADITH_JINA_COLLECTION,
  QDRANT_PAGES_JINA_COLLECTION,
} from "../../src/qdrant";
import { JINA_EMBEDDING_DIMENSIONS } from "../../src/constants";
import { generateJinaEmbeddings } from "../../src/embeddings/jina";
import { normalizeArabicText, truncateForEmbedding } from "../../src/embeddings/gemini";
import { generateSunnahUrl } from "../../src/utils/source-urls";
import crypto from "crypto";

const forceFlag = process.argv.includes("--force");
const collectionArg = process.argv.find((arg) => arg.startsWith("--collection="));
const collectionFilter = collectionArg ? collectionArg.split("=")[1] : "all";
const batchSizeArg = process.argv.find((arg) => arg.startsWith("--batch-size="));
const BATCH_SIZE = batchSizeArg ? parseInt(batchSizeArg.split("=")[1], 10) : 50;
const concurrencyArg = process.argv.find((arg) => arg.startsWith("--concurrency="));
const CONCURRENCY = concurrencyArg ? parseInt(concurrencyArg.split("=")[1], 10) : 3;

// --- Point ID generation (must match Gemini script for consistent IDs) ---

function generatePointId(bookId: string, pageNumber: number, volumeNumber: number): string {
  const input = `${bookId}_${pageNumber}_${volumeNumber}`;
  return crypto.createHash("md5").update(input).digest("hex");
}

function generateAyahPointId(surahNumber: number, ayahNumber: number): string {
  const input = `ayah_${surahNumber}_${ayahNumber}`;
  return crypto.createHash("md5").update(input).digest("hex");
}

function generateHadithPointId(collectionSlug: string, hadithNumber: string): string {
  const input = `hadith_${collectionSlug}_${hadithNumber}`;
  return crypto.createHash("md5").update(input).digest("hex");
}

// --- Concurrency helpers ---

async function processWithConcurrency<T>(
  items: T[],
  fn: (item: T) => Promise<void>,
  concurrency: number,
): Promise<void> {
  const queue = [...items];
  const running: Promise<void>[] = [];

  while (queue.length > 0 || running.length > 0) {
    while (running.length < concurrency && queue.length > 0) {
      const item = queue.shift()!;
      const p = fn(item).then(() => {
        running.splice(running.indexOf(p), 1);
      });
      running.push(p);
    }
    if (running.length > 0) {
      await Promise.race(running);
    }
  }
}

// --- Collection helpers ---

async function ensureCollection(name: string, payloadIndexes?: Array<{ field: string; schema: "keyword" | "integer" | "float" | "bool" | "text" }>): Promise<void> {
  const collections = await qdrant.getCollections();
  const exists = collections.collections.some((c) => c.name === name);

  if (exists && forceFlag) {
    console.log(`  Deleting existing collection: ${name}`);
    await qdrant.deleteCollection(name);
  }

  if (!exists || forceFlag) {
    console.log(`  Creating collection: ${name} (${JINA_EMBEDDING_DIMENSIONS}d, cosine)`);
    await qdrant.createCollection(name, {
      vectors: {
        size: JINA_EMBEDDING_DIMENSIONS,
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
// Format: سورة {nameArabic}، آية {ayahNumber}:\n{normalizedText}\n ||| {englishTranslation}

async function generateQuranEmbeddings(): Promise<void> {
  console.log("\n" + "=".repeat(60));
  console.log("QURAN AYAH EMBEDDINGS (Jina v3)");
  console.log("=".repeat(60));

  await ensureCollection(QDRANT_QURAN_JINA_COLLECTION, [
    { field: "surahNumber", schema: "integer" },
    { field: "ayahNumber", schema: "integer" },
  ]);

  const existingIds = await getExistingIds(QDRANT_QURAN_JINA_COLLECTION);
  console.log(`  Existing points: ${existingIds.size}`);

  const totalAyahs = await prisma.ayah.count();
  console.log(`  Total ayahs in DB: ${totalAyahs}`);

  if (totalAyahs === 0) {
    console.log("  No ayahs found. Run import-quran.ts first.");
    return;
  }

  // Load English translations (Mustafa Khattab) — same as Gemini script
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

  // Pre-fetch all ayahs and build batch queue
  type AyahBatch = Awaited<ReturnType<typeof prisma.ayah.findMany>>;
  const allBatches: AyahBatch[] = [];

  console.log(`  Fetching ayahs from DB and building batch queue...`);
  let dbOffset = 0;
  const FETCH_SIZE = BATCH_SIZE * CONCURRENCY * 2;

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
      if (existingIds.has(id)) { skipped++; return false; }
      return true;
    });

    for (let i = 0; i < toProcess.length; i += BATCH_SIZE) {
      allBatches.push(toProcess.slice(i, i + BATCH_SIZE));
    }

    dbOffset += ayahs.length;
  }

  console.log(`  ${allBatches.length} batches to process, ${skipped} already exist`);
  console.log(`  Concurrency: ${CONCURRENCY}`);

  let batchesDone = 0;
  await processWithConcurrency(
    allBatches,
    async (batch) => {
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

      const embeddings = await generateJinaEmbeddings(texts, "retrieval.passage");

      const points = batch.map((ayah, i) => ({
        id: generateAyahPointId(ayah.surah.number, ayah.ayahNumber),
        vector: embeddings[i],
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

      await qdrant.upsert(QDRANT_QURAN_JINA_COLLECTION, { wait: true, points });
      processed += points.length;
      batchesDone++;

      if (batchesDone % 10 === 0 || batchesDone === allBatches.length) {
        console.log(`  Progress: ${processed} embedded, ${skipped} skipped (batch ${batchesDone}/${allBatches.length})`);
      }
    },
    CONCURRENCY,
  );

  console.log(`  Done: ${processed} new, ${skipped} skipped`);
  try {
    const info = await qdrant.getCollection(QDRANT_QURAN_JINA_COLLECTION);
    console.log(`  Collection points: ${info.points_count}`);
  } catch {}
}

// --- Hadith embeddings ---
// Format: {collectionNameArabic}، {chapterArabic}:\n{normalizedArabicText}

async function generateHadithEmbeddings(): Promise<void> {
  console.log("\n" + "=".repeat(60));
  console.log("HADITH EMBEDDINGS (Jina v3)");
  console.log("=".repeat(60));

  await ensureCollection(QDRANT_HADITH_JINA_COLLECTION, [
    { field: "collectionSlug", schema: "keyword" },
    { field: "bookNumber", schema: "integer" },
    { field: "hadithNumber", schema: "keyword" },
  ]);

  const existingIds = await getExistingIds(QDRANT_HADITH_JINA_COLLECTION);
  console.log(`  Existing points: ${existingIds.size}`);

  const totalHadiths = await prisma.hadith.count();
  console.log(`  Total hadiths in DB: ${totalHadiths}`);

  if (totalHadiths === 0) {
    console.log("  No hadiths found. Run scrape-sunnah.ts first.");
    return;
  }

  let processed = 0;
  let skipped = 0;
  let fetched = 0;

  // Pre-fetch all hadiths from DB in pages, build batch queue
  type HadithBatch = typeof prisma.hadith extends { findMany: (...args: any) => Promise<infer R> } ? R : never;
  const allBatches: HadithBatch[] = [];

  console.log(`  Fetching hadiths from DB and building batch queue...`);
  let dbOffset = 0;
  const FETCH_SIZE = BATCH_SIZE * CONCURRENCY * 2; // fetch larger chunks from DB

  while (dbOffset < totalHadiths) {
    const hadiths = await prisma.hadith.findMany({
      skip: dbOffset,
      take: FETCH_SIZE,
      orderBy: [{ bookId: "asc" }, { hadithNumber: "asc" }],
      select: {
        hadithNumber: true,
        textArabic: true,
        textPlain: true,
        chapterArabic: true,
        chapterEnglish: true,
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

    // Filter already-processed and split into batches
    const toProcess = hadiths.filter((h) => {
      const id = generateHadithPointId(h.book.collection.slug, h.hadithNumber);
      if (existingIds.has(id)) { skipped++; return false; }
      return true;
    });

    // Split into BATCH_SIZE chunks
    for (let i = 0; i < toProcess.length; i += BATCH_SIZE) {
      allBatches.push(toProcess.slice(i, i + BATCH_SIZE));
    }

    dbOffset += hadiths.length;
    fetched += hadiths.length;
  }

  console.log(`  ${allBatches.length} batches to process (${fetched - skipped} hadiths), ${skipped} already exist`);
  console.log(`  Concurrency: ${CONCURRENCY}`);

  let batchesDone = 0;
  await processWithConcurrency(
    allBatches,
    async (batch) => {
      // Build embedding texts: metadata + Arabic (same as Gemini)
      const texts = batch.map((hadith) => {
        const slug = hadith.book.collection.slug;
        const metadataParts = [hadith.book.collection.nameArabic];
        if (hadith.chapterArabic) metadataParts.push(hadith.chapterArabic);
        const metadata = `${metadataParts.join("، ")}:`;

        const arabicText = slug === "hisn" && hadith.chapterArabic
          ? `${hadith.chapterArabic}، ${hadith.textPlain}`
          : hadith.textPlain;

        const normalized = normalizeArabicText(arabicText);
        return truncateForEmbedding(`${metadata}\n${normalized}`);
      });

      const embeddings = await generateJinaEmbeddings(texts, "retrieval.passage");

      const points = batch.map((hadith, i) => {
        const slug = hadith.book.collection.slug;
        const enrichedTextPlain = slug === "hisn" && hadith.chapterArabic
          ? `${hadith.chapterArabic}، ${hadith.textPlain}`
          : hadith.textPlain;

        return {
          id: generateHadithPointId(slug, hadith.hadithNumber),
          vector: embeddings[i],
          payload: {
            collectionSlug: slug,
            collectionNameArabic: hadith.book.collection.nameArabic,
            collectionNameEnglish: hadith.book.collection.nameEnglish,
            bookNumber: hadith.book.bookNumber,
            bookNameArabic: hadith.book.nameArabic,
            bookNameEnglish: hadith.book.nameEnglish,
            hadithNumber: hadith.hadithNumber,
            text: hadith.textArabic,
            textPlain: enrichedTextPlain,
            chapterArabic: hadith.chapterArabic,
            chapterEnglish: hadith.chapterEnglish,
            sunnahUrl: generateSunnahUrl(slug, hadith.hadithNumber, hadith.book.bookNumber),
            embeddingTechnique: "metadata",
          },
        };
      });

      await qdrant.upsert(QDRANT_HADITH_JINA_COLLECTION, { wait: true, points });
      processed += points.length;
      batchesDone++;

      if (batchesDone % 10 === 0 || batchesDone === allBatches.length) {
        console.log(`  Progress: ${processed} embedded, ${skipped} skipped (batch ${batchesDone}/${allBatches.length})`);
      }
    },
    CONCURRENCY,
  );

  console.log(`  Done: ${processed} new, ${skipped} skipped`);
  try {
    const info = await qdrant.getCollection(QDRANT_HADITH_JINA_COLLECTION);
    console.log(`  Collection points: ${info.points_count}`);
  } catch {}
}

// --- Page embeddings ---
// Format: {bookTitle}، {authorName}:\n{normalizedText}

async function generatePageEmbeddings(): Promise<void> {
  console.log("\n" + "=".repeat(60));
  console.log("BOOK PAGE EMBEDDINGS (Jina v3)");
  console.log("=".repeat(60));

  await ensureCollection(QDRANT_PAGES_JINA_COLLECTION, [
    { field: "bookId", schema: "integer" },
    { field: "volumeNumber", schema: "integer" },
  ]);

  const existingIds = await getExistingIds(QDRANT_PAGES_JINA_COLLECTION);
  console.log(`  Existing points: ${existingIds.size}`);

  const totalPages = await prisma.page.count();
  console.log(`  Total pages in DB: ${totalPages}`);

  if (totalPages === 0) {
    console.log("  No pages found. Run import pipeline first.");
    return;
  }

  let processed = 0;
  let skipped = 0;

  // For pages (potentially ~1.5M), stream in chunks rather than pre-fetch all
  console.log(`  Concurrency: ${CONCURRENCY}`);
  const PAGE_FETCH_SIZE = BATCH_SIZE * CONCURRENCY * 2;
  let dbOffset = 0;

  while (dbOffset < totalPages) {
    const pages = await prisma.page.findMany({
      skip: dbOffset,
      take: PAGE_FETCH_SIZE,
      orderBy: [{ bookId: "asc" }, { pageNumber: "asc" }],
      select: {
        bookId: true,
        pageNumber: true,
        volumeNumber: true,
        contentPlain: true,
        book: {
          select: {
            id: true,
            titleArabic: true,
            author: { select: { nameArabic: true } },
          },
        },
      },
    });

    if (pages.length === 0) break;

    const toProcess = pages.filter((p) => {
      const id = generatePointId(p.bookId, p.pageNumber, p.volumeNumber);
      if (existingIds.has(id)) { skipped++; return false; }
      return true;
    });

    // Split into BATCH_SIZE chunks
    const batches: typeof toProcess[] = [];
    for (let i = 0; i < toProcess.length; i += BATCH_SIZE) {
      batches.push(toProcess.slice(i, i + BATCH_SIZE));
    }

    await processWithConcurrency(
      batches,
      async (batch) => {
        const texts = batch.map((page) => {
          const metadata = `${page.book.titleArabic}، ${page.book.author.nameArabic}:`;
          const normalized = normalizeArabicText(page.contentPlain);
          return truncateForEmbedding(`${metadata}\n${normalized}`);
        });

        const embeddings = await generateJinaEmbeddings(texts, "retrieval.passage");

        const points = batch.map((page, i) => ({
          id: generatePointId(page.bookId, page.pageNumber, page.volumeNumber),
          vector: embeddings[i],
          payload: {
            bookId: page.bookId,
            pageNumber: page.pageNumber,
            volumeNumber: page.volumeNumber,
            bookTitle: page.book.titleArabic,
            authorName: page.book.author.nameArabic,
            textSnippet: page.contentPlain.slice(0, 200),
            embeddingTechnique: "metadata",
          },
        }));

        await qdrant.upsert(QDRANT_PAGES_JINA_COLLECTION, { wait: true, points });
        processed += points.length;
      },
      CONCURRENCY,
    );

    dbOffset += pages.length;
    console.log(`  Progress: ${processed} embedded, ${skipped} skipped, ${dbOffset}/${totalPages} scrolled`);
  }

  console.log(`  Done: ${processed} new, ${skipped} skipped`);
  try {
    const info = await qdrant.getCollection(QDRANT_PAGES_JINA_COLLECTION);
    console.log(`  Collection points: ${info.points_count}`);
  } catch {}
}

// --- Main ---

async function main() {
  console.log("=== Jina v3 Embedding Generation ===");
  console.log(`Dimensions: ${JINA_EMBEDDING_DIMENSIONS}`);
  console.log(`Batch size: ${BATCH_SIZE}`);
  console.log(`Concurrency: ${CONCURRENCY}`);
  console.log(`Force: ${forceFlag}`);
  console.log(`Collections: ${collectionFilter}`);

  const toProcess = collectionFilter === "all"
    ? ["quran", "hadith", "pages"]
    : [collectionFilter];

  for (const key of toProcess) {
    if (key === "quran") await generateQuranEmbeddings();
    else if (key === "hadith") await generateHadithEmbeddings();
    else if (key === "pages") await generatePageEmbeddings();
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
