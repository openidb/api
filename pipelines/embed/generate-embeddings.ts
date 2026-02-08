/**
 * Generate Embeddings Script
 *
 * Generates embeddings for all pages in the database and stores them in Qdrant.
 *
 * Collection-specific enrichment:
 *   - Hisn al-Muslim: Prepends chapter title to text for better semantic search
 *     (the dua texts are very short and need chapter context)
 *
 * Usage: bun run scripts/generate-embeddings.ts [options]
 *
 * Options:
 *   --force                    Re-generate embeddings even if they already exist
 *   --batch-size=N             Number of pages to process in each batch (default: 50)
 *   --pages-only               Only process book pages, skip Quran and Hadith embeddings
 *   --hadiths-only             Only process hadiths, skip pages and quran
 *   --hadith-collections=a,b   Only process specific hadith collections (comma-separated slugs)
 *   --model=gemini|bge-m3      Embedding model to use (default: gemini)
 *
 * Examples:
 *   bun run scripts/generate-embeddings.ts --hadiths-only --hadith-collections=hisn --force
 *   bun run scripts/generate-embeddings.ts --hadith-collections=bukhari,muslim
 *   bun run scripts/generate-embeddings.ts --model=bge-m3 --force
 */

import "../env";
import { prisma } from "../../src/db";
import {
  qdrant,
  PAGES_COLLECTION as PAGES_COLLECTION_GEMINI,
  QDRANT_QURAN_COLLECTION,
  HADITHS_COLLECTION as HADITHS_COLLECTION_GEMINI,
  PAGES_COLLECTION_BGE,
  HADITHS_COLLECTION_BGE,
  GEMINI_DIMENSIONS,
  BGE_DIMENSIONS,
} from "../../src/qdrant";
import {
  generateEmbeddings,
  normalizeArabicText,
  truncateForEmbedding,
  type EmbeddingModel,
} from "../../src/embeddings";
import crypto from "crypto";
import { generateSunnahUrl } from "../../src/utils/source-urls";

// Parse command line arguments
const forceFlag = process.argv.includes("--force");
const pagesOnlyFlag = process.argv.includes("--pages-only");
const hadithsOnlyFlag = process.argv.includes("--hadiths-only");
const batchSizeArg = process.argv.find((arg) => arg.startsWith("--batch-size="));
const BATCH_SIZE = batchSizeArg
  ? parseInt(batchSizeArg.split("=")[1], 10)
  : 50;

// Parse --hadith-collections filter (comma-separated list of collection slugs)
const hadithCollectionsArg = process.argv.find((arg) => arg.startsWith("--hadith-collections="));
const hadithCollectionsFilter: string[] | null = hadithCollectionsArg
  ? hadithCollectionsArg.split("=")[1].split(",").map((s) => s.trim())
  : null;

// Parse --model flag (gemini or bge-m3)
const modelArg = process.argv.find((arg) => arg.startsWith("--model="));
const embeddingModel: EmbeddingModel = modelArg?.split("=")[1] === "bge-m3" ? "bge-m3" : "gemini";
const EMBEDDING_DIMENSIONS = embeddingModel === "bge-m3" ? BGE_DIMENSIONS : GEMINI_DIMENSIONS;

// Determine collections based on model
const PAGES_COLLECTION = embeddingModel === "bge-m3" ? PAGES_COLLECTION_BGE : PAGES_COLLECTION_GEMINI;
const HADITHS_COLLECTION = embeddingModel === "bge-m3" ? HADITHS_COLLECTION_BGE : HADITHS_COLLECTION_GEMINI;

console.log(`Using embedding model: ${embeddingModel} (${EMBEDDING_DIMENSIONS} dimensions)`);
console.log(`Pages collection: ${PAGES_COLLECTION}`);
console.log(`Hadiths collection: ${HADITHS_COLLECTION}`);

/**
 * Generate a deterministic point ID from book and page identifiers
 */
function generatePointId(
  bookId: string,  // Now string (shamela book ID is primary key)
  pageNumber: number,
  volumeNumber: number
): string {
  const input = `${bookId}_${pageNumber}_${volumeNumber}`;
  return crypto.createHash("md5").update(input).digest("hex");
}

/**
 * Generate a deterministic point ID for ayahs
 */
function generateAyahPointId(surahNumber: number, ayahNumber: number): string {
  const input = `ayah_${surahNumber}_${ayahNumber}`;
  return crypto.createHash("md5").update(input).digest("hex");
}

/**
 * Generate a deterministic point ID for hadiths.
 * Uses collection slug + hadith number to deduplicate narration variations
 * (different chains for the same hadith).
 */
function generateHadithPointId(collectionSlug: string, hadithNumber: string): string {
  const input = `hadith_${collectionSlug}_${hadithNumber}`;
  return crypto.createHash("md5").update(input).digest("hex");
}

/**
 * Initialize Qdrant collection if it doesn't exist
 */
async function initializeCollection(): Promise<void> {
  try {
    const collections = await qdrant.getCollections();
    const exists = collections.collections.some(
      (c) => c.name === PAGES_COLLECTION
    );

    if (exists && forceFlag) {
      console.log(`Deleting existing collection: ${PAGES_COLLECTION}`);
      await qdrant.deleteCollection(PAGES_COLLECTION);
    }

    if (!exists || forceFlag) {
      console.log(`Creating collection: ${PAGES_COLLECTION}`);
      await qdrant.createCollection(PAGES_COLLECTION, {
        vectors: {
          size: EMBEDDING_DIMENSIONS,
          distance: "Cosine",
        },
        optimizers_config: {
          indexing_threshold: 10000,
        },
      });

      // Create payload indexes for filtering
      await qdrant.createPayloadIndex(PAGES_COLLECTION, {
        field_name: "bookId",
        field_schema: "integer",
      });
      await qdrant.createPayloadIndex(PAGES_COLLECTION, {
        field_name: "volumeNumber",
        field_schema: "integer",
      });

      console.log("Collection created with payload indexes\n");
    } else {
      console.log(`Collection already exists: ${PAGES_COLLECTION}\n`);
    }
  } catch (error) {
    console.error("Error initializing collection:", error);
    throw error;
  }
}

/**
 * Get existing point IDs from Qdrant to skip already processed pages
 */
async function getExistingPointIds(): Promise<Set<string>> {
  if (forceFlag) {
    return new Set();
  }

  try {
    const existingIds = new Set<string>();
    let offset: string | null = null;

    // Scroll through all points to get their IDs
    while (true) {
      const result = await qdrant.scroll(PAGES_COLLECTION, {
        limit: 1000,
        offset: offset ?? undefined,
        with_payload: false,
        with_vector: false,
      });

      for (const point of result.points) {
        existingIds.add(point.id as string);
      }

      if (!result.next_page_offset) {
        break;
      }
      offset = result.next_page_offset as string;
    }

    return existingIds;
  } catch {
    // Collection might be empty or not exist
    return new Set();
  }
}

/**
 * Process a batch of pages: generate embeddings and upsert to Qdrant
 */
async function processBatch(
  pages: Array<{
    id: number;
    bookId: string;
    pageNumber: number;
    volumeNumber: number;
    contentPlain: string;
    book: {
      id: string;
      titleArabic: string;
      author: { nameArabic: string };
    };
  }>
): Promise<number> {
  // Prepare texts for embedding with metadata prefix (book title, author)
  const texts = pages.map((page) => {
    const metadata = `${page.book.titleArabic}، ${page.book.author.nameArabic}:`;
    const normalized = normalizeArabicText(page.contentPlain);
    return truncateForEmbedding(`${metadata}\n${normalized}`);
  });

  // Generate embeddings in batch
  const embeddings = await generateEmbeddings(texts, embeddingModel);

  // Prepare points for Qdrant
  const points = pages.map((page, index) => ({
    id: generatePointId(page.bookId, page.pageNumber, page.volumeNumber),
    vector: embeddings[index],
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

  // Upsert to Qdrant
  await qdrant.upsert(PAGES_COLLECTION, {
    wait: true,
    points,
  });

  return points.length;
}

/**
 * Initialize Quran collection if it doesn't exist
 */
async function initializeQuranCollection(): Promise<void> {
  try {
    const collections = await qdrant.getCollections();
    const exists = collections.collections.some(
      (c) => c.name === QDRANT_QURAN_COLLECTION
    );

    if (exists && forceFlag) {
      console.log(`Deleting existing collection: ${QDRANT_QURAN_COLLECTION}`);
      await qdrant.deleteCollection(QDRANT_QURAN_COLLECTION);
    }

    if (!exists || forceFlag) {
      console.log(`Creating collection: ${QDRANT_QURAN_COLLECTION}`);
      await qdrant.createCollection(QDRANT_QURAN_COLLECTION, {
        vectors: {
          size: EMBEDDING_DIMENSIONS,
          distance: "Cosine",
        },
        optimizers_config: {
          indexing_threshold: 10000,
        },
      });

      // Create payload indexes for filtering
      await qdrant.createPayloadIndex(QDRANT_QURAN_COLLECTION, {
        field_name: "surahNumber",
        field_schema: "integer",
      });
      await qdrant.createPayloadIndex(QDRANT_QURAN_COLLECTION, {
        field_name: "ayahNumber",
        field_schema: "integer",
      });

      console.log("Quran collection created with payload indexes\n");
    } else {
      console.log(`Collection already exists: ${QDRANT_QURAN_COLLECTION}\n`);
    }
  } catch (error) {
    console.error("Error initializing Quran collection:", error);
    throw error;
  }
}

/**
 * Get existing Quran point IDs from Qdrant to skip already processed ayahs
 */
async function getExistingQuranPointIds(): Promise<Set<string>> {
  if (forceFlag) {
    return new Set();
  }

  try {
    const existingIds = new Set<string>();
    let offset: string | null = null;

    while (true) {
      const result = await qdrant.scroll(QDRANT_QURAN_COLLECTION, {
        limit: 1000,
        offset: offset ?? undefined,
        with_payload: false,
        with_vector: false,
      });

      for (const point of result.points) {
        existingIds.add(point.id as string);
      }

      if (!result.next_page_offset) {
        break;
      }
      offset = result.next_page_offset as string;
    }

    return existingIds;
  } catch {
    // Collection might be empty or not exist
    return new Set();
  }
}

/**
 * Process a batch of ayahs: generate embeddings and upsert to Qdrant
 */
async function processAyahBatch(
  ayahs: Array<{
    id: number;
    ayahNumber: number;
    textUthmani: string;
    textPlain: string;
    juzNumber: number;
    pageNumber: number;
    surah: {
      number: number;
      nameArabic: string;
      nameEnglish: string;
    };
  }>
): Promise<number> {
  // Prepare texts for embedding - use plain text for better semantic matching
  const texts = ayahs.map((ayah) => {
    const normalized = normalizeArabicText(ayah.textPlain);
    return truncateForEmbedding(normalized);
  });

  // Generate embeddings in batch
  const embeddings = await generateEmbeddings(texts, embeddingModel);

  // Prepare points for Qdrant
  const points = ayahs.map((ayah, index) => ({
    id: generateAyahPointId(ayah.surah.number, ayah.ayahNumber),
    vector: embeddings[index],
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

  // Upsert to Qdrant
  await qdrant.upsert(QDRANT_QURAN_COLLECTION, {
    wait: true,
    points,
  });

  return points.length;
}

/**
 * Generate embeddings for Quran ayahs
 */
async function generateQuranEmbeddings(): Promise<void> {
  console.log("\n" + "=".repeat(60));
  console.log("QURAN AYAH EMBEDDINGS");
  console.log("=".repeat(60));

  // Initialize Quran collection
  await initializeQuranCollection();

  // Get existing point IDs to skip
  console.log("Checking for existing ayah embeddings...");
  const existingIds = await getExistingQuranPointIds();
  console.log(`Found ${existingIds.size} existing ayah embeddings\n`);

  // Get total ayah count
  const totalAyahs = await prisma.ayah.count();
  console.log(`Total ayahs in database: ${totalAyahs}\n`);

  if (totalAyahs === 0) {
    console.log("No ayahs found in database. Run import-quran.ts first.");
    return;
  }

  let processed = 0;
  let skipped = 0;
  let failed = 0;
  let offset = 0;

  while (offset < totalAyahs) {
    // Fetch batch of ayahs with surah info
    const ayahs = await prisma.ayah.findMany({
      skip: offset,
      take: BATCH_SIZE,
      orderBy: [{ surahId: "asc" }, { ayahNumber: "asc" }],
      select: {
        id: true,
        ayahNumber: true,
        textUthmani: true,
        textPlain: true,
        juzNumber: true,
        pageNumber: true,
        surah: {
          select: {
            number: true,
            nameArabic: true,
            nameEnglish: true,
          },
        },
      },
    });

    if (ayahs.length === 0) break;

    // Filter out already processed ayahs
    const ayahsToProcess = ayahs.filter((ayah) => {
      const pointId = generateAyahPointId(ayah.surah.number, ayah.ayahNumber);
      if (existingIds.has(pointId)) {
        skipped++;
        return false;
      }
      return true;
    });

    if (ayahsToProcess.length > 0) {
      try {
        const count = await processAyahBatch(ayahsToProcess);
        processed += count;
        console.log(
          `Processed ${processed}/${totalAyahs} ayahs (skipped: ${skipped}, failed: ${failed})`
        );
      } catch (error) {
        console.error(`Batch failed:`, error);
        failed += ayahsToProcess.length;
      }
    }

    offset += ayahs.length;

    // Rate limiting: pause briefly between batches
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  console.log("\n" + "=".repeat(60));
  console.log("QURAN EMBEDDING SUMMARY");
  console.log("=".repeat(60));
  console.log(`Processed: ${processed}`);
  console.log(`Skipped:   ${skipped}`);
  console.log(`Failed:    ${failed}`);
  console.log("=".repeat(60));

  // Verify collection
  try {
    const info = await qdrant.getCollection(QDRANT_QURAN_COLLECTION);
    console.log(`\nQuran collection points: ${info.points_count}`);
  } catch (error) {
    console.error("Could not get Quran collection info:", error);
  }
}

/**
 * Initialize Hadith collection if it doesn't exist
 */
async function initializeHadithCollection(): Promise<void> {
  try {
    const collections = await qdrant.getCollections();
    const exists = collections.collections.some(
      (c) => c.name === HADITHS_COLLECTION
    );

    // Only delete entire collection if --force is used WITHOUT --hadith-collections filter
    // When filtering by collection, we just update those specific hadiths
    if (exists && forceFlag && !hadithCollectionsFilter) {
      console.log(`Deleting existing collection: ${HADITHS_COLLECTION}`);
      await qdrant.deleteCollection(HADITHS_COLLECTION);
    }

    if (!exists || (forceFlag && !hadithCollectionsFilter)) {
      console.log(`Creating collection: ${HADITHS_COLLECTION}`);
      await qdrant.createCollection(HADITHS_COLLECTION, {
        vectors: {
          size: EMBEDDING_DIMENSIONS,
          distance: "Cosine",
        },
        optimizers_config: {
          indexing_threshold: 10000,
        },
      });

      // Create payload indexes for filtering
      await qdrant.createPayloadIndex(HADITHS_COLLECTION, {
        field_name: "collectionSlug",
        field_schema: "keyword",
      });
      await qdrant.createPayloadIndex(HADITHS_COLLECTION, {
        field_name: "bookNumber",
        field_schema: "integer",
      });
      await qdrant.createPayloadIndex(HADITHS_COLLECTION, {
        field_name: "hadithNumber",
        field_schema: "keyword",
      });

      console.log("Hadith collection created with payload indexes\n");
    } else {
      console.log(`Collection already exists: ${HADITHS_COLLECTION}\n`);
    }
  } catch (error) {
    console.error("Error initializing Hadith collection:", error);
    throw error;
  }
}

/**
 * Get existing Hadith point IDs from Qdrant to skip already processed hadiths
 */
async function getExistingHadithPointIds(): Promise<Set<string>> {
  // When using --force with or without filter, we want to regenerate
  if (forceFlag) {
    return new Set();
  }

  try {
    const existingIds = new Set<string>();
    let offset: string | null = null;

    while (true) {
      const result = await qdrant.scroll(HADITHS_COLLECTION, {
        limit: 1000,
        offset: offset ?? undefined,
        with_payload: false,
        with_vector: false,
      });

      for (const point of result.points) {
        existingIds.add(point.id as string);
      }

      if (!result.next_page_offset) {
        break;
      }
      offset = result.next_page_offset as string;
    }

    return existingIds;
  } catch {
    // Collection might be empty or not exist
    return new Set();
  }
}

/**
 * Get text for embedding with metadata prefix and optional translation.
 *
 * Format:
 *   ${collectionNameArabic}، ${chapterArabic}:
 *   ${normalizedArabicText}
 *    ||| ${englishTranslation}          (only if available)
 *
 * For Hisn al-Muslim: also includes chapter in display text.
 */
function getHadithTextForEmbedding(
  collectionNameArabic: string,
  slug: string,
  textPlain: string,
  chapterArabic: string | null,
  translationText: string | null
): string {
  // Build metadata prefix
  const metadataParts = [collectionNameArabic];
  if (chapterArabic) {
    metadataParts.push(chapterArabic);
  }
  const metadata = `${metadataParts.join("، ")}:`;

  // For Hisn al-Muslim: also prepend chapter to Arabic text (short duas need context)
  const arabicText = slug === "hisn" && chapterArabic
    ? `${chapterArabic}، ${textPlain}`
    : textPlain;

  const normalized = normalizeArabicText(arabicText);
  const parts = [metadata, normalized];

  if (translationText) {
    parts.push(` ||| ${translationText}`);
  }

  return parts.join("\n");
}

/**
 * Process a batch of hadiths: generate embeddings and upsert to Qdrant
 */
async function processHadithBatch(
  hadiths: Array<{
    id: number;
    hadithNumber: string;
    textArabic: string;
    textPlain: string;
    chapterArabic: string | null;
    chapterEnglish: string | null;
    translationText: string | null;
    book: {
      bookNumber: number;
      nameArabic: string;
      nameEnglish: string;
      collection: {
        slug: string;
        nameArabic: string;
        nameEnglish: string;
      };
    };
  }>
): Promise<number> {
  // Prepare texts for embedding with metadata + Arabic + translation
  const texts = hadiths.map((hadith) => {
    const text = getHadithTextForEmbedding(
      hadith.book.collection.nameArabic,
      hadith.book.collection.slug,
      hadith.textPlain,
      hadith.chapterArabic,
      hadith.translationText
    );
    return truncateForEmbedding(text);
  });

  // Generate embeddings in batch
  const embeddings = await generateEmbeddings(texts, embeddingModel);

  // Prepare points for Qdrant
  const points = hadiths.map((hadith, index) => {
    const slug = hadith.book.collection.slug;

    // For Hisn al-Muslim, enrich the displayed text with chapter context
    const enrichedTextPlain = slug === "hisn" && hadith.chapterArabic
      ? `${hadith.chapterArabic}، ${hadith.textPlain}`
      : hadith.textPlain;

    return {
      id: generateHadithPointId(slug, hadith.hadithNumber),
      vector: embeddings[index],
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
        embeddingTechnique: "metadata-translation",
      },
    };
  });

  // Upsert to Qdrant
  await qdrant.upsert(HADITHS_COLLECTION, {
    wait: true,
    points,
  });

  return points.length;
}

/**
 * Generate embeddings for Hadith
 */
async function generateHadithEmbeddings(): Promise<void> {
  console.log("\n" + "=".repeat(60));
  console.log("HADITH EMBEDDINGS");
  console.log("=".repeat(60));
  if (hadithCollectionsFilter) {
    console.log(`Filter: ${hadithCollectionsFilter.join(", ")}`);
  }

  // Initialize Hadith collection
  await initializeHadithCollection();

  // Get existing point IDs to skip
  console.log("Checking for existing hadith embeddings...");
  const existingIds = await getExistingHadithPointIds();
  console.log(`Found ${existingIds.size} existing hadith embeddings\n`);

  // Build where clause for collections filter
  const whereClause = hadithCollectionsFilter
    ? { book: { collection: { slug: { in: hadithCollectionsFilter } } } }
    : {};

  // Get total hadith count
  const totalHadiths = await prisma.hadith.count({ where: whereClause });
  console.log(`Total hadiths to process: ${totalHadiths}\n`);

  if (totalHadiths === 0) {
    console.log("No hadiths found. Check collection filter or run scrape-sunnah.ts first.");
    return;
  }

  // Pre-fetch all English translations for hadiths into a map (keyed by bookId:hadithNumber)
  console.log("Loading English translations for hadiths...");
  const allTranslations = await prisma.hadithTranslation.findMany({
    where: { language: "en" },
    select: { bookId: true, hadithNumber: true, text: true },
  });
  const translationMap = new Map<string, string>();
  for (const t of allTranslations) {
    translationMap.set(`${t.bookId}:${t.hadithNumber}`, t.text);
  }
  console.log(`Loaded ${translationMap.size} English hadith translations\n`);

  let processed = 0;
  let skipped = 0;
  let failed = 0;
  let withTranslation = 0;
  let withoutTranslation = 0;
  let offset = 0;

  while (offset < totalHadiths) {
    // Fetch batch of hadiths with book and collection info
    const hadiths = await prisma.hadith.findMany({
      where: whereClause,
      skip: offset,
      take: BATCH_SIZE,
      orderBy: [{ bookId: "asc" }, { hadithNumber: "asc" }],
      select: {
        id: true,
        bookId: true,
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
              select: {
                slug: true,
                nameArabic: true,
                nameEnglish: true,
              },
            },
          },
        },
      },
    });

    if (hadiths.length === 0) break;

    // Attach translations to hadiths
    const hadithsWithTranslations = hadiths.map((hadith) => {
      const translation = translationMap.get(`${hadith.bookId}:${hadith.hadithNumber}`) || null;
      if (translation) withTranslation++;
      else withoutTranslation++;
      return { ...hadith, translationText: translation };
    });

    // Filter out already processed hadiths
    const hadithsToProcess = hadithsWithTranslations.filter((hadith) => {
      const pointId = generateHadithPointId(hadith.book.collection.slug, hadith.hadithNumber);
      if (existingIds.has(pointId)) {
        skipped++;
        return false;
      }
      return true;
    });

    if (hadithsToProcess.length > 0) {
      try {
        const count = await processHadithBatch(hadithsToProcess);
        processed += count;
        console.log(
          `Processed ${processed}/${totalHadiths} hadiths (skipped: ${skipped}, failed: ${failed})`
        );
      } catch (error) {
        console.error(`Batch failed:`, error);
        failed += hadithsToProcess.length;
      }
    }

    offset += hadiths.length;

    // Rate limiting: pause briefly between batches
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  console.log("\n" + "=".repeat(60));
  console.log("HADITH EMBEDDING SUMMARY");
  console.log("=".repeat(60));
  console.log(`Processed:           ${processed}`);
  console.log(`Skipped:             ${skipped}`);
  console.log(`With translation:    ${withTranslation}`);
  console.log(`Without translation: ${withoutTranslation}`);
  console.log(`Failed:              ${failed}`);
  console.log("=".repeat(60));

  // Verify collection
  try {
    const info = await qdrant.getCollection(HADITHS_COLLECTION);
    console.log(`\nHadith collection points: ${info.points_count}`);
  } catch (error) {
    console.error("Could not get Hadith collection info:", error);
  }
}

async function main() {
  console.log("Embedding Generation");
  console.log("=".repeat(60));
  console.log(`Collection: ${PAGES_COLLECTION}`);
  console.log(`Quran Collection: ${QDRANT_QURAN_COLLECTION}`);
  console.log(`Hadith Collection: ${HADITHS_COLLECTION}`);
  console.log(`Batch size: ${BATCH_SIZE}`);
  const modeDesc = forceFlag
    ? "Force regenerate all"
    : pagesOnlyFlag
    ? "Pages only (skip Quran/Hadith)"
    : hadithsOnlyFlag
    ? "Hadiths only"
    : "Skip existing";
  console.log(`Mode: ${modeDesc}`);
  console.log();

  if (!hadithsOnlyFlag) {
    // Initialize Qdrant collection
    await initializeCollection();

    // Get existing point IDs to skip
    console.log("Checking for existing embeddings...");
    const existingIds = await getExistingPointIds();
    console.log(`Found ${existingIds.size} existing embeddings\n`);

    // Get total page count
    const totalPages = await prisma.page.count();
    console.log(`Total pages in database: ${totalPages}\n`);

    let processed = 0;
    let skipped = 0;
    let failed = 0;
    let offset = 0;

    while (offset < totalPages) {
      // Fetch batch of pages with book info
      const pages = await prisma.page.findMany({
        skip: offset,
        take: BATCH_SIZE,
        orderBy: [{ bookId: "asc" }, { pageNumber: "asc" }],
        select: {
          id: true,
          bookId: true,
          pageNumber: true,
          volumeNumber: true,
          contentPlain: true,
          book: {
            select: {
              id: true,
              titleArabic: true,
              author: {
                select: { nameArabic: true },
              },
            },
          },
        },
      });

      if (pages.length === 0) break;

      // Filter out already processed pages
      const pagesToProcess = pages.filter((page) => {
        const pointId = generatePointId(
          page.bookId,
          page.pageNumber,
          page.volumeNumber
        );
        if (existingIds.has(pointId)) {
          skipped++;
          return false;
        }
        return true;
      });

      if (pagesToProcess.length > 0) {
        try {
          const count = await processBatch(pagesToProcess);
          processed += count;
          console.log(
            `Processed ${processed}/${totalPages} pages (skipped: ${skipped}, failed: ${failed})`
          );
        } catch (error) {
          console.error(`Batch failed:`, error);
          failed += pagesToProcess.length;
        }
      }

      offset += pages.length;

      // Rate limiting: pause briefly between batches
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    console.log("\n" + "=".repeat(60));
    console.log("EMBEDDING SUMMARY");
    console.log("=".repeat(60));
    console.log(`Processed: ${processed}`);
    console.log(`Skipped:   ${skipped}`);
    console.log(`Failed:    ${failed}`);
    console.log("=".repeat(60));

    // Verify collection
    try {
      const info = await qdrant.getCollection(PAGES_COLLECTION);
      console.log(`\nCollection points: ${info.points_count}`);
      console.log(`Vectors size: ${info.config.params.vectors}`);
    } catch (error) {
      console.error("Could not get collection info:", error);
    }

    console.log("\nPage embedding generation completed!");

    // Skip Quran/Hadith if --pages-only flag is set
    if (pagesOnlyFlag) {
      console.log("\nSkipping Quran and Hadith embeddings (--pages-only mode)");
      return;
    }

    // Generate Quran ayah embeddings
    await generateQuranEmbeddings();
  } else {
    console.log("Skipping pages and Quran embeddings (--hadiths-only mode)\n");
  }

  // Generate Hadith embeddings (skip if --pages-only)
  if (!pagesOnlyFlag) {
    await generateHadithEmbeddings();
  }

  console.log("\nAll embedding generation completed!");
}

main()
  .catch((e) => {
    console.error("\nEmbedding generation failed:");
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
