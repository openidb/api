/**
 * Generate Metadata+Translation Enriched Quran Embeddings
 *
 * Creates embeddings for Quran ayahs using:
 *   1. Metadata prefix (surah name + ayah number)
 *   2. Normalized Arabic text
 *   3. English translation (Dr. Mustafa Khattab)
 *
 * Format: "سورة البقرة، آية 255:\nالله لا اله الا هو الحي القيوم\n ||| Allah! There is no god but He..."
 *
 * Stores embeddings in a separate Qdrant collection while preserving
 * original ayah text in payload for display.
 *
 * Usage: bun run scripts/generate-quran-enriched-embeddings.ts [--force] [--batch-size=50] [--model=gemini|bge-m3]
 *
 * Options:
 *   --force          Re-generate embeddings even if they already exist
 *   --batch-size=N   Number of ayahs to process in each batch (default: 50)
 *   --model=gemini|bge-m3   Embedding model to use (default: gemini)
 */

import "../env";
import { prisma } from "../../src/db";
import {
  qdrant,
  QDRANT_QURAN_COLLECTION,
  QDRANT_QURAN_COLLECTION_BGE,
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

// Parse command line arguments
const forceFlag = process.argv.includes("--force");
const batchSizeArg = process.argv.find((arg) => arg.startsWith("--batch-size="));
const BATCH_SIZE = batchSizeArg
  ? parseInt(batchSizeArg.split("=")[1], 10)
  : 50;

// Parse --model flag (gemini or bge-m3)
const modelArg = process.argv.find((arg) => arg.startsWith("--model="));
const embeddingModel: EmbeddingModel = modelArg?.split("=")[1] === "bge-m3" ? "bge-m3" : "gemini";
const EMBEDDING_DIMENSIONS = embeddingModel === "bge-m3" ? BGE_DIMENSIONS : GEMINI_DIMENSIONS;

// Determine collection based on model
const QURAN_COLLECTION = embeddingModel === "bge-m3" ? QDRANT_QURAN_COLLECTION_BGE : QDRANT_QURAN_COLLECTION;

console.log(`Using embedding model: ${embeddingModel} (${EMBEDDING_DIMENSIONS} dimensions)`);
console.log(`Collection: ${QURAN_COLLECTION}`);

/**
 * Generate a deterministic point ID for enriched ayahs
 */
function generateEnrichedAyahPointId(
  surahNumber: number,
  ayahNumber: number
): string {
  const input = `ayah_enriched_${surahNumber}_${ayahNumber}`;
  return crypto.createHash("md5").update(input).digest("hex");
}

/**
 * Initialize the enriched Quran collection if it doesn't exist
 */
async function initializeCollection(): Promise<void> {
  try {
    const collections = await qdrant.getCollections();
    const exists = collections.collections.some(
      (c) => c.name === QURAN_COLLECTION
    );

    if (exists && forceFlag) {
      console.log(`Deleting existing collection: ${QURAN_COLLECTION}`);
      await qdrant.deleteCollection(QURAN_COLLECTION);
    }

    if (!exists || forceFlag) {
      console.log(`Creating collection: ${QURAN_COLLECTION}`);
      await qdrant.createCollection(QURAN_COLLECTION, {
        vectors: {
          size: EMBEDDING_DIMENSIONS,
          distance: "Cosine",
        },
        optimizers_config: {
          indexing_threshold: 10000,
        },
      });

      // Create payload indexes for filtering
      await qdrant.createPayloadIndex(QURAN_COLLECTION, {
        field_name: "surahNumber",
        field_schema: "integer",
      });
      await qdrant.createPayloadIndex(QURAN_COLLECTION, {
        field_name: "ayahNumber",
        field_schema: "integer",
      });

      console.log("Enriched Quran collection created with payload indexes\n");
    } else {
      console.log(`Collection already exists: ${QURAN_COLLECTION}\n`);
    }
  } catch (error) {
    console.error("Error initializing collection:", error);
    throw error;
  }
}

/**
 * Get existing point IDs from Qdrant to skip already processed ayahs
 */
async function getExistingPointIds(): Promise<Set<string>> {
  if (forceFlag) {
    return new Set();
  }

  try {
    const existingIds = new Set<string>();
    let offset: string | null = null;

    while (true) {
      const result = await qdrant.scroll(QURAN_COLLECTION, {
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

interface AyahWithTranslation {
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
  translationText: string | null;
}

/**
 * Fetch ayahs with their English translations
 */
async function fetchAyahsWithTranslations(
  skip: number,
  take: number
): Promise<AyahWithTranslation[]> {
  // Get ayahs
  const ayahs = await prisma.ayah.findMany({
    skip,
    take,
    orderBy: [{ surahId: "asc" }, { ayahNumber: "asc" }],
    select: {
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

  // Fetch English translations for these ayahs
  const surahAyahPairs = ayahs.map((a) => ({
    surahNumber: a.surah.number,
    ayahNumber: a.ayahNumber,
  }));

  const translations = await prisma.ayahTranslation.findMany({
    where: {
      language: "en",
      OR: surahAyahPairs.map((p) => ({
        surahNumber: p.surahNumber,
        ayahNumber: p.ayahNumber,
      })),
    },
    select: {
      surahNumber: true,
      ayahNumber: true,
      text: true,
    },
  });

  // Create a map for quick lookup
  const translationMap = new Map<string, string>();
  for (const t of translations) {
    translationMap.set(`${t.surahNumber}:${t.ayahNumber}`, t.text);
  }

  // Combine ayahs with translations (all ayahs included, translation may be null)
  return ayahs.map((ayah) => {
    const key = `${ayah.surah.number}:${ayah.ayahNumber}`;
    return {
      ...ayah,
      translationText: translationMap.get(key) || null,
    };
  });
}

/**
 * Process a batch of ayahs: generate embeddings and upsert to Qdrant
 *
 * Embedding text format:
 *   سورة البقرة، آية 255:
 *   الله لا اله الا هو الحي القيوم
 *    ||| Allah! There is no god but He...
 */
async function processBatch(ayahs: AyahWithTranslation[]): Promise<number> {
  // Prepare texts for embedding - metadata + Arabic + English translation
  const texts = ayahs.map((ayah) => {
    const metadata = `سورة ${ayah.surah.nameArabic}، آية ${ayah.ayahNumber}:`;
    const normalizedArabic = normalizeArabicText(ayah.textPlain);
    const parts = [metadata, normalizedArabic];
    if (ayah.translationText) {
      parts.push(` ||| ${ayah.translationText}`);
    }
    return truncateForEmbedding(parts.join("\n"));
  });

  // Generate embeddings in batch
  const embeddings = await generateEmbeddings(texts, embeddingModel);

  // Prepare points for Qdrant
  // Store both original text (for display) and embedded text (for debugging)
  const points = ayahs.map((ayah, index) => ({
    id: generateEnrichedAyahPointId(ayah.surah.number, ayah.ayahNumber),
    vector: embeddings[index],
    payload: {
      // Ayah identification
      surahNumber: ayah.surah.number,
      ayahNumber: ayah.ayahNumber,
      surahNameArabic: ayah.surah.nameArabic,
      surahNameEnglish: ayah.surah.nameEnglish,
      // Original text for display
      text: ayah.textUthmani,
      textPlain: ayah.textPlain,
      // Navigation
      juzNumber: ayah.juzNumber,
      pageNumber: ayah.pageNumber,
      // Embedding metadata - stores EXACTLY what was embedded
      embeddedText: texts[index],
      embeddingModel: embeddingModel,
    },
  }));

  // Upsert to Qdrant
  await qdrant.upsert(QURAN_COLLECTION, {
    wait: true,
    points,
  });

  return points.length;
}

async function main() {
  console.log("Metadata+Translation Enriched Quran Embedding Generation");
  console.log("=".repeat(60));
  console.log(`Collection: ${QURAN_COLLECTION}`);
  console.log(`Technique: metadata + Arabic + English translation`);
  console.log(`Batch size: ${BATCH_SIZE}`);
  console.log(`Mode: ${forceFlag ? "Force regenerate all" : "Skip existing"}`);
  console.log();

  // Check if translations exist
  const translationCount = await prisma.ayahTranslation.count({
    where: { language: "en" },
  });

  console.log(`Found ${translationCount} English translations in database`);
  if (translationCount === 0) {
    console.warn("Warning: No English translations found. Embeddings will use metadata + Arabic only.");
  }

  // Initialize collection
  await initializeCollection();

  // Get existing point IDs to skip
  console.log("Checking for existing embeddings...");
  const existingIds = await getExistingPointIds();
  console.log(`Found ${existingIds.size} existing embeddings\n`);

  // Get total ayah count
  const totalAyahs = await prisma.ayah.count();
  console.log(`Total ayahs in database: ${totalAyahs}\n`);

  let processed = 0;
  let skipped = 0;
  let failed = 0;
  let withTranslation = 0;
  let withoutTranslation = 0;
  let offset = 0;

  while (offset < totalAyahs) {
    // Fetch batch of ayahs with translations
    const ayahsWithTranslations = await fetchAyahsWithTranslations(offset, BATCH_SIZE);

    if (ayahsWithTranslations.length === 0) {
      offset += BATCH_SIZE;
      continue;
    }

    // Track translation coverage
    for (const ayah of ayahsWithTranslations) {
      if (ayah.translationText) withTranslation++;
      else withoutTranslation++;
    }

    // Filter out already processed ayahs
    const ayahsToProcess = ayahsWithTranslations.filter((ayah) => {
      const pointId = generateEnrichedAyahPointId(
        ayah.surah.number,
        ayah.ayahNumber
      );
      if (existingIds.has(pointId)) {
        skipped++;
        return false;
      }
      return true;
    });

    if (ayahsToProcess.length > 0) {
      try {
        const count = await processBatch(ayahsToProcess);
        processed += count;
        console.log(
          `Processed ${processed} ayahs (skipped: ${skipped}, failed: ${failed})`
        );
      } catch (error) {
        console.error(`Batch failed:`, error);
        failed += ayahsToProcess.length;
      }
    }

    offset += BATCH_SIZE;

    // Rate limiting: pause briefly between batches
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  console.log("\n" + "=".repeat(60));
  console.log("EMBEDDING SUMMARY");
  console.log("=".repeat(60));
  console.log(`Processed:          ${processed}`);
  console.log(`Skipped:            ${skipped}`);
  console.log(`With translation:   ${withTranslation}`);
  console.log(`Without translation:${withoutTranslation}`);
  console.log(`Failed:             ${failed}`);
  console.log("=".repeat(60));

  // Verify collection
  try {
    const info = await qdrant.getCollection(QURAN_COLLECTION);
    console.log(`\nMetadata+Translation collection points: ${info.points_count}`);
  } catch (error) {
    console.error("Could not get collection info:", error);
  }
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
