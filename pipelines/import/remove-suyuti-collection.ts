/**
 * Remove Suyuti (Jam' al-Jawami') from the hadith system.
 *
 * Suyuti is both a hadith collection (~36K hadiths) AND a regular book (Turath ID 127677).
 * This script removes it from hadith tables, Elasticsearch, and Qdrant,
 * while keeping it as a regular book with pages.
 *
 * Usage:
 *   bun run pipelines/import/remove-suyuti-collection.ts --dry-run   (default, preview only)
 *   bun run pipelines/import/remove-suyuti-collection.ts --force      (actually delete)
 */

import { PrismaClient } from "@prisma/client";
import { QdrantClient } from "@qdrant/js-client-rest";

const SLUG = "suyuti";
const ES_INDEX = "arabic_hadiths";
const QDRANT_COLLECTIONS = ["hadiths", "hadiths_jina"];

const prisma = new PrismaClient();
const qdrant = new QdrantClient({
  url: process.env.QDRANT_URL || "http://localhost:6333",
});
const esUrl = process.env.ELASTICSEARCH_URL || "http://localhost:9200";

const force = process.argv.includes("--force");
const dryRun = !force;

async function main() {
  console.log(`\n=== Remove Suyuti from Hadith System ===`);
  console.log(`Mode: ${dryRun ? "DRY RUN (pass --force to execute)" : "FORCE — will delete data"}\n`);

  // 1. Look up collection and book IDs
  const collection = await prisma.hadithCollection.findFirst({
    where: { slug: SLUG },
  });

  if (!collection) {
    console.log(`No hadith collection found with slug "${SLUG}". Nothing to do.`);
    await prisma.$disconnect();
    return;
  }

  console.log(`Found collection: ${collection.name} (id=${collection.id})`);

  const books = await prisma.hadithBook.findMany({
    where: { collectionId: collection.id },
    select: { id: true },
  });
  const bookIds = books.map((b) => b.id);
  console.log(`  ${bookIds.length} hadith books`);

  // Count hadiths and translations
  const hadithCount = await prisma.hadith.count({
    where: { bookId: { in: bookIds } },
  });
  const translationCount = await prisma.hadithTranslation.count({
    where: { bookId: { in: bookIds } },
  });
  console.log(`  ${hadithCount} hadiths`);
  console.log(`  ${translationCount} translations`);

  if (dryRun) {
    console.log(`\n--- DRY RUN: Would delete ---`);
    console.log(`  ${translationCount} hadith_translations`);
    console.log(`  ${hadithCount} hadiths`);
    console.log(`  ${bookIds.length} hadith_books`);
    console.log(`  1 hadith_collection (slug="${SLUG}")`);
    console.log(`  ES: delete_by_query on ${ES_INDEX} where collection_slug="${SLUG}"`);
    console.log(`  Qdrant: delete points where collectionSlug="${SLUG}" from ${QDRANT_COLLECTIONS.join(", ")}`);
    console.log(`\nRe-run with --force to execute.`);
    await prisma.$disconnect();
    return;
  }

  // === FORCE MODE: Delete everything ===

  // 2. Delete from Postgres
  console.log(`\n--- Deleting from Postgres ---`);

  if (bookIds.length > 0) {
    const deletedTranslations = await prisma.hadithTranslation.deleteMany({
      where: { bookId: { in: bookIds } },
    });
    console.log(`  Deleted ${deletedTranslations.count} translations`);

    const deletedHadiths = await prisma.hadith.deleteMany({
      where: { bookId: { in: bookIds } },
    });
    console.log(`  Deleted ${deletedHadiths.count} hadiths`);

    const deletedBooks = await prisma.hadithBook.deleteMany({
      where: { collectionId: collection.id },
    });
    console.log(`  Deleted ${deletedBooks.count} hadith books`);
  }

  const deletedCollection = await prisma.hadithCollection.deleteMany({
    where: { slug: SLUG },
  });
  console.log(`  Deleted ${deletedCollection.count} collection(s)`);

  // 3. Delete from Elasticsearch
  console.log(`\n--- Deleting from Elasticsearch ---`);
  try {
    const esResponse = await fetch(`${esUrl}/${ES_INDEX}/_delete_by_query`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        query: {
          term: { collection_slug: SLUG },
        },
      }),
    });
    const esResult = await esResponse.json();
    console.log(`  ES deleted: ${esResult.deleted ?? 0} documents`);
  } catch (err) {
    console.error(`  ES delete failed:`, err);
  }

  // 4. Delete from Qdrant
  console.log(`\n--- Deleting from Qdrant ---`);
  for (const col of QDRANT_COLLECTIONS) {
    try {
      const result = await qdrant.delete(col, {
        filter: {
          must: [
            {
              key: "collectionSlug",
              match: { value: SLUG },
            },
          ],
        },
      });
      console.log(`  Qdrant ${col}: ${result.status}`);
    } catch (err: any) {
      if (err?.status === 404) {
        console.log(`  Qdrant ${col}: collection not found (skipped)`);
      } else {
        console.error(`  Qdrant ${col} delete failed:`, err);
      }
    }
  }

  console.log(`\nDone. Suyuti removed from hadith system.`);
  await prisma.$disconnect();
}

main().catch((err) => {
  console.error(err);
  prisma.$disconnect();
  process.exit(1);
});
