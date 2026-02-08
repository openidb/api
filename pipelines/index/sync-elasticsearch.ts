/**
 * Sync Data to Elasticsearch
 *
 * Syncs pages, hadiths, and ayahs from PostgreSQL to Elasticsearch.
 * Uses bulk indexing for performance.
 *
 * Usage: bun run scripts/sync-elasticsearch.ts
 */

import "../env";
import { prisma } from "../../src/db";
import {
  elasticsearch,
  ES_PAGES_INDEX,
  ES_HADITHS_INDEX,
  ES_AYAHS_INDEX,
} from "../../src/search/elasticsearch";
// Types are handled inline

const BATCH_SIZE = 1000;

interface BulkOperation {
  index: { _index: string; _id: string };
}

type BulkBody = (BulkOperation | Record<string, unknown>)[];

async function syncPages() {
  console.log("\n=== Syncing Pages ===");

  const totalCount = await prisma.page.count();
  console.log(`Total pages in PostgreSQL: ${totalCount}`);

  let processed = 0;
  let offset = 0;

  while (offset < totalCount) {
    const pages = await prisma.page.findMany({
      skip: offset,
      take: BATCH_SIZE,
      select: {
        bookId: true,
        pageNumber: true,
        volumeNumber: true,
        contentPlain: true,
        urlPageIndex: true,
        book: {
          select: {
            titleArabic: true,
            author: { select: { nameArabic: true } },
          },
        },
      },
    });

    if (pages.length === 0) break;

    const bulkBody: BulkBody = [];

    for (const page of pages) {
      // Build text_searchable: metadata + content_plain for BM25 keyword search
      const textSearchable = `${page.book.titleArabic} ${page.book.author.nameArabic} ${page.contentPlain}`;

      bulkBody.push({
        index: {
          _index: ES_PAGES_INDEX,
          _id: `${page.bookId}-${page.pageNumber}`,
        },
      });
      bulkBody.push({
        book_id: page.bookId,
        page_number: page.pageNumber,
        volume_number: page.volumeNumber,
        content_plain: page.contentPlain, // Stored for display, not indexed
        text_searchable: textSearchable,
        url_page_index: page.urlPageIndex,
      });
    }

    const result = await elasticsearch.bulk({ body: bulkBody, refresh: false });

    if (result.errors) {
      const errorItems = result.items.filter((item) => item.index?.error);
      console.error(`Bulk errors: ${errorItems.length}`);
      if (errorItems.length > 0) {
        console.error("First error:", JSON.stringify(errorItems[0].index?.error, null, 2));
      }
    }

    processed += pages.length;
    offset += BATCH_SIZE;

    const pct = ((processed / totalCount) * 100).toFixed(1);
    process.stdout.write(`\rIndexed: ${processed}/${totalCount} (${pct}%)`);
  }

  // Refresh index
  await elasticsearch.indices.refresh({ index: ES_PAGES_INDEX });

  // Verify count
  const esCount = await elasticsearch.count({ index: ES_PAGES_INDEX });
  console.log(`\nElasticsearch pages count: ${esCount.count}`);
}

async function syncHadiths() {
  console.log("\n=== Syncing Hadiths ===");

  const totalCount = await prisma.hadith.count();
  console.log(`Total hadiths in PostgreSQL: ${totalCount}`);

  let processed = 0;
  let offset = 0;

  while (offset < totalCount) {
    // Fetch hadiths with denormalized book/collection data
    const hadiths = await prisma.hadith.findMany({
      skip: offset,
      take: BATCH_SIZE,
      select: {
        id: true,
        bookId: true,
        hadithNumber: true,
        textArabic: true,
        textPlain: true,
        chapterArabic: true,
        chapterEnglish: true,
        isChainVariation: true,
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

    const bulkBody: BulkBody = [];

    for (const hadith of hadiths) {
      // Build text_searchable: metadata + text_plain for BM25 keyword search
      const searchableParts = [hadith.book.collection.nameArabic];
      if (hadith.chapterArabic) searchableParts.push(hadith.chapterArabic);
      searchableParts.push(hadith.textPlain);
      const textSearchable = searchableParts.join(" ");

      bulkBody.push({
        index: {
          _index: ES_HADITHS_INDEX,
          _id: String(hadith.id),
        },
      });
      bulkBody.push({
        id: hadith.id,
        book_id: hadith.bookId,
        hadith_number: hadith.hadithNumber,
        text_arabic: hadith.textArabic,
        text_plain: hadith.textPlain,
        text_searchable: textSearchable,
        chapter_arabic: hadith.chapterArabic,
        chapter_english: hadith.chapterEnglish,
        is_chain_variation: hadith.isChainVariation,
        book_number: hadith.book.bookNumber,
        book_name_arabic: hadith.book.nameArabic,
        book_name_english: hadith.book.nameEnglish,
        collection_slug: hadith.book.collection.slug,
        collection_name_arabic: hadith.book.collection.nameArabic,
        collection_name_english: hadith.book.collection.nameEnglish,
      });
    }

    const result = await elasticsearch.bulk({ body: bulkBody, refresh: false });

    if (result.errors) {
      const errorItems = result.items.filter((item) => item.index?.error);
      console.error(`Bulk errors: ${errorItems.length}`);
      if (errorItems.length > 0) {
        console.error("First error:", JSON.stringify(errorItems[0].index?.error, null, 2));
      }
    }

    processed += hadiths.length;
    offset += BATCH_SIZE;

    const pct = ((processed / totalCount) * 100).toFixed(1);
    process.stdout.write(`\rIndexed: ${processed}/${totalCount} (${pct}%)`);
  }

  // Refresh index
  await elasticsearch.indices.refresh({ index: ES_HADITHS_INDEX });

  // Verify count
  const esCount = await elasticsearch.count({ index: ES_HADITHS_INDEX });
  console.log(`\nElasticsearch hadiths count: ${esCount.count}`);
}

async function syncAyahs() {
  console.log("\n=== Syncing Ayahs ===");

  const totalCount = await prisma.ayah.count();
  console.log(`Total ayahs in PostgreSQL: ${totalCount}`);

  let processed = 0;
  let offset = 0;

  while (offset < totalCount) {
    // Fetch ayahs with denormalized surah data
    const ayahs = await prisma.ayah.findMany({
      skip: offset,
      take: BATCH_SIZE,
      select: {
        id: true,
        ayahNumber: true,
        textUthmani: true,
        textPlain: true,
        juzNumber: true,
        pageNumber: true,
        surahId: true,
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

    const bulkBody: BulkBody = [];

    for (const ayah of ayahs) {
      // Build text_searchable: metadata + text_plain for BM25 keyword search
      const textSearchable = `سورة ${ayah.surah.nameArabic} آية ${ayah.ayahNumber} ${ayah.textPlain}`;

      bulkBody.push({
        index: {
          _index: ES_AYAHS_INDEX,
          _id: String(ayah.id),
        },
      });
      bulkBody.push({
        id: ayah.id,
        ayah_number: ayah.ayahNumber,
        text_uthmani: ayah.textUthmani,
        text_plain: ayah.textPlain,
        text_searchable: textSearchable,
        juz_number: ayah.juzNumber,
        page_number: ayah.pageNumber,
        surah_id: ayah.surahId,
        surah_number: ayah.surah.number,
        surah_name_arabic: ayah.surah.nameArabic,
        surah_name_english: ayah.surah.nameEnglish,
      });
    }

    const result = await elasticsearch.bulk({ body: bulkBody, refresh: false });

    if (result.errors) {
      const errorItems = result.items.filter((item) => item.index?.error);
      console.error(`Bulk errors: ${errorItems.length}`);
      if (errorItems.length > 0) {
        console.error("First error:", JSON.stringify(errorItems[0].index?.error, null, 2));
      }
    }

    processed += ayahs.length;
    offset += BATCH_SIZE;

    const pct = ((processed / totalCount) * 100).toFixed(1);
    process.stdout.write(`\rIndexed: ${processed}/${totalCount} (${pct}%)`);
  }

  // Refresh index
  await elasticsearch.indices.refresh({ index: ES_AYAHS_INDEX });

  // Verify count
  const esCount = await elasticsearch.count({ index: ES_AYAHS_INDEX });
  console.log(`\nElasticsearch ayahs count: ${esCount.count}`);
}

async function main() {
  console.log("Starting Elasticsearch sync...");
  console.log(`Elasticsearch URL: ${process.env.ELASTICSEARCH_URL || "http://localhost:9200"}`);

  // Check Elasticsearch connection
  try {
    const health = await elasticsearch.cluster.health();
    console.log(`Cluster health: ${health.status}`);
  } catch (error) {
    console.error("Failed to connect to Elasticsearch:", error);
    process.exit(1);
  }

  const startTime = Date.now();

  await syncPages();
  await syncHadiths();
  await syncAyahs();

  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n=== Sync completed in ${totalTime}s ===`);

  // Final summary
  const [pagesCount, hadithsCount, ayahsCount] = await Promise.all([
    elasticsearch.count({ index: ES_PAGES_INDEX }),
    elasticsearch.count({ index: ES_HADITHS_INDEX }),
    elasticsearch.count({ index: ES_AYAHS_INDEX }),
  ]);

  console.log("\nFinal counts:");
  console.log(`  Pages:   ${pagesCount.count}`);
  console.log(`  Hadiths: ${hadithsCount.count}`);
  console.log(`  Ayahs:   ${ayahsCount.count}`);

  await prisma.$disconnect();
}

// Run
main().catch((err) => {
  console.error("Sync failed:", err);
  process.exit(1);
});
