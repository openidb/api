/**
 * Export Dictionary Pages for LLM Extraction
 *
 * Reads raw page content from the Page table for a given dictionary book
 * and writes batch JSON files for processing by Claude subagents.
 *
 * Pages are grouped into overlapping chunks (1-page overlap at boundaries)
 * to ensure definitions spanning chunk boundaries are fully captured.
 *
 * Usage:
 *   bun run pipelines/import/dictionary/export-pages-for-extraction.ts \
 *     --slug=tarifat [--pages-per-chunk=5] [--chunks-per-batch=5]
 */

import "../../env";
import { prisma } from "../../../src/db";
import { mkdirSync, writeFileSync } from "fs";
import { resolve } from "path";

const BATCH_DIR = resolve(import.meta.dir, "extraction-batches");

interface PageRecord {
  pageNumber: number;
  volumeNumber: number;
  contentPlain: string;
}

function parseArgs() {
  const args = process.argv.slice(2);
  let slug = "";
  let pagesPerChunk = 0; // 0 = auto-detect
  let chunksPerBatch = 5;

  for (const arg of args) {
    if (arg.startsWith("--slug=")) slug = arg.slice(7);
    else if (arg.startsWith("--pages-per-chunk=")) pagesPerChunk = parseInt(arg.slice(18), 10) || 0;
    else if (arg.startsWith("--chunks-per-batch=")) chunksPerBatch = parseInt(arg.slice(19), 10) || 5;
  }

  if (!slug) {
    console.error("Usage: bun run export-pages-for-extraction.ts --slug=<slug> [--pages-per-chunk=N] [--chunks-per-batch=N]");
    process.exit(1);
  }

  return { slug, pagesPerChunk, chunksPerBatch };
}

function autoDetectChunkSize(pages: PageRecord[]): number {
  if (pages.length === 0) return 5;
  const totalChars = pages.reduce((sum, p) => sum + p.contentPlain.length, 0);
  const avgChars = totalChars / pages.length;

  if (avgChars < 100) return 50;   // short pages (e.g. mukhtar ~13 chars)
  if (avgChars <= 800) return 10;  // medium pages (e.g. muhit ~400 chars)
  return 5;                         // long pages (e.g. tarifat ~1000, wasit ~3900)
}

async function main() {
  const { slug, pagesPerChunk, chunksPerBatch } = parseArgs();

  // Look up dictionary source
  const source = await prisma.dictionarySource.findUnique({ where: { slug } });
  if (!source) {
    console.error(`Dictionary source "${slug}" not found.`);
    const all = await prisma.dictionarySource.findMany({ select: { slug: true, nameEnglish: true } });
    console.error("Available slugs:", all.map((s) => s.slug).join(", "));
    process.exit(1);
  }

  if (!source.bookId) {
    console.error(`Dictionary source "${slug}" has no bookId.`);
    process.exit(1);
  }

  console.log(`Dictionary: ${source.nameEnglish} (${source.nameArabic}), bookId=${source.bookId}`);

  // Fetch all pages for the book
  const pages = await prisma.page.findMany({
    where: { bookId: source.bookId },
    select: { pageNumber: true, volumeNumber: true, contentPlain: true },
    orderBy: [{ volumeNumber: "asc" }, { pageNumber: "asc" }],
  });

  if (pages.length === 0) {
    console.error(`No pages found for bookId=${source.bookId}`);
    process.exit(1);
  }

  const chunkSize = pagesPerChunk || autoDetectChunkSize(pages);
  const totalChars = pages.reduce((sum, p) => sum + p.contentPlain.length, 0);
  const avgChars = Math.round(totalChars / pages.length);

  console.log(`Pages: ${pages.length}, avg ${avgChars} chars/page, chunk size: ${chunkSize} pages`);

  // Create overlapping chunks (1-page overlap at boundaries)
  const chunks: Array<{
    chunkId: number;
    startPage: number;
    endPage: number;
    volumeNumber: number;
    pages: Array<{ pageNumber: number; contentPlain: string }>;
  }> = [];

  let chunkId = 1;
  const step = Math.max(1, chunkSize - 1); // overlap of 1 page

  for (let i = 0; i < pages.length; i += step) {
    const chunkPages = pages.slice(i, i + chunkSize);
    if (chunkPages.length === 0) break;

    chunks.push({
      chunkId: chunkId++,
      startPage: chunkPages[0].pageNumber,
      endPage: chunkPages[chunkPages.length - 1].pageNumber,
      volumeNumber: chunkPages[0].volumeNumber,
      pages: chunkPages.map((p) => ({
        pageNumber: p.pageNumber,
        contentPlain: p.contentPlain,
      })),
    });

    // If last chunk covers remaining pages, stop
    if (i + chunkSize >= pages.length) break;
  }

  console.log(`Chunks: ${chunks.length}`);

  // Group chunks into batch files
  const slugDir = resolve(BATCH_DIR, slug);
  mkdirSync(slugDir, { recursive: true });

  const totalBatches = Math.ceil(chunks.length / chunksPerBatch);
  console.log(`Writing ${totalBatches} batch files (${chunksPerBatch} chunks each)...`);

  for (let i = 0; i < totalBatches; i++) {
    const batchChunks = chunks.slice(i * chunksPerBatch, (i + 1) * chunksPerBatch);
    const batchNum = String(i + 1).padStart(3, "0");
    const filePath = resolve(slugDir, `batch-${batchNum}.json`);

    const batch = {
      slug: source.slug,
      sourceId: source.id,
      bookId: source.bookId,
      dictionaryName: source.nameArabic,
      totalChunks: chunks.length,
      chunks: batchChunks,
    };

    writeFileSync(filePath, JSON.stringify(batch, null, 2));
    console.log(`  batch-${batchNum}.json: ${batchChunks.length} chunks (pages ${batchChunks[0].startPage}-${batchChunks[batchChunks.length - 1].endPage})`);
  }

  console.log(`\nDone. ${totalBatches} batch files written to ${slugDir}`);
}

main()
  .catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
  })
  .finally(() => prisma.$disconnect());
