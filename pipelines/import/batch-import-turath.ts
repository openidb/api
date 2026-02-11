/**
 * Batch Import Books from Turath
 *
 * Reads from turath-catalog.json and imports books sequentially.
 * Supports checkpointing, resume, and progress tracking.
 *
 * Usage:
 *   bun run pipelines/import/batch-import-turath.ts --limit=50                  # Import first 50
 *   bun run pipelines/import/batch-import-turath.ts --all                       # Import all
 *   bun run pipelines/import/batch-import-turath.ts --resume                    # Resume from checkpoint
 *   bun run pipelines/import/batch-import-turath.ts --limit=50 --offset=100     # Skip first 100
 *   bun run pipelines/import/batch-import-turath.ts --limit=50 --dry-run        # Preview only
 *   bun run pipelines/import/batch-import-turath.ts --limit=50 --delay=1000     # 1s between books
 *   bun run pipelines/import/batch-import-turath.ts --limit=50 --no-skip-existing
 */

import "../env";
import { prisma } from "../../src/db";
import { importTurathBook, type ImportResult } from "./import-turath";
import { resolve } from "path";
import { readFileSync, writeFileSync, existsSync } from "fs";

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------

interface BatchArgs {
  limit: number | null;
  offset: number;
  all: boolean;
  dryRun: boolean;
  resume: boolean;
  delay: number;
  skipExisting: boolean;
}

function parseArgs(): BatchArgs {
  const args = process.argv.slice(2);
  const result: BatchArgs = {
    limit: null,
    offset: 0,
    all: false,
    dryRun: false,
    resume: false,
    delay: 750,
    skipExisting: true,
  };

  for (const arg of args) {
    if (arg.startsWith("--limit=")) {
      result.limit = parseInt(arg.slice(8), 10);
    } else if (arg.startsWith("--offset=")) {
      result.offset = parseInt(arg.slice(9), 10);
    } else if (arg === "--all") {
      result.all = true;
    } else if (arg === "--dry-run") {
      result.dryRun = true;
    } else if (arg === "--resume") {
      result.resume = true;
    } else if (arg.startsWith("--delay=")) {
      result.delay = parseInt(arg.slice(8), 10);
    } else if (arg === "--no-skip-existing") {
      result.skipExisting = false;
    }
  }

  if (!result.all && result.limit === null && !result.resume) {
    console.error("Usage: bun run pipelines/import/batch-import-turath.ts --limit=50 | --all | --resume");
    console.error("\nOptions:");
    console.error("  --limit=N              Import N books from catalog");
    console.error("  --offset=N             Skip first N catalog entries");
    console.error("  --all                  Import all books in catalog");
    console.error("  --resume               Resume from last checkpoint");
    console.error("  --dry-run              Preview without DB writes");
    console.error("  --delay=MS             Delay between books in ms (default: 750)");
    console.error("  --no-skip-existing     Re-import books already in DB");
    process.exit(1);
  }

  return result;
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface CatalogBook {
  id: number;
  title: string;
  authorId: number | null;
  categoryId: number;
}

interface Catalog {
  scrapedAt: string;
  totalBooks: number;
  books: CatalogBook[];
}

interface BatchProgress {
  lastIndex: number;
  imported: number;
  skipped: number;
  failed: number;
  startedAt: string;
  failedIds: number[];
}

// ---------------------------------------------------------------------------
// File paths
// ---------------------------------------------------------------------------

const CATALOG_PATH = resolve(import.meta.dir, "turath-catalog.json");
const PROGRESS_PATH = resolve(import.meta.dir, "batch-progress.json");
const CHECKPOINT_INTERVAL = 10;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function loadCatalog(): Catalog {
  if (!existsSync(CATALOG_PATH)) {
    console.error(`Catalog file not found: ${CATALOG_PATH}`);
    console.error("Run scrape-turath-catalog.ts first.");
    process.exit(1);
  }
  return JSON.parse(readFileSync(CATALOG_PATH, "utf-8"));
}

function loadProgress(): BatchProgress | null {
  if (!existsSync(PROGRESS_PATH)) return null;
  try {
    return JSON.parse(readFileSync(PROGRESS_PATH, "utf-8"));
  } catch {
    return null;
  }
}

function saveProgress(progress: BatchProgress): void {
  writeFileSync(PROGRESS_PATH, JSON.stringify(progress, null, 2));
}

function formatDuration(ms: number): string {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);

  if (hours > 0) {
    return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
  }
  if (minutes > 0) {
    return `${minutes}m ${seconds % 60}s`;
  }
  return `${seconds}s`;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const args = parseArgs();
  const catalog = loadCatalog();

  console.log("Turath Batch Import");
  console.log("=".repeat(60));
  console.log(`Catalog:            ${catalog.totalBooks} books (scraped ${catalog.scrapedAt})`);
  console.log(`Mode:               ${args.dryRun ? "DRY RUN" : "LIVE IMPORT"}`);
  console.log(`Skip existing:      ${args.skipExisting ? "yes" : "no"}`);
  console.log(`Delay:              ${args.delay}ms between books`);

  // Determine start index and limit
  let startIndex = args.offset;
  let progress: BatchProgress;

  if (args.resume) {
    const saved = loadProgress();
    if (saved) {
      startIndex = saved.lastIndex + 1;
      progress = saved;
      console.log(`Resuming:           from index ${startIndex} (${saved.imported} imported, ${saved.failed} failed)`);
    } else {
      console.log("No checkpoint found, starting fresh.");
      progress = {
        lastIndex: -1,
        imported: 0,
        skipped: 0,
        failed: 0,
        startedAt: new Date().toISOString(),
        failedIds: [],
      };
    }
  } else {
    progress = {
      lastIndex: startIndex - 1,
      imported: 0,
      skipped: 0,
      failed: 0,
      startedAt: new Date().toISOString(),
      failedIds: [],
    };
  }

  const books = catalog.books.slice(startIndex);
  const limit = args.all ? books.length : (args.limit ?? books.length);
  const booksToProcess = books.slice(0, limit);

  console.log(`Processing:         ${booksToProcess.length} books (index ${startIndex} to ${startIndex + booksToProcess.length - 1})`);
  console.log("=".repeat(60));
  console.log();

  if (booksToProcess.length === 0) {
    console.log("No books to process.");
    return;
  }

  const batchStartTime = Date.now();

  for (let i = 0; i < booksToProcess.length; i++) {
    const book = booksToProcess[i];
    const globalIndex = startIndex + i;
    const displayNum = i + 1;
    const bookStartTime = Date.now();

    console.log(`\n${"─".repeat(60)}`);
    console.log(`[${displayNum}/${booksToProcess.length}] Book ${book.id}: "${book.title}"`);
    console.log(`${"─".repeat(60)}`);

    // Check if already in DB
    if (args.skipExisting && !args.dryRun) {
      const existing = await prisma.book.findUnique({
        where: { id: String(book.id) },
        select: { id: true },
      });
      if (existing) {
        console.log(`  Skipped (already in DB)`);
        progress.skipped++;
        progress.lastIndex = globalIndex;
        continue;
      }
    }

    // Import
    const result: ImportResult = await importTurathBook(String(book.id), {
      dryRun: args.dryRun,
    });

    const elapsed = ((Date.now() - bookStartTime) / 1000).toFixed(1);

    if (result.success) {
      progress.imported++;
      console.log(`\n  [${displayNum}/${booksToProcess.length}] \u2713 Book ${book.id} "${result.title}" (${result.pages} pages, ${elapsed}s)`);
    } else {
      progress.failed++;
      progress.failedIds.push(book.id);
      console.log(`\n  [${displayNum}/${booksToProcess.length}] \u2717 Book ${book.id} FAILED: ${result.error} (${elapsed}s)`);
    }

    progress.lastIndex = globalIndex;

    // Save checkpoint every N books
    if ((i + 1) % CHECKPOINT_INTERVAL === 0) {
      saveProgress(progress);
      console.log(`  [checkpoint saved at index ${globalIndex}]`);
    }

    // Running summary
    const totalElapsed = Date.now() - batchStartTime;
    const avgPerBook = totalElapsed / (i + 1);
    const remaining = booksToProcess.length - (i + 1);
    const eta = formatDuration(remaining * avgPerBook);
    console.log(`  Running: ${progress.imported} imported, ${progress.skipped} skipped, ${progress.failed} failed — ETA: ${eta}`);

    // Delay between books
    if (i < booksToProcess.length - 1) {
      await sleep(args.delay);
    }
  }

  // Final checkpoint
  saveProgress(progress);

  // Summary
  const totalDuration = formatDuration(Date.now() - batchStartTime);

  console.log(`\n${"=".repeat(60)}`);
  console.log("Batch Import Complete");
  console.log("=".repeat(60));
  console.log(`  Total:      ${booksToProcess.length}`);
  console.log(`  Imported:   ${progress.imported}`);
  console.log(`  Skipped:    ${progress.skipped} (already in DB)`);
  console.log(`  Failed:     ${progress.failed}`);
  console.log(`  Duration:   ${totalDuration}`);

  if (progress.failedIds.length > 0) {
    console.log(`  Failed IDs: ${progress.failedIds.join(", ")}`);
  }

  console.log("=".repeat(60));
}

main()
  .catch((e) => {
    console.error("\nBatch import failed:");
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
