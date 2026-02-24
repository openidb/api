/**
 * Backfill missing PDFs in RustFS
 *
 * Finds books that have RustFS-style pdf_url in the database (e.g. "123/0.pdf")
 * but no corresponding object in RustFS, then downloads from Turath and uploads.
 *
 * Usage:
 *   bun run pipelines/import/backfill-pdfs.ts [--dry-run] [--concurrency=5] [--limit=100]
 */

import { prisma } from "../../src/db";
import { s3, BUCKET_NAME } from "../../src/s3";
import {
  HeadObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";

const DRY_RUN = process.argv.includes("--dry-run");
const CONCURRENCY = parseInt(
  process.argv.find((a) => a.startsWith("--concurrency="))?.split("=")[1] ?? "5",
);
const LIMIT = parseInt(
  process.argv.find((a) => a.startsWith("--limit="))?.split("=")[1] ?? "0",
);

interface BookPdfInfo {
  bookId: string;
  keys: string[]; // RustFS keys like "123/0.pdf"
}

async function isInRustFS(key: string): Promise<boolean> {
  try {
    await s3.send(new HeadObjectCommand({ Bucket: BUCKET_NAME, Key: key }));
    return true;
  } catch {
    return false;
  }
}

function buildPdfUrl(
  pdfLinks: { root?: string; files?: string[] } | null,
  volumeIndex: number,
): string | null {
  if (!pdfLinks || !pdfLinks.files || pdfLinks.files.length === 0) return null;

  const rawFile = pdfLinks.files[Math.min(volumeIndex, pdfLinks.files.length - 1)];
  if (!rawFile) return null;

  const file = rawFile.split("|")[0];

  if (!pdfLinks.root) {
    return file.startsWith("http") ? file : null;
  }

  if (pdfLinks.root.startsWith("http")) {
    const root = pdfLinks.root.endsWith("/") ? pdfLinks.root : pdfLinks.root + "/";
    return `${root}${file}`;
  }

  const root = pdfLinks.root.endsWith("/") ? pdfLinks.root : pdfLinks.root + "/";
  return encodeURI(`https://files.turath.io/pdf/${root}${file}`);
}

async function downloadAndStore(sourceUrl: string, key: string): Promise<boolean> {
  try {
    const res = await fetch(sourceUrl, { signal: AbortSignal.timeout(120_000) });
    if (!res.ok) {
      console.warn(`  [pdf] Download failed ${sourceUrl}: ${res.status}`);
      return false;
    }
    const body = await res.arrayBuffer();
    if (body.byteLength < 1000) {
      console.warn(`  [pdf] Suspiciously small PDF (${body.byteLength}B): ${sourceUrl}`);
      return false;
    }

    await s3.send(
      new PutObjectCommand({
        Bucket: BUCKET_NAME,
        Key: key,
        Body: new Uint8Array(body),
        ContentType: "application/pdf",
        Metadata: { sourceUrl },
      }),
    );
    return true;
  } catch (err) {
    console.warn(`  [pdf] Error: ${(err as Error).message}`);
    return false;
  }
}

async function main() {
  console.log("PDF Backfill — Finding missing PDFs...");
  console.log(`  Concurrency: ${CONCURRENCY}`);
  if (DRY_RUN) console.log("  DRY RUN — no downloads will be made");
  if (LIMIT > 0) console.log(`  Limit: ${LIMIT} books`);

  // Step 1: Get all distinct RustFS PDF keys from the DB
  const rows = await prisma.$queryRawUnsafe<{ book_id: string; pdf_url: string }[]>(`
    SELECT DISTINCT book_id, pdf_url
    FROM pages
    WHERE pdf_url IS NOT NULL
      AND pdf_url != ''
      AND pdf_url NOT LIKE 'http%'
    ORDER BY book_id
  `);

  // Group by book
  const bookMap = new Map<string, string[]>();
  for (const row of rows) {
    const keys = bookMap.get(row.book_id) || [];
    keys.push(row.pdf_url);
    bookMap.set(row.book_id, keys);
  }

  console.log(`\n  Total books with RustFS PDF refs: ${bookMap.size}`);
  console.log(`  Total unique PDF keys: ${rows.length}`);

  // Step 2: Check which keys are missing from RustFS
  console.log("\nChecking RustFS for missing objects...");
  const missingBooks: BookPdfInfo[] = [];
  let checked = 0;

  for (const [bookId, keys] of bookMap) {
    // Only check the first key per book (all volumes share the same directory)
    const exists = await isInRustFS(keys[0]);
    checked++;
    if (checked % 500 === 0) console.log(`  Checked ${checked}/${bookMap.size}...`);

    if (!exists) {
      missingBooks.push({ bookId, keys: [...new Set(keys)] });
    }
  }

  console.log(`\n  Missing books: ${missingBooks.length}`);
  if (missingBooks.length === 0) {
    console.log("All PDFs present! Nothing to do.");
    await prisma.$disconnect();
    return;
  }

  if (DRY_RUN) {
    console.log("\nMissing book IDs:");
    for (const b of missingBooks.slice(0, 50)) {
      console.log(`  ${b.bookId} (${b.keys.length} volumes)`);
    }
    if (missingBooks.length > 50) console.log(`  ... and ${missingBooks.length - 50} more`);
    await prisma.$disconnect();
    return;
  }

  // Step 3: Fetch Turath metadata and download PDFs
  const toProcess = LIMIT > 0 ? missingBooks.slice(0, LIMIT) : missingBooks;
  let downloaded = 0;
  let failed = 0;
  let skipped = 0;
  let idx = 0;

  // Process in batches of CONCURRENCY
  for (let i = 0; i < toProcess.length; i += CONCURRENCY) {
    const batch = toProcess.slice(i, i + CONCURRENCY);

    await Promise.all(
      batch.map(async (book) => {
        idx++;
        const { bookId, keys } = book;

        try {
          // Fetch metadata from Turath API
          const metaRes = await fetch(
            `https://api.turath.io/book?id=${bookId}`,
            { signal: AbortSignal.timeout(15_000) },
          );
          if (!metaRes.ok) {
            console.warn(`  [${idx}/${toProcess.length}] Book ${bookId}: Turath API ${metaRes.status}`);
            failed += keys.length;
            return;
          }
          const meta = await metaRes.json();
          const pdfLinks = meta["\u0650"] as { root?: string; files?: string[] } | null;

          if (!pdfLinks?.files || pdfLinks.files.length === 0) {
            console.warn(`  [${idx}/${toProcess.length}] Book ${bookId}: No PDF links in metadata`);
            skipped += keys.length;
            return;
          }

          // Download each volume
          for (const key of keys) {
            // Extract volume index from key (e.g. "123/0.pdf" → 0)
            const match = key.match(/\/(\d+)\.pdf$/);
            if (!match) continue;
            const volIdx = parseInt(match[1]);
            const sourceUrl = buildPdfUrl(pdfLinks, volIdx);
            if (!sourceUrl) {
              skipped++;
              continue;
            }

            const ok = await downloadAndStore(sourceUrl, key);
            if (ok) {
              downloaded++;
            } else {
              failed++;
            }
          }

          console.log(
            `  [${idx}/${toProcess.length}] Book ${bookId}: ${keys.length} volume(s) processed`,
          );
        } catch (err) {
          console.warn(`  [${idx}/${toProcess.length}] Book ${bookId}: ${(err as Error).message}`);
          failed += keys.length;
        }
      }),
    );
  }

  console.log(`\nDone!`);
  console.log(`  Downloaded: ${downloaded}`);
  console.log(`  Failed:     ${failed}`);
  console.log(`  Skipped:    ${skipped}`);

  await prisma.$disconnect();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
