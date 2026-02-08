/**
 * Extract EPUB Pages Script
 *
 * Extracts text from each EPUB page and stores it in the Pages table.
 * This prepares content for embedding generation.
 *
 * Usage: bun run scripts/extract-epub-pages.ts [--force]
 *
 * Options:
 *   --force  Re-extract pages even if they already exist
 */

import "../env";
import { prisma } from "../../src/db";
import JSZip from "jszip";
import * as fs from "fs";
import * as path from "path";

const BOOKS_DIR = path.join(__dirname, "../../data/books");

/**
 * Strip HTML tags and extract plain text
 */
function stripHtml(html: string): string {
  return (
    html
      // Remove script and style elements
      .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, "")
      .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, "")
      // Replace common HTML entities
      .replace(/&nbsp;/g, " ")
      .replace(/&amp;/g, "&")
      .replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">")
      .replace(/&quot;/g, '"')
      .replace(/&#39;/g, "'")
      // Replace block elements with newlines
      .replace(/<\/?(p|div|br|h[1-6]|li|tr)[^>]*>/gi, "\n")
      // Remove all other tags
      .replace(/<[^>]+>/g, "")
      // Clean up whitespace
      .replace(/\n{3,}/g, "\n\n")
      .replace(/[ \t]+/g, " ")
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0)
      .join("\n")
      .trim()
  );
}

/**
 * Extract page number from filename
 * Handles: page_0001.xhtml, page_i.xhtml, etc.
 * Returns sequential pageNumber (for database ordering)
 */
function extractPageNumber(filename: string): number {
  const match = filename.match(/page_(\d+|i)\.xhtml/);
  if (!match) {
    return 0;
  }

  const pageIndex = match[1];
  if (pageIndex === "i") {
    return 0;
  }

  return parseInt(pageIndex, 10);
}

/**
 * Parse the EPUB's page-list from nav.xhtml
 * Returns a map from href (e.g., "page_0057.xhtml") to label (e.g., "66")
 */
async function parseEpubPageList(zip: JSZip): Promise<Map<string, string>> {
  const pageMap = new Map<string, string>();

  try {
    const navFile = zip.file("EPUB/nav.xhtml");
    if (!navFile) {
      return pageMap;
    }

    const navContent = await navFile.async("text");

    // Find page-list section
    const pageListMatch = navContent.match(
      /<nav[^>]*epub:type="page-list"[^>]*>([\s\S]*?)<\/nav>/
    );
    if (!pageListMatch) {
      return pageMap;
    }

    // Extract all page entries: <a href="page_0057.xhtml">66</a>
    const pageEntries = pageListMatch[1].matchAll(
      /<a href="([^"]+)">([^<]+)<\/a>/g
    );

    for (const match of pageEntries) {
      const href = match[1]; // e.g., "page_0057.xhtml"
      const label = match[2]; // e.g., "66"
      pageMap.set(href, label);
    }
  } catch (error) {
    console.warn("  Warning: Could not parse page-list from nav.xhtml:", error);
  }

  return pageMap;
}

/**
 * Extract volume number from page content or filename
 */
function extractVolumeNumber(content: string): number {
  // Look for volume indicators in content
  const volumeMatch = content.match(
    /الجزء\s*[:\s]*(\d+)|المجلد\s*[:\s]*(\d+)|ج\s*(\d+)/
  );
  if (volumeMatch) {
    return parseInt(volumeMatch[1] || volumeMatch[2] || volumeMatch[3], 10);
  }
  return 1;
}

/**
 * Detect content flags based on text patterns
 */
function detectContentFlags(text: string): {
  hasPoetry: boolean;
  hasHadith: boolean;
  hasQuran: boolean;
  hasDialogue: boolean;
} {
  return {
    // Poetry usually has numbered verses or specific meter markers
    hasPoetry: /(\d+)\s*-\s*[^\d]/.test(text) || /[\u0640]{2,}/.test(text),
    // Hadith indicators
    hasHadith:
      /قال رسول الله|صلى الله عليه وسلم|حدثنا|أخبرنا/.test(text),
    // Quran indicators (ayah markers or citation patterns)
    hasQuran: /\{[^}]+\}|﴿[^﴾]+﴾|قال تعالى/.test(text),
    // Dialogue indicators
    hasDialogue: /قال:|قلت:|فقال:|قالوا:|قالت:/.test(text),
  };
}

/**
 * Extract pages from a single EPUB file
 */
async function extractPagesFromEpub(
  epubPath: string,
  bookId: string
): Promise<number> {
  const epubData = fs.readFileSync(epubPath);
  const zip = await JSZip.loadAsync(epubData);

  // Parse the EPUB's page-list to get actual page labels
  const pageListMap = await parseEpubPageList(zip);

  // Find all page files
  const pageFiles = Object.keys(zip.files).filter((name) =>
    name.match(/EPUB\/page_(\d+|i)\.xhtml/)
  );

  // Sort pages by number (i comes first as page 0)
  pageFiles.sort((a, b) => {
    const aNum = extractPageNumber(a);
    const bNum = extractPageNumber(b);
    return aNum - bNum;
  });

  let extractedCount = 0;

  for (const pageFile of pageFiles) {
    const pageNumber = extractPageNumber(pageFile);

    // Get the filename without EPUB/ prefix for page-list lookup
    const filenameOnly = pageFile.replace("EPUB/", "");

    // Get the actual page label from the page-list, fallback to filename index
    let urlPageIndex = pageListMap.get(filenameOnly);
    if (!urlPageIndex) {
      // Fallback: use the filename index (e.g., "0057" from page_0057.xhtml)
      const match = filenameOnly.match(/page_(\d+|i)\.xhtml/);
      urlPageIndex = match ? match[1] : String(pageNumber);
    }

    // Read page content
    const content = await zip.files[pageFile].async("text");
    const plainText = stripHtml(content);

    // Skip empty pages
    if (plainText.length < 10) {
      continue;
    }

    // Detect volume and content flags
    const volumeNumber = extractVolumeNumber(plainText);
    const contentFlags = detectContentFlags(plainText);

    // Upsert the page
    await prisma.page.upsert({
      where: {
        bookId_pageNumber: {
          bookId,
          pageNumber,
        },
      },
      update: {
        contentPlain: plainText,
        contentHtml: content,
        volumeNumber,
        urlPageIndex,
        ...contentFlags,
      },
      create: {
        bookId,
        pageNumber,
        volumeNumber,
        urlPageIndex,
        contentPlain: plainText,
        contentHtml: content,
        ...contentFlags,
        sourceUrl: `https://shamela.ws/book/${bookId}/${urlPageIndex}`,
      },
    });

    extractedCount++;
  }

  return extractedCount;
}

async function main() {
  const forceFlag = process.argv.includes("--force");

  console.log("EPUB Page Extraction");
  console.log("=".repeat(60));
  console.log(`Mode: ${forceFlag ? "Force re-extract all" : "Skip existing"}`);
  console.log();

  // Get all books from database
  const books = await prisma.book.findMany({
    select: {
      id: true,
      titleArabic: true,
      filename: true,
      _count: {
        select: { pages: true },
      },
    },
  });

  console.log(`Found ${books.length} books in database\n`);

  let totalExtracted = 0;
  let booksProcessed = 0;
  let booksSkipped = 0;
  let booksFailed = 0;

  for (const book of books) {
    // Skip if pages already exist (unless force flag)
    if (!forceFlag && book._count.pages > 0) {
      console.log(`  -> ${book.titleArabic.slice(0, 40)}... (${book._count.pages} pages exist, skipping)`);
      booksSkipped++;
      continue;
    }

    const epubPath = path.join(BOOKS_DIR, book.filename);

    // Check if file exists
    if (!fs.existsSync(epubPath)) {
      console.log(`  X  ${book.titleArabic.slice(0, 40)}... (file not found)`);
      booksFailed++;
      continue;
    }

    try {
      // Delete existing pages if force mode
      if (forceFlag && book._count.pages > 0) {
        await prisma.page.deleteMany({ where: { bookId: book.id } });
      }

      const pageCount = await extractPagesFromEpub(
        epubPath,
        book.id
      );

      console.log(`  +  ${book.titleArabic.slice(0, 40)}... (${pageCount} pages)`);
      totalExtracted += pageCount;
      booksProcessed++;
    } catch (error) {
      console.error(`  X  ${book.titleArabic.slice(0, 40)}... (error: ${error})`);
      booksFailed++;
    }
  }

  console.log("\n" + "=".repeat(60));
  console.log("EXTRACTION SUMMARY");
  console.log("=".repeat(60));
  console.log(`Books processed: ${booksProcessed}`);
  console.log(`Books skipped:   ${booksSkipped}`);
  console.log(`Books failed:    ${booksFailed}`);
  console.log(`Total pages:     ${totalExtracted}`);
  console.log("=".repeat(60));
  console.log("\nExtraction completed!");
}

main()
  .catch((e) => {
    console.error("\nExtraction failed:");
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
