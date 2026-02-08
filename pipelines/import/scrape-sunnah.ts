/**
 * Sunnah.com Hadith Scraper
 *
 * Scrapes hadith collections from sunnah.com and stores them in the database.
 * Supports two-phase approach with HTML caching for reliability and resume capability.
 *
 * Usage:
 *   # Download HTML files only (Phase 1)
 *   bun run scripts/scrape-sunnah.ts --download-only
 *   bun run scripts/scrape-sunnah.ts --download-only --collection=bukhari
 *
 *   # Process downloaded HTML files (Phase 2)
 *   bun run scripts/scrape-sunnah.ts --process-only
 *   bun run scripts/scrape-sunnah.ts --process-only --collection=bukhari
 *
 *   # Full scrape (download + process in one go)
 *   bun run scripts/scrape-sunnah.ts --collection=nawawi40
 *   bun run scripts/scrape-sunnah.ts --all
 *
 *   # Custom HTML directory
 *   bun run scripts/scrape-sunnah.ts --download-only --html-dir=/path/to/html
 *
 *   # Resume from a specific book
 *   bun run scripts/scrape-sunnah.ts --collection=bukhari --resume-from=50
 */

import "../env";
import * as cheerio from "cheerio";
import { prisma } from "../../src/db";
import { normalizeArabicText } from "../../src/utils/arabic";
import * as fs from "fs";
import * as path from "path";

// Collection definitions with estimated counts for progress tracking
const COLLECTIONS = [
  { slug: "bukhari", nameEnglish: "Sahih al-Bukhari", nameArabic: "صحيح البخاري", estBooks: 97, estHadiths: 7563 },
  { slug: "muslim", nameEnglish: "Sahih Muslim", nameArabic: "صحيح مسلم", estBooks: 56, estHadiths: 7470 },
  { slug: "abudawud", nameEnglish: "Sunan Abi Dawud", nameArabic: "سنن أبي داود", estBooks: 43, estHadiths: 5274 },
  { slug: "tirmidhi", nameEnglish: "Jami' at-Tirmidhi", nameArabic: "جامع الترمذي", estBooks: 49, estHadiths: 3956 },
  { slug: "nasai", nameEnglish: "Sunan an-Nasa'i", nameArabic: "سنن النسائي", estBooks: 51, estHadiths: 5761 },
  { slug: "ibnmajah", nameEnglish: "Sunan Ibn Majah", nameArabic: "سنن ابن ماجه", estBooks: 37, estHadiths: 4341 },
  { slug: "ahmad", nameEnglish: "Musnad Ahmad", nameArabic: "مسند أحمد", estBooks: 6, estHadiths: 26363 },
  { slug: "malik", nameEnglish: "Muwatta Malik", nameArabic: "موطأ مالك", estBooks: 61, estHadiths: 1594 },
  { slug: "darimi", nameEnglish: "Sunan ad-Darimi", nameArabic: "سنن الدارمي", estBooks: 23, estHadiths: 3503 },
  { slug: "riyadussalihin", nameEnglish: "Riyad as-Salihin", nameArabic: "رياض الصالحين", estBooks: 19, estHadiths: 1896 },
  { slug: "adab", nameEnglish: "Al-Adab Al-Mufrad", nameArabic: "الأدب المفرد", estBooks: 57, estHadiths: 1322 },
  { slug: "shamail", nameEnglish: "Ash-Shama'il Al-Muhammadiyah", nameArabic: "الشمائل المحمدية", estBooks: 55, estHadiths: 415 },
  { slug: "mishkat", nameEnglish: "Mishkat al-Masabih", nameArabic: "مشكاة المصابيح", estBooks: 29, estHadiths: 6294 },
  { slug: "bulugh", nameEnglish: "Bulugh al-Maram", nameArabic: "بلوغ المرام", estBooks: 16, estHadiths: 1358 },
  { slug: "nawawi40", nameEnglish: "An-Nawawi's 40 Hadith", nameArabic: "الأربعون النووية", estBooks: 1, estHadiths: 42 },
  { slug: "qudsi40", nameEnglish: "40 Hadith Qudsi", nameArabic: "الأربعون القدسية", estBooks: 1, estHadiths: 40 },
  { slug: "hisn", nameEnglish: "Hisn al-Muslim", nameArabic: "حصن المسلم", estBooks: 1, estHadiths: 100 },
] as const;

// Parse command line arguments
const args = process.argv.slice(2);
const collectionArg = args.find((arg) => arg.startsWith("--collection="))?.split("=")[1];
const resumeFromArg = args.find((arg) => arg.startsWith("--resume-from="))?.split("=")[1];
const htmlDirArg = args.find((arg) => arg.startsWith("--html-dir="))?.split("=")[1];
const allFlag = args.includes("--all");
const downloadOnlyFlag = args.includes("--download-only");
const processOnlyFlag = args.includes("--process-only");
const resumeFrom = resumeFromArg ? parseInt(resumeFromArg, 10) : 0;

// Default HTML directory
const HTML_DIR = htmlDirArg || "/Volumes/KIOXIA/sunnah-html";

// Rate limiting
const DELAY_BETWEEN_REQUESTS = 1500; // 1.5 seconds between requests

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ============================================================================
// Types
// ============================================================================

interface BookInfo {
  bookNumber: number;
  nameEnglish: string;
  nameArabic: string;
}

interface ScrapedHadith {
  hadithNumber: string;
  textArabic: string;
  chapterArabic?: string;
  chapterEnglish?: string;
}

interface CollectionProgress {
  status: "pending" | "downloading" | "downloaded" | "processing" | "complete";
  totalBooks: number;
  downloadedBooks: number;
  processedBooks: number;
  totalHadiths: number;
  lastBook?: number;
  error?: string;
}

interface Progress {
  lastUpdated: string;
  collections: Record<string, CollectionProgress>;
}

// ============================================================================
// Progress Tracking
// ============================================================================

function getProgressPath(): string {
  return path.join(HTML_DIR, "progress.json");
}

function loadProgress(): Progress {
  const progressPath = getProgressPath();
  if (fs.existsSync(progressPath)) {
    try {
      return JSON.parse(fs.readFileSync(progressPath, "utf-8"));
    } catch (e) {
      console.warn("Failed to load progress.json, starting fresh");
    }
  }
  return {
    lastUpdated: new Date().toISOString(),
    collections: {},
  };
}

function saveProgress(progress: Progress): void {
  progress.lastUpdated = new Date().toISOString();
  const progressPath = getProgressPath();
  fs.mkdirSync(path.dirname(progressPath), { recursive: true });
  fs.writeFileSync(progressPath, JSON.stringify(progress, null, 2));
}

function getCollectionProgress(progress: Progress, slug: string): CollectionProgress {
  if (!progress.collections[slug]) {
    const def = COLLECTIONS.find((c) => c.slug === slug);
    progress.collections[slug] = {
      status: "pending",
      totalBooks: def?.estBooks || 0,
      downloadedBooks: 0,
      processedBooks: 0,
      totalHadiths: 0,
    };
  }
  return progress.collections[slug];
}

// ============================================================================
// File Operations
// ============================================================================

function getCollectionDir(slug: string): string {
  return path.join(HTML_DIR, slug);
}

function ensureCollectionDir(slug: string): void {
  const dir = getCollectionDir(slug);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function saveHtml(slug: string, filename: string, html: string): void {
  ensureCollectionDir(slug);
  const filepath = path.join(getCollectionDir(slug), filename);
  fs.writeFileSync(filepath, html, "utf-8");
}

function loadHtml(slug: string, filename: string): string | null {
  const filepath = path.join(getCollectionDir(slug), filename);
  if (fs.existsSync(filepath)) {
    return fs.readFileSync(filepath, "utf-8");
  }
  return null;
}

function htmlExists(slug: string, filename: string): boolean {
  const filepath = path.join(getCollectionDir(slug), filename);
  return fs.existsSync(filepath);
}

function listHtmlFiles(slug: string): string[] {
  const dir = getCollectionDir(slug);
  if (!fs.existsSync(dir)) {
    return [];
  }
  return fs.readdirSync(dir).filter((f) => f.endsWith(".html"));
}

// ============================================================================
// Network Functions
// ============================================================================

async function fetchPage(url: string): Promise<string> {
  console.log(`  Fetching: ${url}`);
  const response = await fetch(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.5,ar;q=0.3",
    },
  });

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }

  return response.text();
}

// ============================================================================
// HTML Parsing
// ============================================================================

/**
 * Parse the collection index page to get list of books
 */
function parseBooksFromHtml(html: string, collectionSlug: string): BookInfo[] {
  const $ = cheerio.load(html);
  const books: BookInfo[] = [];

  // Find all book links - they follow the pattern /{collection}/{number}
  $("a[href]").each((_, el) => {
    const href = $(el).attr("href");
    const match = href?.match(new RegExp(`^/${collectionSlug}/(\\d+)$`));
    if (match) {
      const bookNumber = parseInt(match[1], 10);
      const text = $(el).text().trim();

      let nameEnglish = text;
      let nameArabic = "";

      // Try to extract Arabic text (RTL characters)
      const arabicMatch = text.match(/[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]+.*$/);
      if (arabicMatch) {
        nameArabic = arabicMatch[0].trim();
        nameEnglish = text.replace(arabicMatch[0], "").trim();
      }

      // Clean up English name
      nameEnglish = nameEnglish.replace(/^\d+\.\s*/, "").trim();

      if (!books.find((b) => b.bookNumber === bookNumber)) {
        books.push({ bookNumber, nameEnglish, nameArabic });
      }
    }
  });

  // Sort by book number
  books.sort((a, b) => a.bookNumber - b.bookNumber);

  return books;
}

/**
 * Improved parsing for aggregate book pages
 * Uses multiple strategies to find hadith content
 */
function parseHadithsFromHtml(html: string, collectionSlug: string): ScrapedHadith[] {
  const $ = cheerio.load(html);
  const hadiths: ScrapedHadith[] = [];
  const seenNumbers = new Set<string>();

  // Strategy 1: Find hadith containers by looking for reference links
  // sunnah.com uses links like /{collection}:{number}
  $(`a[href^="/${collectionSlug}:"]`).each((_, el) => {
    const href = $(el).attr("href");
    const match = href?.match(/:(\d+[a-z]?)$/);
    if (match) {
      const hadithNumber = match[1];
      if (seenNumbers.has(hadithNumber)) return;

      // Find the container that holds this hadith
      const $link = $(el);
      let container = $link.closest(".actualHadithContainer, .hadith_container, .hadithContainer");
      if (!container.length) {
        container = $link.closest("div").parent();
      }

      // Look for Arabic text in the container
      let textArabic = "";

      // Try specific Arabic text selectors
      const arabicSelectors = [
        ".arabic_hadith_full",
        ".arabic_sanad",
        ".text_details",
        ".hadith_narrated",
        "[class*='arabic']",
        "[lang='ar']",
      ];

      for (const selector of arabicSelectors) {
        container.find(selector).each((_, arabicEl) => {
          const text = $(arabicEl).text().trim();
          if (text && text.length > textArabic.length && /[\u0600-\u06FF]/.test(text)) {
            textArabic = text;
          }
        });
        if (textArabic.length > 50) break;
      }

      // Fallback: look for any substantial Arabic text in container
      if (!textArabic || textArabic.length < 50) {
        const containerHtml = container.html() || "";
        const $ = cheerio.load(containerHtml);
        $("*").each((_, node) => {
          const text = $(node).text().trim();
          const arabicMatch = text.match(/[\u0600-\u06FF\s،.؟:ـ«»؛]+/g);
          if (arabicMatch) {
            const combined = arabicMatch.join(" ").trim();
            if (combined.length > textArabic.length && combined.length > 50) {
              textArabic = combined;
            }
          }
        });
      }

      // Get chapter info
      let chapterArabic = "";
      let chapterEnglish = "";
      const chapterContainer = container.prevAll(".chapter, .chapter_title, [class*='chapter']").first();
      if (chapterContainer.length) {
        const text = chapterContainer.text().trim();
        if (/[\u0600-\u06FF]/.test(text)) {
          chapterArabic = text;
        } else {
          chapterEnglish = text;
        }
      }

      if (textArabic && textArabic.length > 20) {
        seenNumbers.add(hadithNumber);
        hadiths.push({
          hadithNumber,
          textArabic: textArabic.trim(),
          chapterArabic: chapterArabic || undefined,
          chapterEnglish: chapterEnglish || undefined,
        });
      }
    }
  });

  // Strategy 2: If no hadiths found, try direct container approach
  if (hadiths.length === 0) {
    $(".actualHadithContainer, .hadith_container, .hadithContainer, [class*='hadith']").each((_, container) => {
      const $container = $(container);

      // Get hadith number
      let hadithNumber = "";
      hadithNumber = $container.attr("data-hadith-number") || "";

      if (!hadithNumber) {
        const numMatch = $container.text().match(/Hadith\s+(\d+[a-z]?)/i);
        if (numMatch) {
          hadithNumber = numMatch[1];
        }
      }

      if (!hadithNumber || seenNumbers.has(hadithNumber)) return;

      // Get Arabic text
      let textArabic = "";
      $container
        .find(".arabic_hadith_full, .arabic, .text_details, [class*='arabic'], [lang='ar']")
        .each((_, el) => {
          const text = $(el).text().trim();
          if (text && text.length > textArabic.length && /[\u0600-\u06FF]/.test(text)) {
            textArabic = text;
          }
        });

      if (!textArabic) {
        const containerText = $container.text();
        const arabicLines = containerText.split("\n").filter((line) => /[\u0600-\u06FF]/.test(line));
        if (arabicLines.length > 0) {
          textArabic = arabicLines.join(" ").trim();
        }
      }

      if (hadithNumber && textArabic && textArabic.length > 20) {
        seenNumbers.add(hadithNumber);
        hadiths.push({
          hadithNumber,
          textArabic: textArabic.trim(),
        });
      }
    });
  }

  return hadiths;
}

/**
 * Parse individual hadith page (for collections without book structure)
 */
function parseDirectHadithFromHtml(html: string, hadithNumber: string): ScrapedHadith | null {
  const $ = cheerio.load(html);

  // Look for Arabic text
  let textArabic = "";
  $(".arabic_hadith_full, .arabic, .text_details, [class*='arabic'], [lang='ar'], .hadith_narrated").each(
    (_, el) => {
      const text = $(el).text().trim();
      if (text && text.length > textArabic.length && /[\u0600-\u06FF]/.test(text)) {
        textArabic = text;
      }
    }
  );

  // Fallback: look for any substantial Arabic text
  if (!textArabic) {
    const bodyText = $("body").text();
    const arabicParts = bodyText.match(/[\u0600-\u06FF\s،.؟:]+/g) || [];
    const longestArabic = arabicParts.reduce((a, b) => (a.length > b.length ? a : b), "");
    if (longestArabic.length > 50) {
      textArabic = longestArabic.trim();
    }
  }

  // Get chapter info
  let chapterArabic = "";
  let chapterEnglish = "";
  $(".chapter, .chapter_title, .englishchapter, h3, h4").each((_, el) => {
    const text = $(el).text().trim();
    if (/[\u0600-\u06FF]/.test(text) && !chapterArabic) {
      chapterArabic = text;
    } else if (!/[\u0600-\u06FF]/.test(text) && text.length < 200 && !chapterEnglish) {
      chapterEnglish = text;
    }
  });

  if (textArabic) {
    return {
      hadithNumber,
      textArabic,
      chapterArabic: chapterArabic || undefined,
      chapterEnglish: chapterEnglish || undefined,
    };
  }

  return null;
}

// ============================================================================
// Download Phase
// ============================================================================

async function downloadCollection(collectionSlug: string, progress: Progress): Promise<void> {
  const collectionDef = COLLECTIONS.find((c) => c.slug === collectionSlug);
  if (!collectionDef) {
    throw new Error(`Unknown collection: ${collectionSlug}`);
  }

  const collProgress = getCollectionProgress(progress, collectionSlug);

  console.log(`\n${"=".repeat(60)}`);
  console.log(`Downloading: ${collectionDef.nameEnglish} (${collectionDef.nameArabic})`);
  console.log(`${"=".repeat(60)}\n`);

  // Download index page if not exists
  if (!htmlExists(collectionSlug, "index.html")) {
    console.log("Downloading collection index...");
    await delay(DELAY_BETWEEN_REQUESTS);
    try {
      const html = await fetchPage(`https://sunnah.com/${collectionSlug}`);
      saveHtml(collectionSlug, "index.html", html);
      console.log("  Saved index.html");
    } catch (error) {
      console.error("  Failed to download index:", error);
      collProgress.status = "pending";
      collProgress.error = String(error);
      saveProgress(progress);
      return;
    }
  } else {
    console.log("Index already downloaded");
  }

  collProgress.status = "downloading";
  saveProgress(progress);

  // Parse book list from index
  const indexHtml = loadHtml(collectionSlug, "index.html")!;
  const books = parseBooksFromHtml(indexHtml, collectionSlug);
  collProgress.totalBooks = books.length;
  saveProgress(progress);

  console.log(`Found ${books.length} books\n`);

  if (books.length === 0) {
    // Collection without book structure - download direct hadith pages
    console.log("No book structure, downloading direct hadith pages...");
    await downloadDirectHadiths(collectionSlug, progress);
    return;
  }

  // Download each book's aggregate page
  for (const book of books) {
    const filename = `${book.bookNumber}.html`;

    if (htmlExists(collectionSlug, filename)) {
      console.log(`Book ${book.bookNumber}: Already downloaded`);
      continue;
    }

    if (resumeFrom > 0 && book.bookNumber < resumeFrom) {
      console.log(`Book ${book.bookNumber}: Skipping (resuming from ${resumeFrom})`);
      continue;
    }

    console.log(`Book ${book.bookNumber}: Downloading... (${book.nameEnglish})`);
    await delay(DELAY_BETWEEN_REQUESTS);

    try {
      const html = await fetchPage(`https://sunnah.com/${collectionSlug}/${book.bookNumber}`);
      saveHtml(collectionSlug, filename, html);
      collProgress.downloadedBooks++;
      collProgress.lastBook = book.bookNumber;
      saveProgress(progress);
      console.log(`  Saved ${filename}`);
    } catch (error) {
      console.error(`  Failed to download book ${book.bookNumber}:`, error);
      collProgress.error = `Book ${book.bookNumber}: ${error}`;
      saveProgress(progress);
      // Continue to next book
    }
  }

  collProgress.status = "downloaded";
  collProgress.error = undefined;
  saveProgress(progress);

  console.log(`\nDownload complete for ${collectionSlug}`);
  console.log(`  Books downloaded: ${collProgress.downloadedBooks}/${collProgress.totalBooks}`);
}

async function downloadDirectHadiths(collectionSlug: string, progress: Progress): Promise<void> {
  const collProgress = getCollectionProgress(progress, collectionSlug);
  let hadithNum = 1;
  let consecutiveFailures = 0;
  const maxConsecutiveFailures = 5;
  let downloadedCount = 0;

  // Check for existing files to determine starting point
  const existingFiles = listHtmlFiles(collectionSlug).filter((f) => f.match(/^hadith_\d+\.html$/));
  if (existingFiles.length > 0) {
    const existingNums = existingFiles.map((f) => parseInt(f.match(/hadith_(\d+)\.html/)?.[1] || "0", 10));
    hadithNum = Math.max(...existingNums) + 1;
    downloadedCount = existingFiles.length;
    console.log(`Resuming from hadith ${hadithNum} (${downloadedCount} already downloaded)`);
  }

  while (consecutiveFailures < maxConsecutiveFailures) {
    const filename = `hadith_${hadithNum}.html`;

    if (htmlExists(collectionSlug, filename)) {
      hadithNum++;
      continue;
    }

    await delay(DELAY_BETWEEN_REQUESTS);

    try {
      const html = await fetchPage(`https://sunnah.com/${collectionSlug}:${hadithNum}`);

      // Check if it's a valid hadith page (not 404 or redirect)
      if (html.includes("hadith not found") || html.length < 1000) {
        consecutiveFailures++;
        console.log(`  Hadith ${hadithNum} not found (${consecutiveFailures}/${maxConsecutiveFailures})`);
      } else {
        consecutiveFailures = 0;
        saveHtml(collectionSlug, filename, html);
        downloadedCount++;
        console.log(`  Downloaded hadith ${hadithNum}`);
        collProgress.downloadedBooks = downloadedCount;
        saveProgress(progress);
      }
    } catch (error) {
      consecutiveFailures++;
      console.error(`  Failed to download hadith ${hadithNum}:`, error);
    }

    hadithNum++;
  }

  collProgress.status = "downloaded";
  collProgress.totalBooks = downloadedCount;
  saveProgress(progress);

  console.log(`\nDownloaded ${downloadedCount} hadith pages for ${collectionSlug}`);
}

// ============================================================================
// Process Phase
// ============================================================================

async function processCollection(collectionSlug: string, progress: Progress): Promise<void> {
  const collectionDef = COLLECTIONS.find((c) => c.slug === collectionSlug);
  if (!collectionDef) {
    throw new Error(`Unknown collection: ${collectionSlug}`);
  }

  const collProgress = getCollectionProgress(progress, collectionSlug);

  console.log(`\n${"=".repeat(60)}`);
  console.log(`Processing: ${collectionDef.nameEnglish} (${collectionDef.nameArabic})`);
  console.log(`${"=".repeat(60)}\n`);

  // Check if HTML files exist
  const htmlFiles = listHtmlFiles(collectionSlug);
  if (htmlFiles.length === 0) {
    console.log("No HTML files found. Run with --download-only first.");
    return;
  }

  collProgress.status = "processing";
  saveProgress(progress);

  // Get or create collection in database
  let collection = await prisma.hadithCollection.findUnique({
    where: { slug: collectionSlug },
  });

  if (!collection) {
    collection = await prisma.hadithCollection.create({
      data: {
        slug: collectionSlug,
        nameEnglish: collectionDef.nameEnglish,
        nameArabic: collectionDef.nameArabic,
      },
    });
    console.log(`Created collection: ${collectionSlug}`);
  }

  // Check if this is a direct-hadith collection
  const hasDirectHadiths = htmlFiles.some((f) => f.startsWith("hadith_"));
  if (hasDirectHadiths) {
    await processDirectHadiths(collection.id, collectionSlug, collectionDef, progress);
    return;
  }

  // Process book-based collection
  const indexHtml = loadHtml(collectionSlug, "index.html");
  if (!indexHtml) {
    console.log("No index.html found. Run with --download-only first.");
    return;
  }

  const books = parseBooksFromHtml(indexHtml, collectionSlug);
  console.log(`Found ${books.length} books in index\n`);

  // Check if this is a single-page collection (all hadiths on index page)
  if (books.length === 0) {
    console.log("No book structure found, checking if hadiths are on index page...");
    const hadiths = parseHadithsFromHtml(indexHtml, collectionSlug);

    if (hadiths.length > 0) {
      console.log(`Found ${hadiths.length} hadiths on index page (single-page collection)\n`);
      await processSinglePageCollection(collection.id, collectionSlug, collectionDef, hadiths, progress);
      return;
    }
  }

  let totalHadiths = 0;

  for (const bookInfo of books) {
    const filename = `${bookInfo.bookNumber}.html`;
    const bookHtml = loadHtml(collectionSlug, filename);

    if (!bookHtml) {
      console.log(`Book ${bookInfo.bookNumber}: HTML file not found, skipping`);
      continue;
    }

    console.log(`Book ${bookInfo.bookNumber}: Processing... (${bookInfo.nameEnglish})`);

    // Get or create book in database
    let book = await prisma.hadithBook.findUnique({
      where: {
        collectionId_bookNumber: {
          collectionId: collection.id,
          bookNumber: bookInfo.bookNumber,
        },
      },
    });

    if (!book) {
      book = await prisma.hadithBook.create({
        data: {
          collectionId: collection.id,
          bookNumber: bookInfo.bookNumber,
          nameEnglish: bookInfo.nameEnglish || `Book ${bookInfo.bookNumber}`,
          nameArabic: bookInfo.nameArabic || "",
        },
      });
    }

    // Parse hadiths from HTML
    const hadiths = parseHadithsFromHtml(bookHtml, collectionSlug);
    console.log(`  Found ${hadiths.length} hadiths`);

    // Store hadiths in database
    for (const hadith of hadiths) {
      try {
        await prisma.hadith.upsert({
          where: {
            bookId_hadithNumber: {
              bookId: book.id,
              hadithNumber: hadith.hadithNumber,
            },
          },
          update: {
            textArabic: hadith.textArabic,
            textPlain: normalizeArabicText(hadith.textArabic),
            chapterArabic: hadith.chapterArabic,
            chapterEnglish: hadith.chapterEnglish,
          },
          create: {
            bookId: book.id,
            hadithNumber: hadith.hadithNumber,
            textArabic: hadith.textArabic,
            textPlain: normalizeArabicText(hadith.textArabic),
            chapterArabic: hadith.chapterArabic,
            chapterEnglish: hadith.chapterEnglish,
          },
        });
        totalHadiths++;
      } catch (error) {
        console.error(`  Failed to save hadith ${hadith.hadithNumber}:`, error);
      }
    }

    collProgress.processedBooks++;
    saveProgress(progress);
  }

  collProgress.status = "complete";
  collProgress.totalHadiths = totalHadiths;
  saveProgress(progress);

  console.log(`\nTotal hadiths saved for ${collectionSlug}: ${totalHadiths}`);
}

async function processDirectHadiths(
  collectionId: number,
  collectionSlug: string,
  collectionDef: { nameEnglish: string; nameArabic: string },
  progress: Progress
): Promise<void> {
  const collProgress = getCollectionProgress(progress, collectionSlug);

  // Create a default book for these hadiths
  let book = await prisma.hadithBook.findFirst({
    where: {
      collectionId,
      bookNumber: 1,
    },
  });

  if (!book) {
    book = await prisma.hadithBook.create({
      data: {
        collectionId,
        bookNumber: 1,
        nameEnglish: collectionDef.nameEnglish,
        nameArabic: collectionDef.nameArabic,
      },
    });
  }

  const htmlFiles = listHtmlFiles(collectionSlug).filter((f) => f.startsWith("hadith_"));
  console.log(`Found ${htmlFiles.length} hadith HTML files\n`);

  let totalHadiths = 0;

  for (const filename of htmlFiles) {
    const match = filename.match(/hadith_(\d+)\.html/);
    if (!match) continue;

    const hadithNumber = match[1];
    const html = loadHtml(collectionSlug, filename);
    if (!html) continue;

    const hadith = parseDirectHadithFromHtml(html, hadithNumber);
    if (hadith) {
      try {
        await prisma.hadith.upsert({
          where: {
            bookId_hadithNumber: {
              bookId: book.id,
              hadithNumber: hadith.hadithNumber,
            },
          },
          update: {
            textArabic: hadith.textArabic,
            textPlain: normalizeArabicText(hadith.textArabic),
            chapterArabic: hadith.chapterArabic,
            chapterEnglish: hadith.chapterEnglish,
          },
          create: {
            bookId: book.id,
            hadithNumber: hadith.hadithNumber,
            textArabic: hadith.textArabic,
            textPlain: normalizeArabicText(hadith.textArabic),
            chapterArabic: hadith.chapterArabic,
            chapterEnglish: hadith.chapterEnglish,
          },
        });
        totalHadiths++;
        console.log(`  Saved hadith ${hadithNumber}`);
      } catch (error) {
        console.error(`  Failed to save hadith ${hadithNumber}:`, error);
      }
    }

    collProgress.processedBooks++;
    saveProgress(progress);
  }

  collProgress.status = "complete";
  collProgress.totalHadiths = totalHadiths;
  saveProgress(progress);

  console.log(`\nTotal hadiths saved: ${totalHadiths}`);
}

/**
 * Process single-page collections where all hadiths are on the index page
 * Examples: hisn (Hisn al-Muslim)
 */
async function processSinglePageCollection(
  collectionId: number,
  collectionSlug: string,
  collectionDef: { nameEnglish: string; nameArabic: string },
  hadiths: ScrapedHadith[],
  progress: Progress
): Promise<void> {
  const collProgress = getCollectionProgress(progress, collectionSlug);

  // Create a default book for these hadiths
  let book = await prisma.hadithBook.findFirst({
    where: {
      collectionId,
      bookNumber: 1,
    },
  });

  if (!book) {
    book = await prisma.hadithBook.create({
      data: {
        collectionId,
        bookNumber: 1,
        nameEnglish: collectionDef.nameEnglish,
        nameArabic: collectionDef.nameArabic,
      },
    });
  }

  let totalHadiths = 0;

  for (const hadith of hadiths) {
    try {
      await prisma.hadith.upsert({
        where: {
          bookId_hadithNumber: {
            bookId: book.id,
            hadithNumber: hadith.hadithNumber,
          },
        },
        update: {
          textArabic: hadith.textArabic,
          textPlain: normalizeArabicText(hadith.textArabic),
          chapterArabic: hadith.chapterArabic,
          chapterEnglish: hadith.chapterEnglish,
        },
        create: {
          bookId: book.id,
          hadithNumber: hadith.hadithNumber,
          textArabic: hadith.textArabic,
          textPlain: normalizeArabicText(hadith.textArabic),
          chapterArabic: hadith.chapterArabic,
          chapterEnglish: hadith.chapterEnglish,
        },
      });
      totalHadiths++;
    } catch (error) {
      console.error(`  Failed to save hadith ${hadith.hadithNumber}:`, error);
    }
  }

  collProgress.status = "complete";
  collProgress.totalHadiths = totalHadiths;
  collProgress.processedBooks = 1;
  saveProgress(progress);

  console.log(`Total hadiths saved for ${collectionSlug}: ${totalHadiths}`);
}

// ============================================================================
// Legacy Full Scrape (download + process in one go)
// ============================================================================

async function scrapeCollection(collectionSlug: string): Promise<void> {
  const progress = loadProgress();

  // Download phase
  await downloadCollection(collectionSlug, progress);

  // Process phase
  await processCollection(collectionSlug, progress);
}

// ============================================================================
// Main
// ============================================================================

async function main() {
  console.log("Sunnah.com Hadith Scraper");
  console.log("=".repeat(60));
  console.log(`HTML Directory: ${HTML_DIR}`);
  console.log(`Mode: ${downloadOnlyFlag ? "Download Only" : processOnlyFlag ? "Process Only" : "Full Scrape"}`);
  console.log("=".repeat(60));

  // Validate HTML directory
  if (downloadOnlyFlag || processOnlyFlag) {
    if (!fs.existsSync(HTML_DIR)) {
      if (downloadOnlyFlag) {
        console.log(`\nCreating HTML directory: ${HTML_DIR}`);
        fs.mkdirSync(HTML_DIR, { recursive: true });
      } else {
        console.error(`\nError: HTML directory does not exist: ${HTML_DIR}`);
        console.error("Run with --download-only first to download HTML files.");
        process.exit(1);
      }
    }
  }

  if (!collectionArg && !allFlag) {
    console.log("\nUsage:");
    console.log("  # Download HTML files only (Phase 1)");
    console.log("  bun run scripts/scrape-sunnah.ts --download-only --collection=bukhari");
    console.log("  bun run scripts/scrape-sunnah.ts --download-only --all");
    console.log("");
    console.log("  # Process downloaded HTML (Phase 2)");
    console.log("  bun run scripts/scrape-sunnah.ts --process-only --collection=bukhari");
    console.log("  bun run scripts/scrape-sunnah.ts --process-only --all");
    console.log("");
    console.log("  # Full scrape (download + process)");
    console.log("  bun run scripts/scrape-sunnah.ts --collection=nawawi40");
    console.log("  bun run scripts/scrape-sunnah.ts --all");
    console.log("");
    console.log("  # Custom HTML directory");
    console.log("  bun run scripts/scrape-sunnah.ts --download-only --html-dir=/path/to/html");
    console.log("");
    console.log("  # Resume from book number");
    console.log("  bun run scripts/scrape-sunnah.ts --download-only --collection=bukhari --resume-from=50");
    console.log("\nAvailable collections:");
    for (const c of COLLECTIONS) {
      console.log(`  ${c.slug.padEnd(20)} - ${c.nameEnglish} (~${c.estHadiths} hadiths)`);
    }
    return;
  }

  const progress = loadProgress();

  // Determine which collections to process
  const collectionsToProcess = allFlag
    ? COLLECTIONS.map((c) => c.slug)
    : collectionArg
      ? [collectionArg]
      : [];

  for (const slug of collectionsToProcess) {
    if (!COLLECTIONS.find((c) => c.slug === slug)) {
      console.error(`Unknown collection: ${slug}`);
      continue;
    }

    if (downloadOnlyFlag) {
      await downloadCollection(slug, progress);
    } else if (processOnlyFlag) {
      await processCollection(slug, progress);
    } else {
      await scrapeCollection(slug);
    }
  }

  // Print final progress summary
  console.log("\n" + "=".repeat(60));
  console.log("Progress Summary:");
  console.log("=".repeat(60));

  for (const [slug, prog] of Object.entries(progress.collections)) {
    console.log(
      `  ${slug.padEnd(20)} - ${prog.status.padEnd(12)} | Books: ${prog.downloadedBooks}/${prog.totalBooks} | Hadiths: ${prog.totalHadiths}`
    );
  }

  console.log("\n" + "=".repeat(60));
  console.log("Completed!");
  console.log("=".repeat(60));
}

main()
  .catch((e) => {
    console.error("\nScraping failed:");
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
