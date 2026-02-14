/**
 * Sunnah.com Hadith English Translation Scraper
 *
 * Extracts English translations from existing HTML files scraped from sunnah.com
 * and stores them in the HadithTranslation table.
 *
 * Usage:
 *   bun run scripts/scrape-hadith-translations.ts --collection=bukhari
 *   bun run scripts/scrape-hadith-translations.ts --all
 */

import "../env";
import * as cheerio from "cheerio";
import { prisma } from "../../src/db";
import { hashHadithTranslation } from "../../src/utils/content-hash";
import * as fs from "fs";
import * as path from "path";

// Collection definitions
const COLLECTIONS = [
  { slug: "bukhari", nameEnglish: "Sahih al-Bukhari" },
  { slug: "muslim", nameEnglish: "Sahih Muslim" },
  { slug: "abudawud", nameEnglish: "Sunan Abi Dawud" },
  { slug: "tirmidhi", nameEnglish: "Jami' at-Tirmidhi" },
  { slug: "nasai", nameEnglish: "Sunan an-Nasa'i" },
  { slug: "ibnmajah", nameEnglish: "Sunan Ibn Majah" },
  { slug: "ahmad", nameEnglish: "Musnad Ahmad" },
  { slug: "malik", nameEnglish: "Muwatta Malik" },
  { slug: "darimi", nameEnglish: "Sunan ad-Darimi" },
  { slug: "riyadussalihin", nameEnglish: "Riyad as-Salihin" },
  { slug: "adab", nameEnglish: "Al-Adab Al-Mufrad" },
  { slug: "shamail", nameEnglish: "Ash-Shama'il Al-Muhammadiyah" },
  { slug: "mishkat", nameEnglish: "Mishkat al-Masabih" },
  { slug: "bulugh", nameEnglish: "Bulugh al-Maram" },
  { slug: "nawawi40", nameEnglish: "An-Nawawi's 40 Hadith" },
  { slug: "qudsi40", nameEnglish: "40 Hadith Qudsi" },
  { slug: "hisn", nameEnglish: "Hisn al-Muslim" },
] as const;

// Parse command line arguments
const args = process.argv.slice(2);
const collectionArg = args.find((arg) => arg.startsWith("--collection="))?.split("=")[1];
const htmlDirArg = args.find((arg) => arg.startsWith("--html-dir="))?.split("=")[1];
const allFlag = args.includes("--all");

// Default HTML directory
const HTML_DIR = htmlDirArg || "/Volumes/KIOXIA/sunnah-html";

// ============================================================================
// Types
// ============================================================================

interface TranslationEntry {
  hadithNumber: string;
  text: string;
}

// ============================================================================
// File Operations
// ============================================================================

function getCollectionDir(slug: string): string {
  return path.join(HTML_DIR, slug);
}

function loadHtml(slug: string, filename: string): string | null {
  const filepath = path.join(getCollectionDir(slug), filename);
  if (fs.existsSync(filepath)) {
    return fs.readFileSync(filepath, "utf-8");
  }
  return null;
}

function listHtmlFiles(slug: string): string[] {
  const dir = getCollectionDir(slug);
  if (!fs.existsSync(dir)) {
    return [];
  }
  return fs.readdirSync(dir).filter((f) => f.endsWith(".html"));
}

// ============================================================================
// HTML Parsing
// ============================================================================

/**
 * Parse English translations from book HTML page
 */
function parseEnglishFromBookHtml(html: string, collectionSlug: string): TranslationEntry[] {
  const $ = cheerio.load(html);
  const translations: TranslationEntry[] = [];
  const seenNumbers = new Set<string>();

  // Find hadith containers
  $(".hadithTextContainers").each((_, container) => {
    const $container = $(container);

    // Get hadith number from reference link or sticky reference
    let hadithNumber = "";

    // Try getting from reference sticky (e.g., "Sahih al-Bukhari 1", "Sahih Muslim 8 a")
    const stickyRef = $container.prevAll(".hadith_reference_sticky").first().text().trim();
    const stickyMatch = stickyRef.match(/(\d+)\s?([a-z]?)$/i);
    if (stickyMatch) {
      hadithNumber = stickyMatch[1] + stickyMatch[2].toLowerCase();
    }

    // Fallback: try anchor name
    if (!hadithNumber) {
      const anchorName = $container.prev("a[name]").attr("name");
      if (anchorName && /^\d+[a-z]?$/i.test(anchorName)) {
        hadithNumber = anchorName;
      }
    }

    // Fallback: try reference link
    if (!hadithNumber) {
      const refLink = $container.find(`a[href^="/${collectionSlug}:"]`).attr("href");
      const refMatch = refLink?.match(/:(\d+)\s?([a-z]?)$/i);
      if (refMatch) {
        hadithNumber = refMatch[1] + refMatch[2].toLowerCase();
      }
    }

    if (!hadithNumber || seenNumbers.has(hadithNumber)) return;

    // Get English text from englishcontainer
    const $englishContainer = $container.find(".englishcontainer");
    if (!$englishContainer.length) return;

    // Get narrated part and text details
    const narrated = $englishContainer.find(".hadith_narrated").text().trim();
    const textDetails = $englishContainer.find(".text_details").text().trim();

    // Combine narrated and text details
    let englishText = "";
    if (narrated && textDetails) {
      englishText = `${narrated}\n\n${textDetails}`;
    } else if (textDetails) {
      englishText = textDetails;
    } else if (narrated) {
      englishText = narrated;
    }

    // Clean up whitespace
    englishText = englishText.replace(/\s+/g, " ").trim();

    if (englishText && englishText.length > 10) {
      seenNumbers.add(hadithNumber);
      translations.push({
        hadithNumber,
        text: englishText,
      });
    }
  });

  return translations;
}

/**
 * Parse English translation from individual hadith HTML page
 */
function parseEnglishFromHadithPage(html: string, hadithNumber: string): TranslationEntry | null {
  const $ = cheerio.load(html);

  // Get English text
  const narrated = $(".hadith_narrated").first().text().trim();
  const textDetails = $(".text_details").first().text().trim();

  let englishText = "";
  if (narrated && textDetails) {
    englishText = `${narrated}\n\n${textDetails}`;
  } else if (textDetails) {
    englishText = textDetails;
  } else if (narrated) {
    englishText = narrated;
  }

  // Clean up whitespace
  englishText = englishText.replace(/\s+/g, " ").trim();

  if (englishText && englishText.length > 10) {
    return {
      hadithNumber,
      text: englishText,
    };
  }

  return null;
}

// ============================================================================
// Database Operations
// ============================================================================

async function storeTranslation(bookId: number, hadithNumber: string, text: string, collectionSlug: string): Promise<void> {
  const contentHash = hashHadithTranslation(collectionSlug, hadithNumber, "en", text);
  await prisma.hadithTranslation.upsert({
    where: {
      bookId_hadithNumber_language: {
        bookId,
        hadithNumber,
        language: "en",
      },
    },
    update: {
      text,
      contentHash,
    },
    create: {
      bookId,
      hadithNumber,
      language: "en",
      text,
      contentHash,
    },
  });
}

// ============================================================================
// Main Processing
// ============================================================================

async function processCollection(collectionSlug: string): Promise<void> {
  const collectionDef = COLLECTIONS.find((c) => c.slug === collectionSlug);
  if (!collectionDef) {
    throw new Error(`Unknown collection: ${collectionSlug}`);
  }

  console.log(`\n${"=".repeat(60)}`);
  console.log(`Processing: ${collectionDef.nameEnglish}`);
  console.log(`${"=".repeat(60)}\n`);

  // Get collection from database
  const collection = await prisma.hadithCollection.findUnique({
    where: { slug: collectionSlug },
    include: { books: true },
  });

  if (!collection) {
    console.log(`Collection ${collectionSlug} not found in database. Run scrape-sunnah.ts first.`);
    return;
  }

  // Check if HTML files exist
  const htmlFiles = listHtmlFiles(collectionSlug);
  if (htmlFiles.length === 0) {
    console.log("No HTML files found.");
    return;
  }

  console.log(`Found ${htmlFiles.length} HTML files`);
  console.log(`Found ${collection.books.length} books in database\n`);

  // Check if this is a direct-hadith collection
  const hasDirectHadiths = htmlFiles.some((f) => f.startsWith("hadith_"));

  if (hasDirectHadiths) {
    // Process individual hadith pages
    const book = collection.books[0];
    if (!book) {
      console.log("No book found in database for collection.");
      return;
    }

    let totalTranslations = 0;

    for (const filename of htmlFiles) {
      const match = filename.match(/hadith_(\d+)\.html/);
      if (!match) continue;

      const hadithNumber = match[1];
      const html = loadHtml(collectionSlug, filename);
      if (!html) continue;

      const translation = parseEnglishFromHadithPage(html, hadithNumber);
      if (translation) {
        await storeTranslation(book.id, translation.hadithNumber, translation.text, collectionSlug);
        totalTranslations++;
      }
    }

    console.log(`Stored ${totalTranslations} translations`);
  } else {
    // Process book-based HTML files
    let totalTranslations = 0;

    for (const book of collection.books) {
      const filename = `${book.bookNumber}.html`;
      const html = loadHtml(collectionSlug, filename);

      if (!html) {
        console.log(`Book ${book.bookNumber}: HTML file not found`);
        continue;
      }

      console.log(`Book ${book.bookNumber}: Parsing...`);

      const translations = parseEnglishFromBookHtml(html, collectionSlug);
      console.log(`  Found ${translations.length} translations`);

      for (const trans of translations) {
        await storeTranslation(book.id, trans.hadithNumber, trans.text, collectionSlug);
        totalTranslations++;
      }
    }

    console.log(`\nTotal translations stored: ${totalTranslations}`);
  }
}

// ============================================================================
// Main
// ============================================================================

async function main() {
  console.log("Sunnah.com Hadith English Translation Scraper");
  console.log("=".repeat(60));
  console.log(`HTML Directory: ${HTML_DIR}`);
  console.log("=".repeat(60));

  // Validate HTML directory
  if (!fs.existsSync(HTML_DIR)) {
    console.error(`Error: HTML directory does not exist: ${HTML_DIR}`);
    process.exit(1);
  }

  if (!collectionArg && !allFlag) {
    console.log("\nUsage:");
    console.log("  bun run scripts/scrape-hadith-translations.ts --collection=bukhari");
    console.log("  bun run scripts/scrape-hadith-translations.ts --all");
    console.log("");
    console.log("  # Custom HTML directory");
    console.log("  bun run scripts/scrape-hadith-translations.ts --html-dir=/path/to/html --all");
    console.log("\nAvailable collections:");
    for (const c of COLLECTIONS) {
      console.log(`  ${c.slug.padEnd(20)} - ${c.nameEnglish}`);
    }
    return;
  }

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

    await processCollection(slug);
  }

  console.log("\n" + "=".repeat(60));
  console.log("Completed!");
  console.log("=".repeat(60));
}

main()
  .catch((e) => {
    console.error("\nProcessing failed:");
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
