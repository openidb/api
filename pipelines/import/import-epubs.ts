/**
 * Import EPUB Files Script
 *
 * Imports all EPUB files from public/books into the database.
 * Reads metadata from Shamela backup files to get author, category, and other details.
 * Uses AI-powered transliteration for proper Arabic to Latin conversion.
 *
 * Usage: bun run scripts/import-epubs.ts [--backup-path /path/to/backup] [--skip-transliteration] [--skip-translation]
 *
 * Options:
 *   --backup-path /path    Path to Shamela backup files
 *   --skip-transliteration Skip AI transliteration (use fallback slugs)
 *   --skip-translation     Skip generating translations for book titles and TOC
 */

import "../env";
import { prisma } from "../../src/db";
import { transliterateArabic } from "../lib/transliterate";
import * as fs from "fs";
import * as path from "path";

const BOOKS_DIR = path.join(__dirname, "../../data/books");

// Default backup path - can be overridden with --backup-path flag
const DEFAULT_BACKUP_PATH = "/Volumes/KIOXIA/shamela-backup";

interface AuthorData {
  name: string;
  shamela_author_id: string;
  death_date_hijri?: string;
  birth_date_hijri?: string;
  death_date_gregorian?: string;
  birth_date_gregorian?: string;
  kunya?: string;
  nasab?: string;
  nisba?: string;
  laqab?: string;
  biography?: string;
  other_works?: Array<{ shamela_id: string; title: string }>;
}

interface BookOverview {
  shamela_id: string;
  title: { arabic: string };
  author?: {
    name: string;
    shamela_author_id?: string;
    nasab?: string;
    kunya?: string;
    nisba?: string;
    laqab?: string;
    other_works?: Array<{ shamela_id: string; title: string }>;
  };
  publication?: {
    publisher?: string;
    edition?: string;
    year_hijri?: string;
    year_gregorian?: string;
  };
  structure?: {
    total_volumes?: number;
    total_pages?: number;
    page_alignment_note?: string;
  };
  classification?: {
    category?: string;
    category_id?: string;
    keywords?: string[];
  };
  editorial?: {
    editor?: string;
    type?: string;
    verification_status?: string;
  };
  description?: string;
}

function parseArgs(): { backupPath: string; skipTransliteration: boolean; skipTranslation: boolean } {
  const args = process.argv.slice(2);
  let backupPath = DEFAULT_BACKUP_PATH;
  let skipTransliteration = false;
  let skipTranslation = false;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--backup-path" && args[i + 1]) {
      backupPath = args[i + 1];
      i++;
    }
    if (args[i] === "--skip-transliteration") {
      skipTransliteration = true;
    }
    if (args[i] === "--skip-translation") {
      skipTranslation = true;
    }
  }

  return { backupPath, skipTransliteration, skipTranslation };
}

// Cache for transliterations during this run
const transliterationCache = new Map<string, string>();

async function getTransliteration(
  arabicText: string,
  skipTransliteration: boolean
): Promise<string> {
  if (skipTransliteration || !arabicText) {
    return createFallbackSlug(arabicText);
  }

  // Check cache first
  if (transliterationCache.has(arabicText)) {
    return transliterationCache.get(arabicText)!;
  }

  // Will be populated by batch transliteration
  return createFallbackSlug(arabicText);
}

function createFallbackSlug(arabicText: string): string {
  if (!arabicText) return "";
  return arabicText
    .replace(/[\u064B-\u065F\u0670]/g, "") // Remove tashkeel
    .replace(/\s+/g, "_")
    .substring(0, 100);
}

function loadBookOverview(backupPath: string, bookId: string): BookOverview | null {
  const overviewPath = path.join(backupPath, "books", bookId, `book_${bookId}_overview.json`);

  if (!fs.existsSync(overviewPath)) {
    return null;
  }

  try {
    const content = fs.readFileSync(overviewPath, "utf-8");
    return JSON.parse(content);
  } catch (error) {
    console.error(`  ⚠ Failed to parse overview for book ${bookId}:`, error);
    return null;
  }
}

function loadAuthorData(backupPath: string, authorId: string): AuthorData | null {
  const authorPath = path.join(backupPath, "authors", authorId, `author_${authorId}_data.json`);

  if (!fs.existsSync(authorPath)) {
    return null;
  }

  try {
    const content = fs.readFileSync(authorPath, "utf-8");
    return JSON.parse(content);
  } catch (error) {
    console.error(`  ⚠ Failed to parse author ${authorId}:`, error);
    return null;
  }
}

function cleanDateString(date: string | undefined): string | null {
  if (!date || date === "٠٠٠" || date === "000" || date === "") {
    return null;
  }
  return date;
}

function createLatinSlug(arabicText: string, transliteration?: string): string {
  // Use transliteration if available, otherwise fallback to slug
  if (transliteration) {
    return transliteration;
  }
  return createFallbackSlug(arabicText);
}

async function ensureAuthor(
  backupPath: string,
  authorId: string,
  authorName: string,
  overviewAuthor?: BookOverview["author"]
): Promise<string> {
  // Load full author data from backup
  const authorData = loadAuthorData(backupPath, authorId);

  // Merge data from overview and full author data
  const name = authorData?.name || authorName;

  // Get transliteration from cache
  const transliteration = transliterationCache.get(name);
  const nameLatin = createLatinSlug(name, transliteration);

  // Check if author already exists
  const existing = await prisma.author.findUnique({
    where: { id: authorId },
  });

  if (existing) {
    // Update transliteration if it changed (from fallback slug to proper transliteration)
    const hasProperTransliteration = transliteration && !existing.nameLatin.includes("_");
    const transliterationChanged = transliteration && existing.nameLatin !== transliteration;

    if (hasProperTransliteration || transliterationChanged) {
      // Check if new nameLatin is unique (excluding current author)
      const otherWithSameName = await prisma.author.findFirst({
        where: {
          nameLatin: transliteration,
          id: { not: authorId },
        },
      });

      if (!otherWithSameName) {
        await prisma.author.update({
          where: { id: authorId },
          data: { nameLatin: transliteration },
        });
        console.log(`    → Updated author transliteration: ${transliteration}`);
      }
    }
    return existing.id;
  }

  // Check if nameLatin already exists and make unique if needed
  let finalNameLatin = nameLatin;
  let counter = 1;
  while (await prisma.author.findUnique({ where: { nameLatin: finalNameLatin } })) {
    finalNameLatin = `${nameLatin} (${counter})`;
    counter++;
  }

  try {
    const author = await prisma.author.create({
      data: {
        id: authorId,
        nameArabic: name,
        nameLatin: finalNameLatin,
        kunya: authorData?.kunya || overviewAuthor?.kunya || null,
        nasab: authorData?.nasab || overviewAuthor?.nasab || null,
        nisba: authorData?.nisba || overviewAuthor?.nisba || null,
        laqab: authorData?.laqab || overviewAuthor?.laqab || null,
        birthDateHijri: cleanDateString(authorData?.birth_date_hijri),
        deathDateHijri: cleanDateString(authorData?.death_date_hijri),
        birthDateGregorian: cleanDateString(authorData?.birth_date_gregorian),
        deathDateGregorian: cleanDateString(authorData?.death_date_gregorian),
        biography: authorData?.biography || null,
      },
    });

    console.log(`  📝 Created author: ${name} (ID: ${authorId})`);

    // Import other works as AuthorWork entries
    if (authorData?.other_works && authorData.other_works.length > 0) {
      let addedWorks = 0;
      for (const work of authorData.other_works) {
        try {
          // Check if already exists
          const existing = await prisma.authorWork.findFirst({
            where: {
              authorId: author.id,
              bookId: work.shamela_id,
            },
          });

          if (!existing) {
            await prisma.authorWork.create({
              data: {
                authorId: author.id,
                bookId: work.shamela_id,
                title: work.title,
              },
            });
            addedWorks++;
          }
        } catch (error) {
          // Ignore errors
        }
      }
      if (addedWorks > 0) {
        console.log(`    → Added ${addedWorks} other works`);
      }
    }

    return author.id;
  } catch (error: any) {
    if (error.code === "P2002") {
      // Unique constraint error - author might have been created by another process
      const existing = await prisma.author.findUnique({ where: { id: authorId } });
      if (existing) return existing.id;
    }
    throw error;
  }
}

async function ensureCategory(
  categoryName: string,
  categoryId?: string
): Promise<number> {
  // Check if category exists by name
  const existing = await prisma.category.findUnique({
    where: { nameArabic: categoryName },
  });

  if (existing) {
    return existing.id;
  }

  // Create new category
  const category = await prisma.category.create({
    data: {
      nameArabic: categoryName,
      nameEnglish: null,
      code: categoryId || null,
    },
  });

  console.log(`  📁 Created category: ${categoryName}`);
  return category.id;
}

// No default author - books must have author info from backup

async function ensureDefaultCategory(): Promise<number> {
  const defaultCategory = await prisma.category.upsert({
    where: { nameArabic: "عام" },
    update: {},
    create: {
      nameArabic: "عام",
      nameEnglish: "General",
    },
  });
  return defaultCategory.id;
}

interface BookToImport {
  shamelaId: string;
  filename: string;
  titleFromFilename: string;
  overview: BookOverview | null;
}

async function main() {
  const { backupPath, skipTransliteration, skipTranslation } = parseArgs();

  console.log("📚 Importing EPUB files to database...");
  console.log(`📂 Backup path: ${backupPath}`);
  console.log(`🔤 Transliteration: ${skipTransliteration ? "disabled" : "enabled"}`);
  console.log(`🌍 Translation: ${skipTranslation ? "disabled" : "enabled"}\n`);

  // Check if backup path exists
  const backupBooksPath = path.join(backupPath, "books");
  if (!fs.existsSync(backupBooksPath)) {
    console.log(`⚠ Backup path not found: ${backupBooksPath}`);
    console.log("  Will use default author/category for all books\n");
  }

  // Get all EPUB files
  const files = fs.readdirSync(BOOKS_DIR).filter((f) => f.endsWith(".epub"));
  console.log(`Found ${files.length} EPUB files\n`);

  // Phase 1: Collect all books and their metadata
  console.log("Phase 1: Loading metadata...");
  const booksToImport: BookToImport[] = [];
  const arabicTextsToTransliterate = new Set<string>();

  for (const filename of files) {
    const match = filename.match(/^(\d+)_(.+)\.epub$/);
    if (!match) continue;

    const shamelaId = match[1];
    const titleFromFilename = match[2].replace(/_/g, " ");
    const overview = loadBookOverview(backupPath, shamelaId);

    booksToImport.push({ shamelaId, filename, titleFromFilename, overview });

    // Collect Arabic texts that need transliteration
    const title = overview?.title?.arabic || titleFromFilename;
    arabicTextsToTransliterate.add(title);

    if (overview?.author?.name) {
      arabicTextsToTransliterate.add(overview.author.name);
    }

    // Also load author data to get full name
    if (overview?.author?.shamela_author_id) {
      const authorData = loadAuthorData(backupPath, overview.author.shamela_author_id);
      if (authorData?.name) {
        arabicTextsToTransliterate.add(authorData.name);
      }
    }
  }

  console.log(`  Found ${booksToImport.length} books to process`);
  console.log(`  Found ${arabicTextsToTransliterate.size} unique Arabic texts\n`);

  // Phase 2: Batch transliterate all Arabic texts
  if (!skipTransliteration && arabicTextsToTransliterate.size > 0) {
    console.log("Phase 2: Transliterating Arabic texts...");

    try {
      const textsArray = Array.from(arabicTextsToTransliterate);
      const transliterations = await transliterateArabic(textsArray, (done, total) => {
        process.stdout.write(`\r  Progress: ${done}/${total} texts`);
      });

      // Populate cache
      for (const [arabic, latin] of transliterations) {
        transliterationCache.set(arabic, latin);
      }

      console.log(`\n  ✓ Transliterated ${transliterationCache.size} texts\n`);
    } catch (error) {
      console.error("\n  ⚠ Transliteration failed, using fallback slugs:", error);
      console.log("  Tip: Set OPENROUTER_API_KEY or use --skip-transliteration\n");
    }
  } else if (skipTransliteration) {
    console.log("Phase 2: Skipping transliteration (--skip-transliteration flag)\n");
  }

  // Phase 3: Import books to database
  console.log("Phase 3: Importing books to database...");

  // Ensure default category exists
  const defaultCategoryId = await ensureDefaultCategory();

  let imported = 0;
  let updated = 0;
  let skipped = 0;
  const importedBookIds: string[] = [];

  for (const { shamelaId, filename, titleFromFilename, overview } of booksToImport) {
    // Determine author - skip books without author info
    if (!overview?.author?.shamela_author_id) {
      console.log(`  ⚠ Skipping ${shamelaId} - no author info in backup`);
      skipped++;
      continue;
    }

    let authorId: string;
    try {
      authorId = await ensureAuthor(
        backupPath,
        overview.author.shamela_author_id,
        overview.author.name,
        overview.author
      );
    } catch (error) {
      console.error(`  ⚠ Failed to create author for book ${shamelaId}:`, error);
      skipped++;
      continue;
    }

    // Determine category
    let categoryId = defaultCategoryId;
    if (overview?.classification?.category) {
      try {
        categoryId = await ensureCategory(
          overview.classification.category,
          overview.classification.category_id
        );
      } catch (error) {
        console.error(`  ⚠ Failed to create category for book ${shamelaId}:`, error);
      }
    }

    // Prepare book data
    const title = overview?.title?.arabic || titleFromFilename;
    const titleTransliteration = transliterationCache.get(title);

    const bookData = {
      titleArabic: title,
      titleLatin: createLatinSlug(title, titleTransliteration),
      filename: filename,
      authorId: authorId,
      categoryId: categoryId,
      // Publication info
      publicationYearHijri: overview?.publication?.year_hijri || null,
      publicationYearGregorian: overview?.publication?.year_gregorian || null,
      publicationEdition: overview?.publication?.edition || null,
      // Structure
      totalVolumes: overview?.structure?.total_volumes || 1,
      totalPages: overview?.structure?.total_pages || null,
      pageAlignmentNote: overview?.structure?.page_alignment_note || null,
      // Editorial
      verificationStatus: overview?.editorial?.verification_status || null,
      editorialType: overview?.editorial?.type || null,
      // Content
      descriptionHtml: overview?.description || null,
    };

    // Check if already exists
    const existing = await prisma.book.findUnique({
      where: { id: shamelaId },
    });

    if (existing) {
      // Update if transliteration changed or metadata improved
      const transliterationChanged = titleTransliteration && existing.titleLatin !== titleTransliteration;

      if (transliterationChanged) {
        await prisma.book.update({
          where: { id: shamelaId },
          data: bookData,
        });
        console.log(`  ↻ Updated ${title.substring(0, 40)}... (ID: ${shamelaId})`);
        updated++;
      } else {
        console.log(`  → ${title.substring(0, 40)}... (ID: ${shamelaId}) - already exists`);
        skipped++;
      }
      continue;
    }

    try {
      await prisma.book.create({
        data: {
          id: shamelaId,
          ...bookData,
        },
      });

      console.log(`  ✓ ${title.substring(0, 40)}... (ID: ${shamelaId})`);
      imported++;
      importedBookIds.push(shamelaId);
    } catch (error) {
      console.error(`  ✗ Failed to import ${filename}:`, error);
    }
  }

  // Phase 4: Translation generation (not yet implemented in openislamicdb)
  if (!skipTranslation && importedBookIds.length > 0) {
    console.log("\nPhase 4: Translation generation skipped (not yet implemented)");
  }

  console.log("\n" + "=".repeat(60));
  console.log("📊 IMPORT SUMMARY");
  console.log("=".repeat(60));
  console.log(`Imported: ${imported}`);
  console.log(`Updated:  ${updated}`);
  console.log(`Skipped:  ${skipped}`);
  console.log(`Total:    ${files.length}`);
  console.log("=".repeat(60));
  console.log("\n✨ Import completed!");
}

main()
  .catch((e) => {
    console.error("\n❌ Import failed:");
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
