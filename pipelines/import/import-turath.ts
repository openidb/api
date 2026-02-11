/**
 * Import Book from Turath API
 *
 * Fetches a book directly from Turath.io's API and imports it into the database.
 * Turath uses the same IDs as Shamela and provides volume numbers + printed page numbers
 * that our EPUB pipeline currently lacks.
 *
 * Usage:
 *   bun run pipelines/import/import-turath.ts --id=4                    # Import book 4
 *   bun run pipelines/import/import-turath.ts --id=4 --dry-run          # Preview only
 */

import "../env";
import { prisma } from "../../src/db";
import { hashPage } from "../../src/utils/content-hash";
import { s3, BUCKET_NAME } from "../../src/s3";
import { ensureBucket } from "../../src/utils/s3-bucket";
import { HeadObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------

function parseArgs(): { id: string; dryRun: boolean } {
  const args = process.argv.slice(2);
  let id = "";
  let dryRun = false;

  for (const arg of args) {
    if (arg.startsWith("--id=")) {
      id = arg.slice(5);
    } else if (arg === "--dry-run") {
      dryRun = true;
    }
  }

  if (!id) {
    console.error("Usage: bun run pipelines/import/import-turath.ts --id=<book_id> [--dry-run]");
    process.exit(1);
  }

  return { id, dryRun };
}

// ---------------------------------------------------------------------------
// Turath API types
// ---------------------------------------------------------------------------

/** Obfuscated key mappings for the books-v3 content JSON */
const META_KEYS = {
  "\u064B": "meta",       // ً  — top-level metadata object
  "\u064C": "id",         // ٌ
  "\u064D": "name",       // ٍ
  "\u064E": "type",       // َ
  "\u064F": "printed",    // ُ  (1=matches print, 3=auto-numbered)
  "\u0650": "pdf_links",  // ِ
  "\u0651": "info",       // ّ
  "\u0652": "info_long",  // ْ
  "\u0653": "version",    // ٓ
  "\u0654": "author_id",  // ٔ
  "\u0655": "cat_id",     // ٕ
  "\u0656": "date_built", // ٖ
  "\u0657": "author_page_start", // ٗ
} as const;

const STRUCTURE_KEYS = {
  "\u0658": "structure",    // ٘  — top-level structure object
  "\u0659": "vol_labels",   // ٙ  — volume label array
  "\u065A": "toc",          // ٚ  — table of contents
  "\u065B": "page_map",     // ٛ  — vol+page → sequential index
  "\u065C": "vol_bounds",   // ٜ  — volume page ranges
} as const;

interface TurathMeta {
  id: number;
  name: string;
  type: number;
  printed: number;
  pdf_links: unknown;
  info: string;
  info_long: string;
  version: string;
  author_id: number;
  cat_id: number;
  date_built: number;
  author_page_start: number;
}

interface TurathPage {
  text: string;
  vol: string;
  page: number;
}

interface TurathContent {
  meta: TurathMeta;
  vol_labels: string[];
  toc: Array<{ title: string; level: number; page: number }>;
  pages: TurathPage[];
}

interface TurathBookApiResponse {
  meta: {
    id: number;
    name: string;
    author_id: number;
    cat_id: number;
    info: string;
    pdf_links: unknown;
    author_page_start: number;
  };
}

interface TurathAuthorApiResponse {
  info: string; // JSON-encoded string with name, biography, death, etc.
}

// ---------------------------------------------------------------------------
// Fetch helpers
// ---------------------------------------------------------------------------

async function fetchJson<T>(url: string, label: string): Promise<T> {
  console.log(`  Fetching ${label}...`);
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`Failed to fetch ${label}: ${res.status} ${res.statusText}`);
  }
  return res.json() as Promise<T>;
}

async function fetchTurathContent(id: string): Promise<TurathContent> {
  const raw = await fetchJson<Record<string, unknown>>(
    `https://files.turath.io/books-v3/${id}.json`,
    "content"
  );

  // Decode obfuscated metadata
  const rawMeta = raw["\u064B"] as Record<string, unknown> | undefined;
  if (!rawMeta) {
    throw new Error("Content file missing metadata (ً key)");
  }

  const meta: TurathMeta = {
    id: rawMeta["\u064C"] as number,
    name: rawMeta["\u064D"] as string,
    type: rawMeta["\u064E"] as number,
    printed: rawMeta["\u064F"] as number,
    pdf_links: rawMeta["\u0650"],
    info: rawMeta["\u0651"] as string || "",
    info_long: rawMeta["\u0652"] as string || "",
    version: rawMeta["\u0653"] as string || "",
    author_id: rawMeta["\u0654"] as number,
    cat_id: rawMeta["\u0655"] as number,
    date_built: rawMeta["\u0656"] as number,
    author_page_start: rawMeta["\u0657"] as number || 0,
  };

  // Decode structure
  const rawStructure = raw["\u0658"] as Record<string, unknown> | undefined;
  const vol_labels = (rawStructure?.["\u0659"] as string[]) || [];
  const toc = (rawStructure?.["\u065A"] as Array<{ title: string; level: number; page: number }>) || [];

  // Pages array is stored under plain "pages" key
  const pages = (raw["pages"] as TurathPage[]) || [];

  return { meta, vol_labels, toc, pages };
}

async function fetchTurathAuthor(authorId: number): Promise<{ name: string; biography: string | null; deathDateHijri: string | null; birthDateHijri: string | null }> {
  try {
    const data = await fetchJson<TurathAuthorApiResponse>(
      `https://api.turath.io/author?id=${authorId}`,
      "author"
    );

    // The info field is a JSON-encoded string
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(data.info);
    } catch {
      // Sometimes it's double-encoded
      try {
        parsed = JSON.parse(JSON.parse(data.info));
      } catch {
        return { name: "", biography: null, deathDateHijri: null, birthDateHijri: null };
      }
    }

    return {
      name: (parsed.name as string) || "",
      biography: (parsed.bio as string) || (parsed.biography as string) || null,
      deathDateHijri: (parsed.death as string) || null,
      birthDateHijri: (parsed.birth as string) || null,
    };
  } catch (error) {
    console.warn(`  Warning: Could not fetch author ${authorId}:`, error);
    return { name: "", biography: null, deathDateHijri: null, birthDateHijri: null };
  }
}

// ---------------------------------------------------------------------------
// HTML stripping
// ---------------------------------------------------------------------------

function stripHtml(html: string): string {
  return (
    html
      .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, "")
      .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, "")
      .replace(/&nbsp;/g, " ")
      .replace(/&amp;/g, "&")
      .replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">")
      .replace(/&quot;/g, '"')
      .replace(/&#39;/g, "'")
      .replace(/<\/?(p|div|br|h[1-6]|li|tr)[^>]*>/gi, "\n")
      .replace(/<[^>]+>/g, "")
      .replace(/\n{3,}/g, "\n\n")
      .replace(/[ \t]+/g, " ")
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0)
      .join("\n")
      .trim()
  );
}

// ---------------------------------------------------------------------------
// Content detection
// ---------------------------------------------------------------------------

function detectContentFlags(text: string) {
  return {
    hasPoetry: /(\d+)\s*-\s*[^\d]/.test(text) || /[\u0640]{2,}/.test(text),
    hasHadith: /قال رسول الله|صلى الله عليه وسلم|حدثنا|أخبرنا/.test(text),
    hasQuran: /\{[^}]+\}|﴿[^﴾]+﴾|قال تعالى/.test(text),
    hasDialogue: /قال:|قلت:|فقال:|قالوا:|قالت:/.test(text),
  };
}

// ---------------------------------------------------------------------------
// Arabic-Indic digit conversion
// ---------------------------------------------------------------------------

function toWesternDigits(s: string): string {
  return s.replace(/[٠-٩]/g, (d) => String("٠١٢٣٤٥٦٧٨٩".indexOf(d)));
}

// ---------------------------------------------------------------------------
// Parse structured `info` field from Turath metadata
// ---------------------------------------------------------------------------

interface ParsedInfo {
  editor: string | null;
  publisher: string | null;
  publisherLocation: string | null;
  edition: string | null;
  yearHijri: string | null;
  yearGregorian: string | null;
  pageAlignmentNote: string | null;
  isVerified: boolean;
  authorBirthDateHijri: string | null;
  authorDeathDateHijri: string | null;
}

function parseInfoField(info: string): ParsedInfo {
  const result: ParsedInfo = {
    editor: null,
    publisher: null,
    publisherLocation: null,
    edition: null,
    yearHijri: null,
    yearGregorian: null,
    pageAlignmentNote: null,
    isVerified: false,
    authorBirthDateHijri: null,
    authorDeathDateHijri: null,
  };

  if (!info) return result;

  const lines = info.split("\n");

  for (const line of lines) {
    const trimmed = line.trim();

    // المحقق: (editor)
    const editorMatch = trimmed.match(/^المحقق\s*:\s*(.+)/);
    if (editorMatch) {
      result.editor = editorMatch[1].trim();
      result.isVerified = true;
      continue;
    }

    // الناشر: (publisher — may include location after ، or -)
    const publisherMatch = trimmed.match(/^الناشر\s*:\s*(.+)/);
    if (publisherMatch) {
      const raw = publisherMatch[1].trim();
      // Split on " - " or "، " or "," to separate name from location
      const sepMatch = raw.match(/^(.+?)\s*[-،,]\s+(.+)$/);
      if (sepMatch) {
        result.publisher = sepMatch[1].trim();
        result.publisherLocation = sepMatch[2].trim();
      } else {
        result.publisher = raw;
      }
      continue;
    }

    // الطبعة: (edition + year)
    const editionMatch = trimmed.match(/^الطبعة\s*:\s*(.+)/);
    if (editionMatch) {
      const raw = editionMatch[1].trim();
      result.edition = raw;

      // Extract Hijri year (3-4 Arabic-Indic or Western digits followed by هـ)
      const hijriMatch = toWesternDigits(raw).match(/(\d{3,4})\s*هـ/);
      if (hijriMatch) {
        result.yearHijri = hijriMatch[1];
      }

      // Extract Gregorian year (4 digits followed by م)
      const gregMatch = toWesternDigits(raw).match(/(\d{4})\s*م/);
      if (gregMatch) {
        result.yearGregorian = gregMatch[1];
      }
      continue;
    }

    // Bracketed alignment notes: [ترقيم الكتاب موافق للمطبوع] or [الكتاب مرقم آليا]
    const bracketMatch = trimmed.match(/\[([^\]]+)\]/);
    if (bracketMatch) {
      const note = bracketMatch[1].trim();
      if (note.includes("ترقيم") || note.includes("مرقم") || note.includes("موافق")) {
        result.pageAlignmentNote = note;
      }
      continue;
    }

    // المؤلف: line — extract birth/death dates
    const authorMatch = trimmed.match(/^المؤلف\s*:\s*(.+)/);
    if (authorMatch) {
      const authorLine = toWesternDigits(authorMatch[1]);
      // Pattern: (birth - death هـ) e.g. (٧٧٣ - ٨٥٢ هـ)
      const birthDeathMatch = authorLine.match(/\((\d{1,4})\s*[-–]\s*(\d{1,4})\s*هـ?\)/);
      if (birthDeathMatch) {
        result.authorBirthDateHijri = birthDeathMatch[1];
        result.authorDeathDateHijri = birthDeathMatch[2];
      } else {
        // Pattern: (ت death هـ) e.g. (ت ٣٨٥ هـ)
        const deathOnlyMatch = authorLine.match(/\(ت\s*(\d{1,4})\s*هـ?\)/);
        if (deathOnlyMatch) {
          result.authorDeathDateHijri = deathOnlyMatch[1];
        }
      }
      continue;
    }
  }

  return result;
}

// ---------------------------------------------------------------------------
// PDF URL builder
// ---------------------------------------------------------------------------

function buildPdfUrl(
  pdfLinks: { root?: string; files?: string[] } | null,
  volumeIndex: number
): string | null {
  if (!pdfLinks || !pdfLinks.files || pdfLinks.files.length === 0) {
    return null;
  }

  const rawFile = pdfLinks.files[Math.min(volumeIndex, pdfLinks.files.length - 1)];
  if (!rawFile) return null;

  // Strip volume label after "|" (e.g. "01p_2021.pdf|المقدمة" → "01p_2021.pdf")
  const file = rawFile.split("|")[0];

  // Pattern 1: No root — files are full URLs (e.g. https://archive.org/download/...)
  if (!pdfLinks.root) {
    return file.startsWith("http") ? file : null;
  }

  // Pattern 2: Archive.org or other full-URL root
  if (pdfLinks.root.startsWith("http")) {
    const root = pdfLinks.root.endsWith("/") ? pdfLinks.root : pdfLinks.root + "/";
    return `${root}${file}`;
  }

  // Pattern 3: Turath-hosted — relative Arabic path under files.turath.io/pdf/
  const root = pdfLinks.root.endsWith("/") ? pdfLinks.root : pdfLinks.root + "/";
  return encodeURI(`https://files.turath.io/pdf/${root}${file}`);
}

// ---------------------------------------------------------------------------
// Overview page content generator (Turath plain-text format)
// ---------------------------------------------------------------------------

function generateOverviewContent(
  bookTitle: string,
  authorName: string,
  info: string,
  toc: Array<{ title: string; level: number; page: number }>,
): string {
  // Use the same plain-text-with-newlines format that Turath pages use,
  // so the HtmlReader's formatContentHtml() renders it correctly.
  const lines: string[] = [];

  lines.push(`<span data-type="title">${bookTitle}</span>`);
  lines.push(authorName);
  lines.push("");

  // Info block lines
  if (info) {
    for (const line of info.split("\n")) {
      const trimmed = line.trim();
      if (trimmed) {
        lines.push(trimmed);
      }
    }
    lines.push("");
  }

  // TOC section with clickable page links
  if (toc.length > 0) {
    lines.push(`<span data-type="title">فهرس الكتاب</span>`);
    lines.push("");
    for (const entry of toc) {
      const indent = "  ".repeat(entry.level);
      lines.push(`${indent}<a data-page="${entry.page}" style="cursor:pointer;color:inherit;text-decoration:underline;text-decoration-style:dotted">${entry.title} — ص ${entry.page}</a>`);
    }
  }

  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Volume label → number mapping
// ---------------------------------------------------------------------------

function buildVolumeLabelMap(volLabels: string[]): Map<string, number> {
  const map = new Map<string, number>();
  for (let i = 0; i < volLabels.length; i++) {
    const label = volLabels[i];
    // Numeric labels map directly
    const num = parseInt(label, 10);
    if (!isNaN(num)) {
      map.set(label, num);
    } else {
      // Named volumes (e.g. "المقدمة") → use 1-based index position
      // but offset to 0 if it's the first (introduction) volume
      map.set(label, i === 0 ? 0 : i);
    }
  }
  return map;
}

function resolveVolumeNumber(vol: string, labelMap: Map<string, number>): number {
  // Direct lookup from volume labels
  if (labelMap.has(vol)) {
    return labelMap.get(vol)!;
  }

  // Try numeric parse
  const num = parseInt(vol, 10);
  if (!isNaN(num)) return num;

  // Fallback
  return 1;
}

// ---------------------------------------------------------------------------
// Category extraction from info text
// ---------------------------------------------------------------------------

/** Turath cat_id → Arabic category name */
const TURATH_CATEGORIES: Record<number, string> = {
  0: "عام",
  1: "العقيدة",
  2: "التفاسير",
  3: "علوم القرآن",
  4: "الأحاديث والآثار",
  5: "شروح الحديث",
  6: "علوم الحديث",
  7: "الفقه العام",
  8: "الفقه الحنفي",
  9: "الفقه المالكي",
  10: "الفقه الشافعي",
  11: "الفقه الحنبلي",
  12: "فقه عام",
  13: "أصول الفقه والقواعد الفقهية",
  14: "اللغة العربية",
  15: "البلاغة",
  16: "الأدب والبلاغة",
  17: "التراجم والطبقات",
  18: "الأنساب",
  19: "التاريخ",
  20: "السيرة النبوية",
  21: "الرقائق والزهد",
  22: "الآداب والأخلاق",
  23: "الدعوة",
  24: "التربية والتعليم",
  25: "الثقافة الإسلامية",
};

function getCategoryName(catId: number): string {
  return TURATH_CATEGORIES[catId] || "عام";
}

// ---------------------------------------------------------------------------
// PDF download & S3 upload
// ---------------------------------------------------------------------------

async function isAlreadyUploaded(key: string): Promise<boolean> {
  try {
    await s3.send(new HeadObjectCommand({ Bucket: BUCKET_NAME, Key: key }));
    return true;
  } catch {
    return false;
  }
}

async function downloadAndStorePdf(
  sourceUrl: string,
  bookId: string,
  volumeIndex: number,
): Promise<string | null> {
  const key = `${bookId}/${volumeIndex}.pdf`;

  try {
    const res = await fetch(sourceUrl, {
      signal: AbortSignal.timeout(60_000),
    });
    if (!res.ok) {
      console.warn(`  [pdf] Failed to download ${sourceUrl}: ${res.status}`);
      return null;
    }

    const body = await res.arrayBuffer();

    await s3.send(
      new PutObjectCommand({
        Bucket: BUCKET_NAME,
        Key: key,
        Body: new Uint8Array(body),
        ContentType: "application/pdf",
        Metadata: { sourceUrl },
      }),
    );

    return key;
  } catch (err) {
    console.warn(`  [pdf] Error storing ${sourceUrl}:`, (err as Error).message);
    return null;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// Exported result type
// ---------------------------------------------------------------------------

export interface ImportResult {
  bookId: string;
  title: string;
  pages: number;
  success: boolean;
  error?: string;
}

// ---------------------------------------------------------------------------
// Core import logic (reusable)
// ---------------------------------------------------------------------------

export async function importTurathBook(
  id: string,
  options: { dryRun?: boolean } = {},
): Promise<ImportResult> {
  const { dryRun = false } = options;

  try {
    // 1. Fetch book metadata from Turath API
    console.log("Step 1: Fetching metadata...");
    const bookApi = await fetchJson<TurathBookApiResponse>(
      `https://api.turath.io/book?id=${id}`,
      "book metadata"
    );

    // 2. Fetch content file
    console.log("\nStep 2: Fetching content...");
    const content = await fetchTurathContent(id);

    // 3. Fetch author
    console.log("\nStep 3: Fetching author...");
    const authorInfo = await fetchTurathAuthor(content.meta.author_id);
    const authorName = authorInfo.name || bookApi.meta.name;

    // Build volume label map
    const volLabelMap = buildVolumeLabelMap(content.vol_labels);

    // Compute stats
    const volumeSet = new Set<number>();
    for (const page of content.pages) {
      volumeSet.add(resolveVolumeNumber(page.vol, volLabelMap));
    }
    const totalVolumes = Math.max(1, volumeSet.size);

    // Parse structured info field
    const parsedInfo = parseInfoField(content.meta.info);

    // Compute printed flag fallback for pageAlignmentNote
    const printedFlagNote = content.meta.printed === 1
      ? "ترقيم الكتاب موافق للمطبوع"
      : content.meta.printed === 3
      ? "الكتاب مرقم آليا"
      : null;

    // Parse PDF links
    const pdfLinks = content.meta.pdf_links as { root?: string; files?: string[] } | null;

    // Map volume label → 0-based index into pdf_links.files.
    // When pdf_links.files has more entries than vol_labels, the extra
    // prefix files are intros/covers (e.g. "تقديم") — offset accordingly.
    const pdfFileCount = pdfLinks?.files?.length ?? 0;
    const pdfOffset = Math.max(0, pdfFileCount - content.vol_labels.length);
    const volLabelToIndex = new Map<string, number>();
    for (let i = 0; i < content.vol_labels.length; i++) {
      volLabelToIndex.set(content.vol_labels[i], i + pdfOffset);
    }

    // Print summary
    console.log("\n" + "-".repeat(60));
    console.log("Book Summary:");
    console.log(`  Title:           ${content.meta.name}`);
    console.log(`  Author:          ${authorName} (ID: ${content.meta.author_id})`);
    console.log(`  Category ID:     ${content.meta.cat_id} (${getCategoryName(content.meta.cat_id)})`);
    console.log(`  Pages:           ${content.pages.length}`);
    console.log(`  Volumes:         ${totalVolumes} (labels: ${content.vol_labels.join(", ") || "none"})`);
    console.log(`  TOC entries:     ${content.toc.length}`);
    console.log(`  Printed match:   ${content.meta.printed === 1 ? "yes" : content.meta.printed === 3 ? "auto-numbered" : `unknown (${content.meta.printed})`}`);
    console.log(`  Type:            ${content.meta.type}`);

    // Show parsed info fields
    console.log("\nParsed Info:");
    console.log(`  Editor:          ${parsedInfo.editor || "(none)"}`);
    console.log(`  Publisher:       ${parsedInfo.publisher || "(none)"}`);
    console.log(`  Pub. location:   ${parsedInfo.publisherLocation || "(none)"}`);
    console.log(`  Edition:         ${parsedInfo.edition || "(none)"}`);
    console.log(`  Year (Hijri):    ${parsedInfo.yearHijri || "(none)"}`);
    console.log(`  Year (Greg.):    ${parsedInfo.yearGregorian || "(none)"}`);
    console.log(`  Alignment note:  ${parsedInfo.pageAlignmentNote || printedFlagNote || "(none)"}`);
    console.log(`  Verified:        ${parsedInfo.isVerified ? "yes (محقق)" : "no"}`);
    if (parsedInfo.authorBirthDateHijri) {
      console.log(`  Author birth:    ${parsedInfo.authorBirthDateHijri} هـ`);
    }
    if (parsedInfo.authorDeathDateHijri) {
      console.log(`  Author death:    ${parsedInfo.authorDeathDateHijri} هـ`);
    }

    // Show PDF info
    if (pdfLinks?.files && pdfLinks.files.length > 0) {
      const sampleUrl = buildPdfUrl(pdfLinks, 0);
      console.log(`\n  PDF files:       ${pdfLinks.files.length}`);
      console.log(`  PDF root:        ${pdfLinks.root || "(full URLs in files)"}`);
      if (sampleUrl) console.log(`  PDF sample:      ${sampleUrl.substring(0, 80)}${sampleUrl.length > 80 ? "..." : ""}`);
    }

    console.log("-".repeat(60));

    // Show first few pages as preview
    if (content.pages.length > 0) {
      console.log("\nPage preview (first 3):");
      for (const page of content.pages.slice(0, 3)) {
        const plain = stripHtml(page.text);
        console.log(`  [vol=${page.vol}, page=${page.page}] ${plain.substring(0, 80)}...`);
      }
    }

    if (dryRun) {
      console.log("\n[DRY RUN] No database changes made.");
      return { bookId: id, title: content.meta.name, pages: content.pages.length, success: true };
    }

    // 4. Ensure Author in DB
    console.log("\nStep 4: Ensuring author...");
    const authorId = String(content.meta.author_id);

    const existingAuthor = await prisma.author.findUnique({ where: { id: authorId } });

    // Resolve birth/death dates: prefer API data, fallback to parsed info field
    const authorBirthDate = authorInfo.birthDateHijri || parsedInfo.authorBirthDateHijri || null;
    const authorDeathDate = authorInfo.deathDateHijri || parsedInfo.authorDeathDateHijri || null;

    if (existingAuthor) {
      console.log(`  Author already exists: ${existingAuthor.nameArabic}`);
      // Update biography and dates if we have new data and existing is empty
      const authorUpdates: Record<string, string> = {};
      if (authorInfo.biography && !existingAuthor.biography) {
        authorUpdates.biography = authorInfo.biography;
        authorUpdates.biographySource = "turath.io";
      }
      if (authorBirthDate && !existingAuthor.birthDateHijri) {
        authorUpdates.birthDateHijri = authorBirthDate;
      }
      if (authorDeathDate && !existingAuthor.deathDateHijri) {
        authorUpdates.deathDateHijri = authorDeathDate;
      }
      if (!existingAuthor.biographySource && authorInfo.biography) {
        authorUpdates.biographySource = "turath.io";
      }
      if (Object.keys(authorUpdates).length > 0) {
        await prisma.author.update({
          where: { id: authorId },
          data: authorUpdates,
        });
        console.log(`  Updated author fields: ${Object.keys(authorUpdates).join(", ")}`);
      }
    } else {
      await prisma.author.create({
        data: {
          id: authorId,
          nameArabic: authorName,
          biography: authorInfo.biography,
          biographySource: authorInfo.biography ? "turath.io" : null,
          deathDateHijri: authorDeathDate,
          birthDateHijri: authorBirthDate,
        },
      });
      console.log(`  Created author: ${authorName}`);
    }

    // 5. Ensure Category in DB
    console.log("\nStep 5: Ensuring category...");
    const categoryName = getCategoryName(content.meta.cat_id);

    let categoryRecord = await prisma.category.findUnique({ where: { nameArabic: categoryName } });
    if (!categoryRecord) {
      categoryRecord = await prisma.category.create({
        data: {
          nameArabic: categoryName,
          code: String(content.meta.cat_id),
        },
      });
      console.log(`  Created category: ${categoryName}`);
    } else {
      console.log(`  Category exists: ${categoryName}`);
    }

    // 6. Build TOC with mapped page numbers
    console.log("\nStep 6: Building table of contents...");

    // Map printed page number → 0-based index in pages array, then +1 for overview page offset
    const printedToIndex = new Map<number, number>();
    for (let i = 0; i < content.pages.length; i++) {
      const pp = content.pages[i].page;
      if (!printedToIndex.has(pp)) {
        printedToIndex.set(pp, i + 1); // +1 because page 0 is the overview page
      }
    }

    const tocEntries = content.toc.map((entry) => ({
      title: entry.title,
      level: entry.level,
      page: printedToIndex.get(entry.page) ?? (entry.page + 1),
    }));

    console.log(`  ${tocEntries.length} TOC entries mapped`);

    // 6b. Ensure Publisher in DB
    let publisherRecord: { id: number } | null = null;
    if (parsedInfo.publisher) {
      console.log("\nStep 7b: Ensuring publisher...");
      publisherRecord = await prisma.publisher.upsert({
        where: { name: parsedInfo.publisher },
        create: { name: parsedInfo.publisher, location: parsedInfo.publisherLocation },
        update: {},
      });
      console.log(`  Publisher: ${parsedInfo.publisher} (ID: ${publisherRecord.id})`);
    }

    // 6c. Ensure Editor in DB
    let editorRecord: { id: number } | null = null;
    if (parsedInfo.editor) {
      console.log("\nStep 7c: Ensuring editor...");
      editorRecord = await prisma.editor.upsert({
        where: { name: parsedInfo.editor },
        create: { name: parsedInfo.editor },
        update: {},
      });
      console.log(`  Editor: ${parsedInfo.editor} (ID: ${editorRecord.id})`);
    }

    // 7. Create Book record
    console.log("\nStep 7: Creating book...");
    const bookId = String(id);

    const displayTitle = content.meta.name;

    const bookData = {
      titleArabic: displayTitle,
      authorId,
      categoryId: categoryRecord.id,
      totalVolumes,
      totalPages: content.pages.length + 1, // +1 for overview page
      publisherId: publisherRecord?.id || null,
      editorId: editorRecord?.id || null,
      publicationYearHijri: parsedInfo.yearHijri || null,
      publicationYearGregorian: parsedInfo.yearGregorian || null,
      publicationEdition: parsedInfo.edition || null,
      publicationLocation: parsedInfo.publisherLocation || null,
      pageAlignmentNote: parsedInfo.pageAlignmentNote || printedFlagNote || null,
      verificationStatus: parsedInfo.isVerified ? "محقق" : null,
      descriptionHtml: content.meta.info_long || content.meta.info || null,
      summary: content.meta.info_long ? content.meta.info : null,
      filename: `turath:${id}`,
      tableOfContents: tocEntries.length > 0 ? tocEntries : undefined,
    };

    const existingBook = await prisma.book.findUnique({ where: { id: bookId } });
    if (existingBook) {
      console.log(`  Book already exists (ID: ${bookId}). Updating metadata...`);
      await prisma.book.update({ where: { id: bookId }, data: bookData });
    } else {
      await prisma.book.create({ data: { id: bookId, ...bookData } });
      console.log(`  Created book: ${displayTitle}`);
    }

    // 8. Download & store PDFs in RustFS
    const pdfKeyMap = new Map<number, string>(); // volumeIndex → RustFS key
    const failedVolumes = new Set<number>(); // track volumes whose download failed

    if (pdfLinks?.files && pdfLinks.files.length > 0) {
      console.log("\nStep 8: Downloading PDFs to RustFS...");
      await ensureBucket();

      let downloaded = 0;
      let skippedPdfs = 0;
      let failedPdfs = 0;

      for (let vi = 0; vi < pdfLinks.files.length; vi++) {
        const sourceUrl = buildPdfUrl(pdfLinks, vi);
        if (!sourceUrl) {
          failedPdfs++;
          failedVolumes.add(vi);
          continue;
        }

        const key = `${bookId}/${vi}.pdf`;

        if (await isAlreadyUploaded(key)) {
          pdfKeyMap.set(vi, key);
          skippedPdfs++;
          continue;
        }

        const result = await downloadAndStorePdf(sourceUrl, bookId, vi);
        if (result) {
          pdfKeyMap.set(vi, result);
          downloaded++;
        } else {
          failedPdfs++;
          failedVolumes.add(vi);
        }

        // Rate limit: 1s delay between downloads to avoid archive.org throttling
        if (vi < pdfLinks.files.length - 1) {
          await sleep(1000);
        }
      }

      console.log(`  Downloaded: ${downloaded}, Skipped (cached): ${skippedPdfs}, Failed: ${failedPdfs}`);
    } else {
      console.log("\nStep 8: No PDF links available, skipping PDF download.");
    }

    // 9. Create Page records
    console.log("\nStep 9: Importing pages...");

    // Delete existing pages for this book (clean re-import)
    const deletedCount = await prisma.page.deleteMany({ where: { bookId } });
    if (deletedCount.count > 0) {
      console.log(`  Deleted ${deletedCount.count} existing pages`);
    }

    // 9a. Generate and insert overview page (pageNumber=0)
    const overviewContent = generateOverviewContent(
      displayTitle,
      authorName,
      content.meta.info,
      content.toc,
    );
    const overviewPlain = stripHtml(overviewContent);
    const overviewHash = hashPage(bookId, 0, overviewPlain);

    // Use first volume's PDF for the overview page
    const firstVolumeIndex = content.pages.length > 0
      ? (volLabelToIndex.get(content.pages[0].vol) ?? 0)
      : 0;
    const overviewPdfUrl = pdfKeyMap.get(firstVolumeIndex)
      ?? (failedVolumes.has(firstVolumeIndex) ? null : buildPdfUrl(pdfLinks, firstVolumeIndex));

    await prisma.page.create({
      data: {
        bookId,
        pageNumber: 0,
        urlPageIndex: "i",
        volumeNumber: 0,
        printedPageNumber: null,
        contentHtml: overviewContent,
        contentPlain: overviewPlain,
        contentHash: overviewHash,
        sourceUrl: `https://app.turath.io/book/${id}`,
        pdfUrl: overviewPdfUrl,
      },
    });
    console.log("  Created overview page (page 0)");

    // 9b. Batch insert content pages (shifted by +1: first content page = pageNumber 1)
    const BATCH_SIZE = 100;
    let importedPages = 1; // Start at 1 counting the overview page
    let skippedPages = 0;

    for (let i = 0; i < content.pages.length; i += BATCH_SIZE) {
      const batch = content.pages.slice(i, i + BATCH_SIZE);
      const pageRecords = [];

      for (let j = 0; j < batch.length; j++) {
        const page = batch[j];
        const pageNumber = i + j + 1; // +1 offset for overview page
        const volumeNumber = resolveVolumeNumber(page.vol, volLabelMap);
        const printedPageNumber = page.page;

        const contentHtml = page.text;
        const contentPlain = stripHtml(contentHtml);

        // Skip empty pages
        if (contentPlain.length < 5) {
          skippedPages++;
          continue;
        }

        const contentHash = hashPage(bookId, pageNumber, contentPlain);
        const contentFlags = detectContentFlags(contentPlain);

        // Resolve PDF: prefer RustFS key, fallback to source URL only if download wasn't attempted/failed
        const volumeIndex = volLabelToIndex.get(page.vol) ?? 0;
        const pagePdfUrl = pdfKeyMap.get(volumeIndex)
          ?? (failedVolumes.has(volumeIndex) ? null : buildPdfUrl(pdfLinks, volumeIndex));

        pageRecords.push({
          bookId,
          pageNumber,
          volumeNumber,
          printedPageNumber,
          contentPlain,
          contentHtml,
          contentHash,
          sourceUrl: `https://app.turath.io/book/${id}#p-${printedPageNumber}`,
          pdfUrl: pagePdfUrl,
          ...contentFlags,
        });
      }

      if (pageRecords.length > 0) {
        await prisma.page.createMany({ data: pageRecords });
        importedPages += pageRecords.length;
      }

      // Progress
      const pct = Math.round(((i + batch.length) / content.pages.length) * 100);
      process.stdout.write(`\r  Progress: ${importedPages} pages imported (${pct}%)`);
    }

    console.log(); // newline after progress

    // 10. Print final summary
    console.log("\n" + "=".repeat(60));
    console.log("IMPORT COMPLETE");
    console.log("=".repeat(60));
    console.log(`  Book:            ${content.meta.name}`);
    console.log(`  ID:              ${bookId}`);
    console.log(`  Author:          ${authorName} (ID: ${authorId})`);
    console.log(`  Category:        ${categoryName}`);
    console.log(`  Pages imported:  ${importedPages} (incl. overview)`);
    console.log(`  Pages skipped:   ${skippedPages} (empty)`);
    console.log(`  Volumes:         ${totalVolumes}`);
    console.log(`  PDFs stored:     ${pdfKeyMap.size}`);
    if (publisherRecord) console.log(`  Publisher:       ${parsedInfo.publisher}`);
    if (editorRecord) console.log(`  Editor:          ${parsedInfo.editor}`);
    if (parsedInfo.yearHijri) console.log(`  Year (Hijri):    ${parsedInfo.yearHijri}`);
    if (parsedInfo.yearGregorian) console.log(`  Year (Greg.):    ${parsedInfo.yearGregorian}`);
    console.log("=".repeat(60));

    return { bookId, title: content.meta.name, pages: importedPages, success: true };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`  Import failed for book ${id}: ${message}`);
    return { bookId: id, title: "", pages: 0, success: false, error: message };
  }
}

// ---------------------------------------------------------------------------
// CLI entry point
// ---------------------------------------------------------------------------

async function main() {
  const { id, dryRun } = parseArgs();

  console.log("Turath Book Import");
  console.log("=".repeat(60));
  console.log(`Book ID:            ${id}`);
  console.log(`Mode:               ${dryRun ? "DRY RUN (no DB writes)" : "LIVE IMPORT"}`);
  console.log("=".repeat(60));
  console.log();

  const result = await importTurathBook(id, { dryRun });

  if (!result.success) {
    process.exit(1);
  }
}

// Only run main() when executed directly (not imported)
const isDirectRun = process.argv[1]?.endsWith("/import-turath.ts") ||
  process.argv[1]?.endsWith("/import-turath.js");

if (isDirectRun) {
  main()
    .catch((e) => {
      console.error("\nImport failed:");
      console.error(e);
      process.exit(1);
    })
    .finally(async () => {
      await prisma.$disconnect();
    });
}
