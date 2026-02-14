/**
 * Import Extracted Dictionary Definitions
 *
 * Reads batch files produced by LLM extraction and
 * inserts definitions into the DictionarySubEntry table.
 * Includes quality filters and programmatic cleanup/root extraction.
 *
 * Usage:
 *   bun run pipelines/import/dictionary/import-extracted-definitions.ts \
 *     --slug=tarifat [--dry-run]
 */

import "../../env";
import { prisma } from "../../../src/db";
import {
  normalizeArabic,
  normalizeArabicLight,
  hasTashkeel,
  extractArabicRoot,
  normalizeWeakRoot,
  stripDefiniteArticle,
} from "../../../src/utils/arabic-text";
import { readdirSync, readFileSync } from "fs";
import { resolve } from "path";

const BATCH_DIR = resolve(import.meta.dir, "extraction-batches");
const BATCH_INSERT_SIZE = 500;

// Section header patterns that should NOT be headwords
const SECTION_HEADER_RE = /^(كتاب|باب|فصل|حرف|فهرس|مقدمة)\b/;
// Root grouping headers like "الخاء والراء والميم"
const ROOT_GROUP_RE = /^ال\S+ وال\S+ وال\S+$/;
// Page number / cross-reference only
const PAGE_REF_RE = /^[\d\s\/٠-٩\u0660-\u0669]+$/;
// Junk definitions
const JUNK_DEF_RE = /^(مهمل|_{3,}|[-=]{3,})$/;
// Footnote marker pattern in headwords (after cleanup)
const FOOTNOTE_IN_HEADWORD_RE = /\(\^?[0-9٠-٩]+\)/;
// Headword is entirely digits (Arabic or Western)
const NUMBER_HEADWORD_RE = /^[0-9٠-٩\u0660-\u0669\s.,-]+$/;
// Manuscript note headword: في م, في ك, كذا في, في الأصل, etc.
const MANUSCRIPT_NOTE_RE = /^(في [مكأ]|كذا في|في الأصل)/;
// Arabic letter range for detecting Arabic-only content
const ARABIC_LETTER_RE = /[\u0600-\u06FF]/;

interface ExtractedDef {
  headword: string;
  root?: string;
  rootNormalized?: string;
  definition: string;
  pageNumber: number;
}

interface BatchFile {
  slug: string;
  sourceId: number;
  bookId: string;
  dictionaryName: string;
  totalChunks: number;
  chunks: Array<{ chunkId: number }>;
  extracted?: Array<{
    chunkId: number;
    definitions: ExtractedDef[];
  }>;
}

/**
 * Clean headword: strip footnote markers, Quranic brackets, leading conjunctions, citation markers.
 */
function cleanHeadword(hw: string): string {
  let cleaned = hw.trim();
  // Strip footnote markers: (^23), (^١٢), (23), (١٢) when they look like footnotes
  cleaned = cleaned.replace(/\(\^?[0-9٠-٩]+\)/g, "").trim();
  // Strip Quranic brackets
  cleaned = cleaned.replace(/[﴿﴾]/g, "").trim();
  // Strip leading و conjunction (و followed by space)
  cleaned = cleaned.replace(/^و\s+/, "").trim();
  // Strip leading في المثل, يقال, ويقال (citation markers)
  cleaned = cleaned.replace(/^(في المثل|يقال|ويقال)\s*:?\s*/, "").trim();
  return cleaned;
}

/**
 * Clean definition text: strip footnote markers.
 */
function cleanDefinition(def: string): string {
  return def.replace(/\(\^?[0-9٠-٩]+\)/g, "").trim();
}

/**
 * Extract root programmatically from headword.
 * Uses the morphological pipeline from arabic-text.ts.
 * Returns { root, rootNormalized } or empty strings.
 */
function extractRootFromHeadword(headword: string): { root: string; rootNormalized: string } {
  const normalized = normalizeArabic(headword);
  const stripped = stripDefiniteArticle(headword);

  // Try pattern extraction on both forms
  const extracted = extractArabicRoot(stripped) || extractArabicRoot(normalized);
  if (extracted) {
    // Validate: root should be 3-4 Arabic letters
    if (extracted.length >= 3 && extracted.length <= 4 && ARABIC_LETTER_RE.test(extracted)) {
      return { root: extracted, rootNormalized: extracted };
    }
  }

  // For short words (3-4 letters after stripping), use as root directly
  if (stripped.length >= 3 && stripped.length <= 4 && ARABIC_LETTER_RE.test(stripped)) {
    return { root: stripped, rootNormalized: stripped };
  }

  return { root: "", rootNormalized: "" };
}

/**
 * Validate and fix an LLM-provided root.
 * Strips "ال" prefix, validates length and content.
 */
function validateLLMRoot(root: string | undefined): string {
  if (!root) return "";
  let r = root.trim();
  // Strip ال prefix (any length)
  if (r.startsWith("ال")) {
    r = r.slice(2);
  }
  // Normalize
  r = normalizeArabic(r);
  // Must be 3-4 Arabic letters
  if (r.length < 3 || r.length > 4) return "";
  if (!ARABIC_LETTER_RE.test(r)) return "";
  return r;
}

/**
 * Quality filter: returns a rejection reason or null if valid.
 * Applied AFTER cleanHeadword/cleanDefinition.
 */
function getRejectReason(def: { headword: string; definition: string; pageNumber: number }): string | null {
  const hw = def.headword.trim();
  const defText = def.definition.trim();

  // Empty
  if (!hw || !defText) return "empty";

  // Page zero (frontmatter/intro)
  if (def.pageNumber === 0) return "page_zero";

  // Headword is all numbers
  if (NUMBER_HEADWORD_RE.test(hw)) return "number_headword";

  // Headword starts with footnote marker (even after cleanup)
  if (hw.startsWith("(^") || FOOTNOTE_IN_HEADWORD_RE.test(hw)) return "footnote_as_headword";

  // Manuscript note as headword
  if (MANUSCRIPT_NOTE_RE.test(hw)) return "manuscript_note";

  // Headword too long (section headers, sentences, Quran verses)
  if (hw.length > 40) return "headword_too_long";

  // Headword is a section header
  if (SECTION_HEADER_RE.test(hw)) return "section_header";

  // Root grouping header
  if (ROOT_GROUP_RE.test(hw)) return "root_group_header";

  // Definition is just page numbers
  if (PAGE_REF_RE.test(defText)) return "page_ref_only";

  // Definition is junk
  if (JUNK_DEF_RE.test(defText)) return "junk_definition";

  // Definition too short (less than 3 chars, excluding whitespace)
  if (defText.replace(/\s/g, "").length < 3) return "definition_too_short";

  return null;
}

function parseArgs() {
  const args = process.argv.slice(2);
  let slug = "";
  let dryRun = false;

  for (const arg of args) {
    if (arg.startsWith("--slug=")) slug = arg.slice(7);
    else if (arg === "--dry-run") dryRun = true;
  }

  if (!slug) {
    console.error("Usage: bun run import-extracted-definitions.ts --slug=<slug> [--dry-run]");
    process.exit(1);
  }

  return { slug, dryRun };
}

async function main() {
  const { slug, dryRun } = parseArgs();

  const slugDir = resolve(BATCH_DIR, slug);
  let files: string[];
  try {
    files = readdirSync(slugDir)
      .filter((f) => f.endsWith(".json"))
      .sort();
  } catch {
    console.error(`No batch directory found at ${slugDir}`);
    process.exit(1);
  }

  if (files.length === 0) {
    console.error(`No batch files found in ${slugDir}`);
    process.exit(1);
  }

  // Read all batch files and collect definitions
  const allDefs: Array<{
    sourceId: number;
    bookId: string;
    headword: string;
    headwordNormalized: string;
    headwordVocalized: string;
    root: string;
    rootNormalized: string;
    definitionPlain: string;
    pageNumber: number;
  }> = [];

  let extractedBatches = 0;
  let skippedBatches = 0;
  let cleanupStats = { footnoteStripped: 0, quranBracketStripped: 0, conjunctionStripped: 0, citationStripped: 0 };
  const rejectCounts = new Map<string, number>();

  for (const file of files) {
    const batch: BatchFile = JSON.parse(readFileSync(resolve(slugDir, file), "utf-8"));

    if (!batch.extracted || batch.extracted.length === 0) {
      skippedBatches++;
      continue;
    }
    extractedBatches++;

    for (const chunk of batch.extracted) {
      if (!chunk.definitions) continue;
      // Handle nested format: some agents returned {chunkId, definitions: {chunkId, definitions: [...]}}
      let defs = chunk.definitions;
      if (!Array.isArray(defs) && typeof defs === "object" && Array.isArray((defs as any).definitions)) {
        defs = (defs as any).definitions;
      }
      if (!Array.isArray(defs)) continue;
      for (const def of defs) {
        if (!def.headword || !def.definition) continue;

        // Step 1: Clean headword and definition
        const rawHw = def.headword.trim();
        const headword = cleanHeadword(rawHw);
        const definition = cleanDefinition(def.definition.trim());

        // Track cleanup stats
        if (rawHw !== headword) {
          if (/\(\^?[0-9٠-٩]+\)/.test(rawHw)) cleanupStats.footnoteStripped++;
          if (/[﴿﴾]/.test(rawHw)) cleanupStats.quranBracketStripped++;
          if (/^و\s+/.test(rawHw)) cleanupStats.conjunctionStripped++;
          if (/^(في المثل|يقال|ويقال)/.test(rawHw)) cleanupStats.citationStripped++;
        }

        // Step 2: Quality filter (on cleaned text)
        const rejectReason = getRejectReason({ headword, definition, pageNumber: def.pageNumber });
        if (rejectReason) {
          rejectCounts.set(rejectReason, (rejectCounts.get(rejectReason) || 0) + 1);
          continue;
        }

        const headwordNormalized = normalizeArabic(headword);
        const headwordVocalized = hasTashkeel(headword) ? normalizeArabicLight(headword) : "";

        // Step 3: Root extraction — programmatic first, LLM fallback
        let rootNorm = "";
        let rootRaw = "";

        // Try programmatic extraction
        const programmatic = extractRootFromHeadword(headword);
        if (programmatic.rootNormalized) {
          rootNorm = programmatic.rootNormalized;
          rootRaw = programmatic.root;
        } else {
          // Fallback: validate LLM-provided root
          const llmRoot = validateLLMRoot(def.root || (def as any).rootNormalized);
          if (llmRoot) {
            rootNorm = llmRoot;
            rootRaw = def.root?.trim() || llmRoot;
          }
        }

        allDefs.push({
          sourceId: batch.sourceId,
          bookId: batch.bookId,
          headword,
          headwordNormalized,
          headwordVocalized,
          root: rootRaw,
          rootNormalized: rootNorm,
          definitionPlain: definition,
          pageNumber: def.pageNumber,
        });
      }
    }
  }

  console.log(`Batch files: ${files.length} total, ${extractedBatches} extracted, ${skippedBatches} skipped`);
  console.log(`Raw valid definitions: ${allDefs.length}`);

  // Cleanup stats
  const totalCleanups = cleanupStats.footnoteStripped + cleanupStats.quranBracketStripped +
    cleanupStats.conjunctionStripped + cleanupStats.citationStripped;
  if (totalCleanups > 0) {
    console.log("Headword cleanup:");
    if (cleanupStats.footnoteStripped) console.log(`  footnote markers stripped: ${cleanupStats.footnoteStripped}`);
    if (cleanupStats.quranBracketStripped) console.log(`  Quran brackets stripped: ${cleanupStats.quranBracketStripped}`);
    if (cleanupStats.conjunctionStripped) console.log(`  conjunction و stripped: ${cleanupStats.conjunctionStripped}`);
    if (cleanupStats.citationStripped) console.log(`  citation markers stripped: ${cleanupStats.citationStripped}`);
  }

  if (rejectCounts.size > 0) {
    console.log("Rejected:");
    for (const [reason, count] of [...rejectCounts.entries()].sort((a, b) => b[1] - a[1])) {
      console.log(`  ${reason}: ${count}`);
    }
  }

  // Deduplicate by (sourceId, headwordNormalized, pageNumber)
  const seen = new Set<string>();
  const dedupDefs = allDefs.filter((d) => {
    const key = `${d.sourceId}:${d.headwordNormalized}:${d.pageNumber}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  const dupes = allDefs.length - dedupDefs.length;
  console.log(`After dedup: ${dedupDefs.length} definitions (${dupes} duplicates removed)`);

  if (dedupDefs.length === 0) {
    console.log("No definitions to import.");
    return;
  }

  if (dryRun) {
    console.log("\n--- DRY RUN ---");
    console.log(`Would insert ${dedupDefs.length} sub-entries for source "${slug}"`);

    // Show sample
    console.log("\nSample definitions (first 20):");
    for (const d of dedupDefs.slice(0, 20)) {
      const defPreview = d.definitionPlain.length > 80
        ? d.definitionPlain.slice(0, 80) + "..."
        : d.definitionPlain;
      const voc = d.headwordVocalized ? ` [voc: ${d.headwordVocalized}]` : "";
      console.log(`  p${d.pageNumber} ${d.headword}${voc} [${d.rootNormalized}]: ${defPreview}`);
    }

    // Stats
    const withRoot = dedupDefs.filter((d) => d.rootNormalized.length > 0).length;
    const withVocalized = dedupDefs.filter((d) => d.headwordVocalized.length > 0).length;
    const avgDefLen = Math.round(dedupDefs.reduce((s, d) => s + d.definitionPlain.length, 0) / dedupDefs.length);

    // Root quality stats
    const rootLenDist = new Map<number, number>();
    const badRoots: string[] = [];
    for (const d of dedupDefs) {
      if (d.rootNormalized) {
        const len = d.rootNormalized.length;
        rootLenDist.set(len, (rootLenDist.get(len) || 0) + 1);
        if (len > 4 || d.rootNormalized.startsWith("ال")) {
          badRoots.push(d.rootNormalized);
        }
      }
    }

    console.log(`\nStats: ${withRoot}/${dedupDefs.length} have root (${((withRoot / dedupDefs.length) * 100).toFixed(1)}%), ${withVocalized}/${dedupDefs.length} have vocalized, avg def length: ${avgDefLen} chars`);
    console.log("Root length distribution:", [...rootLenDist.entries()].sort((a, b) => a[0] - b[0]).map(([len, cnt]) => `${len}-letter: ${cnt}`).join(", "));
    if (badRoots.length > 0) {
      console.log(`Bad roots (>4 chars or starts with ال): ${badRoots.length} — samples: ${[...new Set(badRoots)].slice(0, 10).join(", ")}`);
    }
    return;
  }

  // Delete existing sub-entries for this source
  const source = await prisma.dictionarySource.findUnique({ where: { slug } });
  if (!source) {
    console.error(`Source "${slug}" not found in database.`);
    process.exit(1);
  }

  const deleted = await prisma.dictionarySubEntry.deleteMany({ where: { sourceId: source.id } });
  console.log(`Deleted ${deleted.count} existing sub-entries for "${slug}"`);

  // Batch insert
  let inserted = 0;
  for (let i = 0; i < dedupDefs.length; i += BATCH_INSERT_SIZE) {
    const batch = dedupDefs.slice(i, i + BATCH_INSERT_SIZE);
    await prisma.dictionarySubEntry.createMany({
      data: batch.map((d, idx) => ({
        sourceId: d.sourceId,
        headword: d.headword,
        headwordNormalized: d.headwordNormalized,
        headwordVocalized: d.headwordVocalized,
        root: d.root,
        rootNormalized: d.rootNormalized,
        definitionPlain: d.definitionPlain,
        definitionHtml: d.definitionPlain,
        bookId: d.bookId,
        pageNumber: d.pageNumber,
        position: i + idx,
        entryId: null,
      })),
    });
    inserted += batch.length;
    if (inserted % 2000 === 0 || inserted === dedupDefs.length) {
      console.log(`  Inserted ${inserted}/${dedupDefs.length}`);
    }
  }

  console.log(`\nDone. Inserted ${inserted} sub-entries for "${slug}".`);
}

main()
  .catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
  })
  .finally(() => prisma.$disconnect());
