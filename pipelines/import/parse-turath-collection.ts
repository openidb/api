/**
 * Generic Turath Hadith Parser
 *
 * Reads chunk-NNN.json files from {slug}-pages-cache/ and produces
 * chunk-NNN.extracted.json files with parsed hadiths.
 *
 * Uses collection-specific configuration from turath-hadith-configs.ts
 * to determine hadith numbering patterns and kitab/bab heading detection.
 * Isnad/matn splitting is deferred to LLM (split-isnad-matn-llm.ts).
 *
 * Parser types:
 *   - 'standard': N - pattern (most collections)
 *   - 'muslim':   Delegates to parse-muslim-pages.ts (special dual numbering)
 *
 * Usage:
 *   bun run pipelines/import/parse-turath-collection.ts --collection=abudawud
 *   bun run pipelines/import/parse-turath-collection.ts --collection=abudawud --chunk=5
 *   bun run pipelines/import/parse-turath-collection.ts --collection=abudawud --dry-run
 */

import { readFileSync, writeFileSync, readdirSync, existsSync } from "fs";
import { join } from "path";
import { getConfig, ALL_REMAINING, type CollectionConfig } from "./turath-hadith-configs";

// =============================================================================
// Shared Utilities
// =============================================================================

/** Convert Arabic-Indic numerals (٠-٩) to Western digits */
function toWestern(s: string): string {
  return s.replace(/[٠-٩]/g, (d) => "٠١٢٣٤٥٦٧٨٩".indexOf(d).toString());
}

/** Strip printed page markers like ⦗٣٢⦘ from text */
function stripPageMarkers(s: string): string {
  return s
    .replace(/\s*⦗[٠-٩]+⦘\s*/g, " ")
    .replace(/\s{2,}/g, " ")
    .trim();
}

/** Strip footnote markers like (^١) (^٢) from text */
function stripFootnoteMarkers(s: string): string {
  return s.replace(/\(\^[٠-٩0-9]+\)/g, "").replace(/\s{2,}/g, " ");
}

/** Strip Arabic diacritics (tashkeel) for comparison */
function stripDiacritics(s: string): string {
  return s.replace(/[\u064B-\u065F\u0670]/g, "");
}

/**
 * Clean kitab heading text:
 * - Strip leading number + dash (e.g., "٧ - ")
 * - Strip بسملة
 * - Strip trailing periods
 */
function cleanKitabName(s: string): string {
  // Remove leading number + dash
  s = s.replace(/^[٠-٩]+ - /, "");

  // Match بسملة regardless of diacritic ordering
  const plain = stripDiacritics(s);
  const bismIdx = plain.indexOf("بسم الله الرحمن الرحيم");
  if (bismIdx >= 0) {
    let origPos = 0;
    let strippedCount = 0;
    while (strippedCount < bismIdx && origPos < s.length) {
      if (!/[\u064B-\u065F\u0670]/.test(s[origPos])) strippedCount++;
      origPos++;
    }
    s = s.substring(0, origPos).trim();
  }

  return s
    .replace(/\s*﷽\s*/g, "")
    .replace(/\.\s*$/, "")
    .trim();
}

/** Split text at footnote separator (_________) */
function splitFootnotes(text: string): { main: string; footnotes: string | null } {
  const sepIdx = text.indexOf("_________");
  if (sepIdx === -1) return { main: text, footnotes: null };
  const main = text.substring(0, sepIdx).trim();
  const footnotes = text
    .substring(sepIdx)
    .replace(/^_+\s*/, "")
    .trim();
  return { main, footnotes: footnotes || null };
}

/** Strip trailing bab/kitab headings from hadith body */
function stripTrailingHeadings(text: string): string {
  const lines = text.split("\n");
  while (lines.length > 0) {
    const last = lines[lines.length - 1].trim();
    if (!last) {
      lines.pop();
      continue;
    }
    const plain = stripDiacritics(last);
    if (
      /^\([٠-٩]+\)\s*(\([٠-٩]+\)\s*)?-?\s*باب/.test(plain) ||
      /^[٠-٩]+ - كتاب/.test(plain) ||
      /^[٠-٩]+ - باب/.test(plain) ||
      /^كتاب\s/.test(plain) ||
      /^باب\s/.test(plain) ||
      /^أبواب[\s]/.test(plain) ||
      /^\[كتاب/.test(plain) ||
      /^ذكر\s/.test(plain)
    ) {
      lines.pop();
    } else {
      break;
    }
  }
  return lines.join("\n");
}

/** Strip trailing headings and chapter intros from footnotes text */
function stripFootnoteTrailingHeadings(text: string): string {
  const lines = text.split("\n");
  // Remove trailing heading lines from end of footnotes
  while (lines.length > 0) {
    const last = lines[lines.length - 1].trim();
    if (!last) {
      lines.pop();
      continue;
    }
    const plain = stripDiacritics(last);
    if (
      // Bab headings (various formats)
      /^\([٠-٩]+\)\s*(\([٠-٩]+\)\s*)?-?\s*باب/.test(plain) ||
      /^[٠-٩]+ - باب/.test(plain) ||
      /^[٠-٩]+ - كتاب/.test(plain) ||
      /^باب\s/.test(plain) ||
      /^كتاب\s/.test(plain) ||
      /^أبواب[\s]/.test(plain) ||
      /^\[كتاب/.test(plain) ||
      /^ذكر\s/.test(plain) ||
      // Quran intros (common in Riyadussalihin)
      /^قال الله تعالى/.test(plain) ||
      // "As for the hadiths:" intro
      /^واما الاحاديث/.test(plain) ||
      // Quranic verse brackets at start
      /^﴿/.test(last)
    ) {
      lines.pop();
    } else {
      break;
    }
  }
  return lines.join("\n").trim();
}

// =============================================================================
// Chain Variation Detection
// =============================================================================

/** Detect chain variations: short texts referencing a previous hadith */
function detectChainVariation(fullText: string): boolean {
  const stripped = stripDiacritics(fullText);

  if (stripped.length < 200) {
    if (
      /بمثله|بنحوه|مثله|نحوه|بمثل ذلك|بنحو حديثهم|بنحو حديث|بهذا الإسناد/.test(
        stripped
      )
    ) {
      return true;
    }
    // Very short text — likely a brief cross-reference
    if (stripped.length < 60) return true;
  }
  return false;
}

// =============================================================================
// Heading Parsing
// =============================================================================

function parseHeadings(
  text: string,
  currentKitab: string,
  currentBab: string,
  config: CollectionConfig
): { kitab: string; bab: string } {
  let kitab = currentKitab;
  let bab = currentBab;

  const lines = text.split("\n");
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    const plain = stripDiacritics(trimmed);

    // Kitab detection
    if (config.kitabStyle === "numbered") {
      // Muslim-style: N - كتاب TEXT
      if (/^[٠-٩]+ - كتاب/.test(plain)) {
        kitab = cleanKitabName(trimmed);
        bab = "";
      }
    } else {
      // Standalone: كتاب TEXT or كِتَابُ TEXT
      if (/^كتاب\s/.test(plain) || /^كتاب$/.test(plain)) {
        kitab = cleanKitabName(trimmed);
        bab = "";
      }
      // أبواب heading (e.g., Tirmidhi uses أبواب الطهارة)
      if (/^أبواب[\s]/.test(plain) || /^أبواب$/.test(plain)) {
        kitab = cleanKitabName(trimmed);
        bab = "";
      }
    }

    // Bracketed kitab heading: [كتاب TEXT] (e.g., Nasai)
    if (/^\[كتاب/.test(plain)) {
      const bracketEnd = trimmed.indexOf("]");
      kitab = bracketEnd >= 0
        ? trimmed.substring(1, bracketEnd).trim()
        : trimmed.substring(1).trim();
      bab = "";
    }

    // Bab detection
    if (config.babStyle === "numbered") {
      // Muslim-style: (N) باب TEXT or Tirmidhi-style: (N) (M) بَابُ TEXT
      if (/^\([٠-٩]+\)\s*(\([٠-٩]+\)\s*)?-?\s*باب/.test(plain)) {
        bab = trimmed;
      }
    } else if (config.babStyle === "standalone") {
      // Standard: باب TEXT or بَابُ TEXT
      if (/^باب\s/.test(plain) || /^باب$/.test(plain)) {
        bab = trimmed;
      }
      // Numbered bab: N - باب TEXT (e.g., Riyadussalihin)
      if (/^[٠-٩]+ - باب/.test(plain)) {
        bab = trimmed;
      }
    }
  }

  return { kitab, bab };
}

// =============================================================================
// Types
// =============================================================================

interface PageData {
  pageNumber: number;
  volumeNumber: number;
  printedPageNumber: number;
  contentPlain: string;
}

interface ChunkData {
  chunkId: number;
  pagesFrom: number;
  pagesTo: number;
  pages: PageData[];
}

interface ParsedHadith {
  hadithNumber: string;
  sequentialNumber: number;
  parenthesizedNumber: number;
  isnad: string;
  matn: string;
  kitab: string;
  bab: string;
  footnotes: string | null;
  pageStart: number;
  pageEnd: number;
  isChainVariation: boolean;
}

interface ExtractedChunk {
  chunkId: number;
  lastKitab: string;
  lastBab: string;
  hadiths: ParsedHadith[];
}

// =============================================================================
// Standard Parser (N - pattern)
// =============================================================================

function parseStandard(
  pages: PageData[],
  config: CollectionConfig
): {
  hadiths: ParsedHadith[];
  lastKitab: string;
  lastBab: string;
} {
  let currentKitab = "";
  let currentBab = "";
  const hadiths: ParsedHadith[] = [];

  // Build concatenated text with page position tracking.
  // Split footnotes PER-PAGE before concatenation so multi-page hadith text
  // isn't swallowed by the first page's footnote separator.
  const pageBreakPositions: { pos: number; page: PageData }[] = [];
  const pageFootnotes: Map<number, string> = new Map(); // pageNumber → footnotes
  let fullText = "";

  for (const seg of pages) {
    const stripped = stripFootnoteMarkers(seg.contentPlain);
    const { main, footnotes } = splitFootnotes(stripped);
    if (footnotes) pageFootnotes.set(seg.pageNumber, footnotes);
    if (fullText.length > 0) {
      pageBreakPositions.push({ pos: fullText.length, page: seg });
      fullText += "\n";
    } else {
      pageBreakPositions.push({ pos: 0, page: seg });
    }
    fullText += main;
  }

  // Check for special initial kitab
  if (config.initialKitab && !currentKitab) {
    if (config.initialKitab.pattern.test(fullText)) {
      currentKitab = config.initialKitab.name;
      if (config.initialKitab.bab) currentBab = config.initialKitab.bab;
    }
  }

  // Find all hadith start positions using N - pattern (or N/ N - for dual style)
  const hadithStarts: {
    index: number;
    number: number;
    matchEnd: number;
  }[] = [];

  const isDual = config.numberingStyle === "dual";
  const pattern = isDual
    ? /(?:^|\n)\s*([٠-٩]+)\s*\/\s*[٠-٩]+\s*-\s*/g
    : /(?:^|\n)\s*([٠-٩]+)\s*-\s*/g;
  let match;
  while ((match = pattern.exec(fullText)) !== null) {
    const num = parseInt(toWestern(match[1]), 10);
    hadithStarts.push({
      index: match.index,
      number: num,
      matchEnd: match.index + match[0].length,
    });
  }

  // Filter out matches that are actually kitab/bab headings (not hadiths)
  const filteredStarts = hadithStarts.filter((hs) => {
    const afterMatch = fullText.substring(hs.matchEnd, hs.matchEnd + 80);
    const plain = stripDiacritics(afterMatch);

    // Skip if this is a kitab heading: N - كتاب ... or N - [كتاب ...]
    if (/^كتاب[\s\u064B-\u065F]/.test(plain) || /^كتاب$/.test(plain.trim())) {
      return false;
    }
    if (/^\[كتاب/.test(plain)) {
      return false;
    }
    // Skip if this is explicitly a bab heading: N - باب ...
    if (/^باب[\s\u064B-\u065F]/.test(plain) || /^باب$/.test(plain.trim())) {
      return false;
    }
    // Skip if this is an أبواب heading
    if (/^أبواب[\s]/.test(plain) || /^أبواب$/.test(plain.trim())) {
      return false;
    }
    // Skip if this is a ذكر section heading (e.g., Ibn Hibban)
    if (/^ذكر\s/.test(plain)) {
      return false;
    }

    // When requireIsnadStart is set, only accept hadiths that start with
    // a transmission phrase (أخبرنا, حدثنا, etc.) or reference marker
    if (config.requireIsnadStart) {
      // Normalize alef variants (أ إ آ ٱ → ا) + strip leading dash
      const normalized = plain
        .replace(/[أإآٱ]/g, "ا")
        .replace(/^-\s*/, "");
      const isnadPhrases = [
        /^اخبرنا/, /^حدثنا/, /^حدثني/, /^انبانا/, /^انا /,
        /^اخبرني/, /^قال:?\s/, /^عن /, /^سمعت /,
        /^بمثله/, /^نحوه/, /^مثله/,
      ];
      const matches = isnadPhrases.some((p) => p.test(normalized));
      if (!matches) return false;
    }

    return true;
  });

  // Sort by position in text
  filteredStarts.sort((a, b) => a.index - b.index);

  // Helper: find page at position
  function getPageAtPos(pos: number): PageData {
    let result = pageBreakPositions[0].page;
    for (const bp of pageBreakPositions) {
      if (bp.pos <= pos) result = bp.page;
      else break;
    }
    return result;
  }

  // Process each hadith
  for (let i = 0; i < filteredStarts.length; i++) {
    const hs = filteredStarts[i];
    const nextStart =
      i + 1 < filteredStarts.length
        ? filteredStarts[i + 1].index
        : fullText.length;

    // Text before this hadith
    const prevEnd = i > 0 ? filteredStarts[i - 1].matchEnd : 0;
    const textBefore = fullText.substring(i > 0 ? prevEnd : 0, hs.index);

    // Hadith body text
    const hadithBody = fullText.substring(hs.matchEnd, nextStart).trim();

    // Parse headings from text before
    const headings = parseHeadings(textBefore, currentKitab, currentBab, config);
    currentKitab = headings.kitab;
    currentBab = headings.bab;

    // Check implicit kitabs
    if (config.implicitKitabs && config.implicitKitabs[hs.number]) {
      const implicitName = config.implicitKitabs[hs.number];
      if (implicitName !== currentKitab) {
        currentKitab = implicitName;
        currentBab = "";
      }
    }

    // Page range
    const startPage = getPageAtPos(hs.matchEnd);
    const endPage = getPageAtPos(Math.min(nextStart - 1, fullText.length - 1));

    // Collect footnotes from pages this hadith spans (already split per-page)
    const startPageNum = startPage.pageNumber;
    const endPageNum = endPage.pageNumber;
    const hadithFootnotes: string[] = [];
    for (let pn = startPageNum; pn <= endPageNum; pn++) {
      const fn = pageFootnotes.get(pn);
      if (fn) hadithFootnotes.push(fn);
    }
    const footnotesRaw = hadithFootnotes.length > 0 ? hadithFootnotes.join("\n") : null;

    // Strip trailing headings if configured (from both main body and footnotes)
    const main = config.stripTrailingHeadings
      ? stripTrailingHeadings(hadithBody)
      : hadithBody;
    const footnotes = config.stripTrailingHeadings && footnotesRaw
      ? stripFootnoteTrailingHeadings(footnotesRaw) || null
      : footnotesRaw;

    // Store full text in matn (isnad/matn splitting done later by LLM)
    const cleanedMain = stripPageMarkers(main.trim());

    hadiths.push({
      hadithNumber: String(hs.number),
      sequentialNumber: hs.number,
      parenthesizedNumber: hs.number,
      isnad: "",
      matn: cleanedMain,
      kitab: currentKitab,
      bab: currentBab,
      footnotes,
      pageStart: startPage.pageNumber,
      pageEnd: endPage.pageNumber,
      isChainVariation: detectChainVariation(cleanedMain),
    });
  }

  return { hadiths, lastKitab: currentKitab, lastBab: currentBab };
}

// =============================================================================
// Chunk Processing
// =============================================================================

function processChunk(
  filename: string,
  config: CollectionConfig,
  dryRun: boolean,
  inheritKitab?: string,
  inheritBab?: string
): {
  lastKitab: string;
  lastBab: string;
  count: number;
  emptyKitab: number;
  emptyBab: number;
  emptyMatn: number;
  chainVars: number;
  withFootnotes: number;
} | null {
  const filepath = join(config.cacheDir, filename);
  const chunk: ChunkData = JSON.parse(readFileSync(filepath, "utf8"));

  // Parse pages using standard parser
  const result = parseStandard(chunk.pages, config);

  // Apply inherited kitab/bab from previous chunk
  if (inheritKitab || inheritBab) {
    let kitab = inheritKitab || "";
    let bab = inheritBab || "";
    for (const h of result.hadiths) {
      // Reset bab when kitab changes (prevent cross-contamination)
      if (h.kitab && h.kitab !== kitab) bab = "";
      if (!h.kitab && kitab) h.kitab = kitab;
      if (!h.bab && bab) h.bab = bab;
      if (h.kitab) kitab = h.kitab;
      if (h.bab) bab = h.bab;
    }
    if (!result.lastKitab && kitab) result.lastKitab = kitab;
    if (!result.lastBab && bab) result.lastBab = bab;
  }

  // Apply kitabRanges for collections without explicit kitab headings in text.
  // When ranges are configured, ALWAYS apply them (override inheritance).
  // For overlapping ranges, prefer the most specific (latest start).
  if (config.kitabRanges) {
    for (const h of result.hadiths) {
      const num = h.sequentialNumber;
      const matching = config.kitabRanges.filter(
        (r) => num >= r.start && num <= r.end
      );
      if (matching.length > 0) {
        // Prefer most specific range (latest start = most specific)
        matching.sort((a, b) => b.start - a.start);
        h.kitab = matching[0].name;
      }
    }
    // Also update lastKitab from ranges
    if (result.hadiths.length > 0) {
      const lastH = result.hadiths[result.hadiths.length - 1];
      if (lastH.kitab) result.lastKitab = lastH.kitab;
    }
  }

  // Deduplicate within chunk
  const seen = new Set<string>();
  const deduped: ParsedHadith[] = [];
  for (const h of result.hadiths) {
    const key = `${h.hadithNumber}-${h.pageStart}`;
    if (!seen.has(key)) {
      seen.add(key);
      deduped.push(h);
    }
  }

  // Stats
  const emptyKitab = deduped.filter((h) => !h.kitab).length;
  const emptyBab = deduped.filter((h) => !h.bab).length;
  const emptyMatn = deduped.filter((h) => !h.matn).length;
  const chainVars = deduped.filter((h) => h.isChainVariation).length;
  const withFootnotes = deduped.filter((h) => h.footnotes).length;

  const chunkLabel = filename.replace(".json", "");
  console.log(
    `  ${chunkLabel}: ${deduped.length} hadiths` +
      ` (kitab: ${emptyKitab === 0 ? "ok" : emptyKitab + " empty"}` +
      `, bab: ${emptyBab === 0 ? "ok" : emptyBab + " empty"}` +
      `, matn: ${emptyMatn === 0 ? "ok" : emptyMatn + " empty"}` +
      `, chain-var: ${chainVars}` +
      `, footnotes: ${withFootnotes})`
  );

  if (!dryRun) {
    const output: ExtractedChunk = {
      chunkId: chunk.chunkId,
      lastKitab: result.lastKitab,
      lastBab: result.lastBab,
      hadiths: deduped,
    };
    const outPath = join(
      config.cacheDir,
      filename.replace(".json", ".extracted.json")
    );
    writeFileSync(outPath, JSON.stringify(output, null, 2), "utf8");
  }

  return {
    lastKitab: result.lastKitab,
    lastBab: result.lastBab,
    count: deduped.length,
    emptyKitab,
    emptyBab,
    emptyMatn,
    chainVars,
    withFootnotes,
  };
}

// =============================================================================
// CLI
// =============================================================================

function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes("--dry-run");
  const chunkArg = args.find((a) => a.startsWith("--chunk="));
  const specificChunk = chunkArg ? parseInt(chunkArg.split("=")[1]) : null;
  const collectionArg = args.find((a) => a.startsWith("--collection="));

  if (!collectionArg) {
    console.error("Usage: bun run parse-turath-collection.ts --collection=SLUG [--chunk=N] [--dry-run]");
    console.error(`Available: ${ALL_REMAINING.join(", ")}`);
    process.exit(1);
  }

  const slug = collectionArg.slice(13);
  const config = getConfig(slug);

  if (config.parserType === "muslim") {
    console.error("Muslim uses a specialized parser. Run parse-muslim-pages.ts instead.");
    process.exit(1);
  }

  console.log(`=== Parsing ${config.name} ===`);
  console.log(`  Parser: standard (N - pattern)`);
  console.log(`  Kitab style: ${config.kitabStyle}`);
  console.log(`  Bab style: ${config.babStyle}`);
  console.log(`  Mode: ${dryRun ? "DRY RUN" : "WRITE"}`);

  if (!existsSync(config.cacheDir)) {
    console.error(`Cache directory not found: ${config.cacheDir}`);
    console.error("Run export-turath-pages.ts first.");
    process.exit(1);
  }

  // Find chunk files
  const files = readdirSync(config.cacheDir)
    .filter((f) => f.match(/^chunk-\d+\.json$/))
    .sort();

  if (files.length === 0) {
    console.error(`No chunk files found in ${config.cacheDir}`);
    process.exit(1);
  }

  console.log(`  Found ${files.length} chunk files.\n`);

  if (specificChunk !== null) {
    const targetFile = `chunk-${String(specificChunk).padStart(3, "0")}.json`;
    if (!files.includes(targetFile)) {
      console.error(`Chunk file not found: ${targetFile}`);
      process.exit(1);
    }
    processChunk(targetFile, config, dryRun);
  } else {
    // Process all chunks, maintaining kitab/bab state
    let prevKitab = "";
    let prevBab = "";
    let totalHadiths = 0;
    let emptyKitab = 0;
    let emptyBab = 0;
    let emptyMatn = 0;
    let chainVars = 0;
    let withFootnotes = 0;

    for (const file of files) {
      const result = processChunk(file, config, dryRun, prevKitab, prevBab);
      if (result) {
        prevKitab = result.lastKitab;
        prevBab = result.lastBab;
        totalHadiths += result.count;
        emptyKitab += result.emptyKitab;
        emptyBab += result.emptyBab;
        emptyMatn += result.emptyMatn;
        chainVars += result.chainVars;
        withFootnotes += result.withFootnotes;
      }
    }

    console.log("\n=== TOTALS ===");
    console.log(`Total hadiths: ${totalHadiths}`);
    if (totalHadiths > 0) {
      console.log(
        `Empty kitab: ${emptyKitab} (${((emptyKitab / totalHadiths) * 100).toFixed(1)}%)`
      );
      console.log(
        `Empty bab: ${emptyBab} (${((emptyBab / totalHadiths) * 100).toFixed(1)}%)`
      );
      console.log(
        `Empty matn: ${emptyMatn} (${((emptyMatn / totalHadiths) * 100).toFixed(1)}%)`
      );
      console.log(
        `Chain variations: ${chainVars} (${((chainVars / totalHadiths) * 100).toFixed(1)}%)`
      );
      console.log(
        `With footnotes: ${withFootnotes} (${((withFootnotes / totalHadiths) * 100).toFixed(1)}%)`
      );
    }
  }
}

main();
