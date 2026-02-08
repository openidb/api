/**
 * Generate Synthetic Queries for Training Data (Precision-Optimized)
 *
 * Uses Gemini 2.0 Flash via OpenRouter to generate diverse search queries
 * for each Arabic passage. Implements stratified sampling for quality over quantity.
 *
 * Stratified Sampling Strategy (5K passages instead of all 52K):
 * - Quran (1,000 of 5,625):
 *   - 200 short ayahs (< 50 chars) - need most help
 *   - 200 famous/commonly searched
 *   - 200 with key Islamic terms
 *   - 200 narrative passages
 *   - 200 random
 * - Hadith (4,000 of 46,674):
 *   - 500 from 40 Nawawi
 *   - 500 fiqh-related
 *   - 500 aqeedah-related
 *   - 500 ethical teachings
 *   - 1000 long hadiths (> 200 chars)
 *   - 1000 random
 *
 * Output format (JSONL):
 * {"query": "generated query", "pos": ["arabic passage"], "neg": [], "source": "synthetic", "query_type": "natural_question", "language": "en"}
 *
 * Usage:
 *   bun run training/scripts/generate-synthetic-queries.ts [options]
 *
 * Options:
 *   --source=<type>    Source: quran | hadith | all (default: all)
 *   --limit=<n>        Limit number of passages to process (default: stratified)
 *   --offset=<n>       Skip first N passages (default: 0)
 *   --batch-size=<n>   Passages per API call (default: 5)
 *   --output=<file>    Output file (default: synthetic_queries.jsonl)
 *   --dry-run          Show what would be done without API calls
 *   --pilot            Run on 200 passages only (for validation)
 *   --full             Process all passages (original behavior, ~$45)
 *   --stratified       Use stratified sampling (default, ~$5 for 5K passages)
 */

import "dotenv/config";
import * as fs from "fs";
import * as path from "path";
import { prisma } from "../../lib/db";

// Configuration
const DATA_DIR = path.join(__dirname, "../data");
const DEFAULT_BATCH_SIZE = 5;
const OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions";
const MODEL = "google/gemini-2.0-flash-001"; // Fast and cost-effective
const REQUEST_DELAY_MS = 100; // Small delay between requests to avoid rate limits
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 2000;

// Stratified sampling configuration
const STRATIFIED_QURAN_COUNT = 1000;
const STRATIFIED_HADITH_COUNT = 4000;
const QUERIES_PER_PASSAGE = 3; // Reduced from 5 for higher quality

interface Passage {
  id: string;
  text: string;
  source: "quran" | "hadith";
  category?: string; // For stratified sampling tracking
  metadata: {
    surahNumber?: number;
    ayahNumber?: number;
    surahName?: string;
    collection?: string;
    bookName?: string;
    hadithNumber?: string;
  };
}

interface GeneratedQuery {
  query: string;
  queryType: string;
  language: string;
}

interface TrainingPair {
  query: string;
  pos: string[];
  neg: string[];
  source: string;
  query_type: string;
  language: string;
  passage_id: string;
}

// Key Islamic terms for stratified sampling
const KEY_ISLAMIC_TERMS = [
  "صلاة", "صلوة", "زكاة", "زكوة", "صوم", "صيام", "حج", "جهاد",
  "توحيد", "إيمان", "إحسان", "تقوى", "صبر", "شكر", "توبة",
  "جنة", "نار", "آخرة", "قيامة", "حساب", "ميزان",
];

// Famous surah names for sampling
const FAMOUS_SURAHS = [1, 2, 18, 36, 55, 67, 112, 113, 114]; // Fatiha, Baqarah, Kahf, Yasin, Rahman, Mulk, last 3

// Parse command line arguments
function parseArgs() {
  const args = process.argv.slice(2);
  const options: Record<string, string | boolean> = {};

  for (const arg of args) {
    if (arg.startsWith("--")) {
      const [key, value] = arg.slice(2).split("=");
      options[key] = value ?? true;
    }
  }

  return {
    source: (options["source"] as string) || "all",
    limit: options["limit"] ? parseInt(options["limit"] as string) : undefined,
    offset: parseInt(options["offset"] as string) || 0,
    batchSize: parseInt(options["batch-size"] as string) || DEFAULT_BATCH_SIZE,
    output: (options["output"] as string) || path.join(DATA_DIR, "synthetic_queries.jsonl"),
    dryRun: options["dry-run"] === true,
    pilot: options["pilot"] === true,
    full: options["full"] === true,
    stratified: !options["full"], // Stratified is default unless --full is specified
  };
}

/**
 * Fetch Quran passages with stratified sampling
 */
async function fetchQuranPassagesStratified(): Promise<Passage[]> {
  console.log("  Fetching Quran passages with stratified sampling...");

  const passages: Passage[] = [];
  const seenIds = new Set<string>();

  // Helper to add unique passages
  const addPassages = (newPassages: Passage[], category: string, limit: number) => {
    let added = 0;
    for (const p of newPassages) {
      if (!seenIds.has(p.id) && added < limit) {
        p.category = category;
        passages.push(p);
        seenIds.add(p.id);
        added++;
      }
    }
    console.log(`    ${category}: ${added} passages`);
  };

  // 1. Short ayahs (< 50 chars) - need most help
  const shortAyahs = await prisma.ayah.findMany({
    select: {
      id: true,
      textPlain: true,
      ayahNumber: true,
      surah: { select: { number: true, nameArabic: true, nameEnglish: true } },
    },
    where: { textPlain: { not: "" } },
    orderBy: [{ surahId: "asc" }, { ayahNumber: "asc" }],
  });

  addPassages(
    shortAyahs
      .filter((a) => a.textPlain.length > 10 && a.textPlain.length < 50)
      .map((a) => ({
        id: `quran_${a.surah.number}_${a.ayahNumber}`,
        text: a.textPlain,
        source: "quran" as const,
        metadata: {
          surahNumber: a.surah.number,
          ayahNumber: a.ayahNumber,
          surahName: a.surah.nameEnglish,
        },
      })),
    "short_ayahs",
    200
  );

  // 2. Famous surahs
  const famousAyahs = shortAyahs.filter((a) => FAMOUS_SURAHS.includes(a.surah.number));
  addPassages(
    famousAyahs.map((a) => ({
      id: `quran_${a.surah.number}_${a.ayahNumber}`,
      text: a.textPlain,
      source: "quran" as const,
      metadata: {
        surahNumber: a.surah.number,
        ayahNumber: a.ayahNumber,
        surahName: a.surah.nameEnglish,
      },
    })),
    "famous_surahs",
    200
  );

  // 3. Key Islamic terms
  const termAyahs = shortAyahs.filter((a) =>
    KEY_ISLAMIC_TERMS.some((term) => a.textPlain.includes(term))
  );
  addPassages(
    termAyahs.map((a) => ({
      id: `quran_${a.surah.number}_${a.ayahNumber}`,
      text: a.textPlain,
      source: "quran" as const,
      metadata: {
        surahNumber: a.surah.number,
        ayahNumber: a.ayahNumber,
        surahName: a.surah.nameEnglish,
      },
    })),
    "key_terms",
    200
  );

  // 4. Narrative passages (longer, story-like)
  const narrativeAyahs = shortAyahs.filter((a) => a.textPlain.length > 150);
  addPassages(
    narrativeAyahs.map((a) => ({
      id: `quran_${a.surah.number}_${a.ayahNumber}`,
      text: a.textPlain,
      source: "quran" as const,
      metadata: {
        surahNumber: a.surah.number,
        ayahNumber: a.ayahNumber,
        surahName: a.surah.nameEnglish,
      },
    })),
    "narrative",
    200
  );

  // 5. Random remaining to fill quota
  const remaining = STRATIFIED_QURAN_COUNT - passages.length;
  if (remaining > 0) {
    const randomAyahs = shortAyahs
      .filter((a) => !seenIds.has(`quran_${a.surah.number}_${a.ayahNumber}`))
      .filter((a) => a.textPlain.length >= 20)
      .sort(() => Math.random() - 0.5)
      .slice(0, remaining);

    addPassages(
      randomAyahs.map((a) => ({
        id: `quran_${a.surah.number}_${a.ayahNumber}`,
        text: a.textPlain,
        source: "quran" as const,
        metadata: {
          surahNumber: a.surah.number,
          ayahNumber: a.ayahNumber,
          surahName: a.surah.nameEnglish,
        },
      })),
      "random",
      remaining
    );
  }

  return passages;
}

/**
 * Fetch Hadith passages with stratified sampling
 */
async function fetchHadithPassagesStratified(): Promise<Passage[]> {
  console.log("  Fetching Hadith passages with stratified sampling...");

  const passages: Passage[] = [];
  const seenIds = new Set<string>();

  const addPassages = (newPassages: Passage[], category: string, limit: number) => {
    let added = 0;
    for (const p of newPassages) {
      if (!seenIds.has(p.id) && added < limit) {
        p.category = category;
        passages.push(p);
        seenIds.add(p.id);
        added++;
      }
    }
    console.log(`    ${category}: ${added} passages`);
  };

  // Fetch all hadiths
  const allHadiths = await prisma.hadith.findMany({
    select: {
      id: true,
      textPlain: true,
      hadithNumber: true,
      book: {
        select: {
          nameEnglish: true,
          slug: true,
          collection: { select: { slug: true, nameEnglish: true } },
        },
      },
    },
    where: { textPlain: { not: "" } },
  });

  const toPassage = (h: (typeof allHadiths)[0]): Passage => ({
    id: `hadith_${h.book.collection.slug}_${h.hadithNumber}`,
    text: h.textPlain,
    source: "hadith" as const,
    metadata: {
      collection: h.book.collection.nameEnglish,
      bookName: h.book.nameEnglish,
      hadithNumber: h.hadithNumber,
    },
  });

  // 1. 40 Nawawi (high-frequency, foundational)
  const nawawiHadiths = allHadiths.filter((h) => h.book.slug === "nawawi40");
  addPassages(nawawiHadiths.map(toPassage), "nawawi40", 500);

  // 2. Fiqh-related (rulings) - look for keywords
  const fiqhKeywords = ["حلال", "حرام", "فرض", "واجب", "سنة", "مكروه", "حكم", "قضاء"];
  const fiqhHadiths = allHadiths.filter((h) =>
    fiqhKeywords.some((kw) => h.textPlain.includes(kw))
  );
  addPassages(fiqhHadiths.map(toPassage), "fiqh", 500);

  // 3. Aqeedah-related (beliefs)
  const aqeedahKeywords = ["الله", "إيمان", "توحيد", "ملائكة", "قدر", "آخرة", "جنة", "نار"];
  const aqeedahHadiths = allHadiths.filter((h) =>
    aqeedahKeywords.some((kw) => h.textPlain.includes(kw))
  );
  addPassages(aqeedahHadiths.map(toPassage), "aqeedah", 500);

  // 4. Ethics/Akhlaq
  const akhlaqKeywords = ["خلق", "أدب", "صدق", "أمانة", "جار", "والد", "أخ", "رحم"];
  const akhlaqHadiths = allHadiths.filter((h) =>
    akhlaqKeywords.some((kw) => h.textPlain.includes(kw))
  );
  addPassages(akhlaqHadiths.map(toPassage), "akhlaq", 500);

  // 5. Long hadiths (> 200 chars) - more context
  const longHadiths = allHadiths.filter((h) => h.textPlain.length > 200);
  addPassages(longHadiths.map(toPassage), "long", 1000);

  // 6. Random remaining
  const remaining = STRATIFIED_HADITH_COUNT - passages.length;
  if (remaining > 0) {
    const randomHadiths = allHadiths
      .filter((h) => !seenIds.has(`hadith_${h.book.collection.slug}_${h.hadithNumber}`))
      .filter((h) => h.textPlain.length >= 30)
      .sort(() => Math.random() - 0.5)
      .slice(0, remaining);

    addPassages(randomHadiths.map(toPassage), "random", remaining);
  }

  return passages;
}

/**
 * Fetch all Quran passages (original behavior)
 */
async function fetchQuranPassagesAll(limit?: number, offset = 0): Promise<Passage[]> {
  const ayahs = await prisma.ayah.findMany({
    select: {
      id: true,
      textPlain: true,
      ayahNumber: true,
      surah: {
        select: {
          number: true,
          nameArabic: true,
          nameEnglish: true,
        },
      },
    },
    where: {
      textPlain: { not: "" },
    },
    orderBy: [{ surahId: "asc" }, { ayahNumber: "asc" }],
    skip: offset,
    take: limit,
  });

  return ayahs
    .filter((a) => a.textPlain.length >= 20)
    .map((a) => ({
      id: `quran_${a.surah.number}_${a.ayahNumber}`,
      text: a.textPlain,
      source: "quran" as const,
      metadata: {
        surahNumber: a.surah.number,
        ayahNumber: a.ayahNumber,
        surahName: a.surah.nameEnglish,
      },
    }));
}

/**
 * Fetch all Hadith passages (original behavior)
 */
async function fetchHadithPassagesAll(limit?: number, offset = 0): Promise<Passage[]> {
  const hadiths = await prisma.hadith.findMany({
    select: {
      id: true,
      textPlain: true,
      hadithNumber: true,
      book: {
        select: {
          nameEnglish: true,
          collection: {
            select: {
              slug: true,
              nameEnglish: true,
            },
          },
        },
      },
    },
    where: {
      textPlain: { not: "" },
    },
    orderBy: [{ bookId: "asc" }, { hadithNumber: "asc" }],
    skip: offset,
    take: limit,
  });

  return hadiths
    .filter((h) => h.textPlain.length >= 30)
    .map((h) => ({
      id: `hadith_${h.book.collection.slug}_${h.hadithNumber}`,
      text: h.textPlain,
      source: "hadith" as const,
      metadata: {
        collection: h.book.collection.nameEnglish,
        bookName: h.book.nameEnglish,
        hadithNumber: h.hadithNumber,
      },
    }));
}

/**
 * Build the improved prompt for query generation (3 queries, higher quality)
 */
function buildPrompt(passages: Passage[]): string {
  const passageList = passages
    .map((p, i) => {
      let context = "";
      if (p.source === "quran") {
        context = `[Quran - Surah ${p.metadata.surahName}, Ayah ${p.metadata.ayahNumber}]`;
      } else {
        context = `[Hadith - ${p.metadata.collection}]`;
      }
      return `${i + 1}. ${context}\n${p.text}`;
    })
    .join("\n\n");

  return `Generate 3 HIGH-QUALITY search queries for each Arabic Islamic passage below.

Requirements for EACH query:
1. Each query must be SPECIFIC enough to find THIS passage, not just the general topic
2. Include at least one query using KEY TERMINOLOGY from the passage
3. For Quranic text: Consider literal meaning and common tafsir interpretations
4. For Hadith: Include the topic and any specific ruling or teaching

Quality criteria:
- A good query should retrieve this passage in top 3 results
- Avoid generic queries like "hadith about Islam" or "Quran verse"
- For Arabic queries, use فصحى but include terms users actually search
- Be specific: "hadith about patience during illness" > "hadith about patience"

Query types to generate:
1. A natural question in English (e.g., "What does the Quran say about seeking help through patience?")
2. A keyword-style query in Arabic using key terms from the text (e.g., "الصبر الصلاة الاستعانة")
3. A conceptual/thematic query focusing on the main teaching (e.g., "Islamic guidance on perseverance during hardship")

Arabic Passages:
${passageList}

Return your response as a JSON array with one object per passage. Each object should have:
- "passage_index": the passage number (1-${passages.length})
- "queries": array of 3 query objects, each with "text" (the query), "type" (natural_question/keywords/conceptual), and "language" (en/ar)

Example format:
[
  {
    "passage_index": 1,
    "queries": [
      {"text": "What is the Islamic guidance on seeking help through patience and prayer?", "type": "natural_question", "language": "en"},
      {"text": "الصبر الصلاة الاستعانة بالله", "type": "keywords", "language": "ar"},
      {"text": "spiritual strength through worship during difficulties", "type": "conceptual", "language": "en"}
    ]
  }
]

IMPORTANT: Return ONLY valid JSON, no other text.`;
}

/**
 * Call OpenRouter API to generate queries
 */
async function generateQueriesWithLLM(
  passages: Passage[],
  retries = 0
): Promise<Map<number, GeneratedQuery[]>> {
  const apiKey = process.env.OPENROUTER_API_KEY;
  if (!apiKey) {
    throw new Error("OPENROUTER_API_KEY environment variable not set");
  }

  const prompt = buildPrompt(passages);

  try {
    const response = await fetch(OPENROUTER_API_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
        "HTTP-Referer": "https://github.com/arabic-texts-library",
        "X-Title": "Arabic Islamic Text Training Data Generator",
      },
      body: JSON.stringify({
        model: MODEL,
        messages: [
          {
            role: "user",
            content: prompt,
          },
        ],
        temperature: 0.7,
        max_tokens: 4096,
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`OpenRouter API error: ${response.status} - ${errorText}`);
    }

    const data = await response.json();
    const content = data.choices?.[0]?.message?.content;

    if (!content) {
      throw new Error("No content in API response");
    }

    // Parse JSON from response (handle markdown code blocks)
    let jsonStr = content.trim();
    if (jsonStr.startsWith("```json")) {
      jsonStr = jsonStr.slice(7);
    } else if (jsonStr.startsWith("```")) {
      jsonStr = jsonStr.slice(3);
    }
    if (jsonStr.endsWith("```")) {
      jsonStr = jsonStr.slice(0, -3);
    }
    jsonStr = jsonStr.trim();

    const parsed = JSON.parse(jsonStr) as Array<{
      passage_index: number;
      queries: Array<{ text: string; type: string; language: string }>;
    }>;

    // Convert to map
    const result = new Map<number, GeneratedQuery[]>();
    for (const item of parsed) {
      const queries = item.queries.map((q) => ({
        query: q.text,
        queryType: q.type,
        language: q.language,
      }));
      result.set(item.passage_index - 1, queries); // Convert to 0-indexed
    }

    return result;
  } catch (error) {
    if (retries < MAX_RETRIES) {
      console.warn(`Retrying after error (attempt ${retries + 1}):`, error);
      await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
      return generateQueriesWithLLM(passages, retries + 1);
    }
    throw error;
  }
}

/**
 * Process a batch of passages and generate training pairs
 */
async function processBatch(
  passages: Passage[],
  dryRun: boolean
): Promise<TrainingPair[]> {
  if (dryRun) {
    // In dry run, just return placeholder pairs
    return passages.flatMap((p) =>
      Array(QUERIES_PER_PASSAGE)
        .fill(null)
        .map((_, i) => ({
          query: `[DRY RUN] Query ${i + 1} for ${p.id}`,
          pos: [p.text],
          neg: [],
          source: "synthetic",
          query_type: "dry_run",
          language: i < 1 ? "en" : i < 2 ? "ar" : "en",
          passage_id: p.id,
        }))
    );
  }

  const queriesMap = await generateQueriesWithLLM(passages);
  const pairs: TrainingPair[] = [];

  for (let i = 0; i < passages.length; i++) {
    const passage = passages[i];
    const queries = queriesMap.get(i) || [];

    for (const q of queries) {
      pairs.push({
        query: q.query,
        pos: [passage.text],
        neg: [],
        source: "synthetic",
        query_type: q.queryType,
        language: q.language,
        passage_id: passage.id,
      });
    }
  }

  return pairs;
}

/**
 * Append training pairs to JSONL file
 */
function appendToJsonl(pairs: TrainingPair[], outputPath: string): void {
  const lines = pairs.map((p) => JSON.stringify(p)).join("\n") + "\n";
  fs.appendFileSync(outputPath, lines);
}

async function main() {
  const options = parseArgs();

  console.log("=".repeat(60));
  console.log("Synthetic Query Generation for BGE-M3 Training");
  console.log(options.stratified ? "(Precision-Optimized with Stratified Sampling)" : "(Full Dataset)");
  console.log("=".repeat(60));
  console.log();

  // Check API key
  if (!process.env.OPENROUTER_API_KEY && !options.dryRun) {
    console.error("ERROR: OPENROUTER_API_KEY environment variable not set.");
    process.exit(1);
  }

  // Determine mode
  let mode = "stratified";
  if (options.pilot) {
    mode = "pilot";
    console.log("PILOT MODE: Processing 200 passages only (~$0.20).\n");
  } else if (options.full) {
    mode = "full";
    console.log("FULL MODE: Processing all passages (~$45).\n");
  } else {
    console.log("STRATIFIED MODE: Processing ~5,000 selected passages (~$5).\n");
  }

  // Fetch passages
  console.log("Fetching passages from database...");
  let passages: Passage[] = [];

  if (mode === "pilot") {
    // Pilot mode: 100 Quran + 100 Hadith
    const quranPassages = await fetchQuranPassagesAll(100, 0);
    const hadithPassages = await fetchHadithPassagesAll(100, 0);
    passages = [...quranPassages, ...hadithPassages];
    console.log(`  Pilot: ${quranPassages.length} Quran, ${hadithPassages.length} Hadith\n`);
  } else if (mode === "full") {
    // Full mode: all passages
    if (options.source === "quran" || options.source === "all") {
      const quranPassages = await fetchQuranPassagesAll(options.limit, options.offset);
      passages.push(...quranPassages);
      console.log(`  Quran: ${quranPassages.length} passages`);
    }
    if (options.source === "hadith" || options.source === "all") {
      const hadithPassages = await fetchHadithPassagesAll(options.limit, options.offset);
      passages.push(...hadithPassages);
      console.log(`  Hadith: ${hadithPassages.length} passages`);
    }
    console.log();
  } else {
    // Stratified mode (default)
    if (options.source === "quran" || options.source === "all") {
      const quranPassages = await fetchQuranPassagesStratified();
      passages.push(...quranPassages);
    }
    if (options.source === "hadith" || options.source === "all") {
      const hadithPassages = await fetchHadithPassagesStratified();
      passages.push(...hadithPassages);
    }
    console.log();
  }

  // Apply overall limit if specified
  if (options.limit && mode !== "pilot") {
    passages = passages.slice(0, options.limit);
  }

  console.log(`Total passages to process: ${passages.length}`);
  console.log(`Queries per passage: ${QUERIES_PER_PASSAGE}`);
  console.log(`Expected training pairs: ~${passages.length * QUERIES_PER_PASSAGE}\n`);

  if (passages.length === 0) {
    console.error("No passages found. Please check the database.");
    process.exit(1);
  }

  // Configuration summary
  console.log("Configuration:");
  console.log(`  Model: ${MODEL}`);
  console.log(`  Batch size: ${options.batchSize}`);
  console.log(`  Output: ${options.output}`);
  console.log(`  Dry run: ${options.dryRun}`);
  console.log();

  // Estimate cost
  const estimatedInputTokens = passages.length * 350;
  const estimatedOutputTokens = passages.length * 60; // Reduced for 3 queries
  const estimatedCost = (estimatedInputTokens * 0.1 + estimatedOutputTokens * 0.4) / 1000000;
  console.log("Cost estimate (Gemini 2.0 Flash):");
  console.log(`  Input tokens: ~${(estimatedInputTokens / 1000000).toFixed(2)}M`);
  console.log(`  Output tokens: ~${(estimatedOutputTokens / 1000000).toFixed(2)}M`);
  console.log(`  Estimated cost: ~$${estimatedCost.toFixed(2)}`);
  console.log();

  if (options.dryRun) {
    console.log("DRY RUN - No API calls will be made.");
    console.log(`Would generate ~${passages.length * QUERIES_PER_PASSAGE} training pairs.`);

    // Show sample prompt
    console.log("\nSample prompt for first batch:");
    console.log("-".repeat(40));
    const sampleBatch = passages.slice(0, Math.min(options.batchSize, passages.length));
    console.log(buildPrompt(sampleBatch).substring(0, 1500) + "...");

    // Show category distribution
    if (mode === "stratified") {
      console.log("\nCategory distribution:");
      const categories = new Map<string, number>();
      for (const p of passages) {
        const cat = p.category || "unknown";
        categories.set(cat, (categories.get(cat) || 0) + 1);
      }
      for (const [cat, count] of categories.entries()) {
        console.log(`  ${cat}: ${count}`);
      }
    }
    return;
  }

  // Initialize output file (clear if exists)
  fs.writeFileSync(options.output, "");

  // Process in batches
  let totalPairs = 0;
  let processedPassages = 0;
  const startTime = Date.now();

  console.log("Generating queries...");
  for (let i = 0; i < passages.length; i += options.batchSize) {
    const batch = passages.slice(i, i + options.batchSize);

    try {
      const pairs = await processBatch(batch, false);
      appendToJsonl(pairs, options.output);
      totalPairs += pairs.length;
      processedPassages += batch.length;

      // Progress report
      const elapsed = (Date.now() - startTime) / 1000;
      const rate = processedPassages / elapsed;
      const remaining = (passages.length - processedPassages) / rate;
      console.log(
        `  Processed ${processedPassages}/${passages.length} passages ` +
          `(${totalPairs} pairs, ${rate.toFixed(1)}/sec, ~${Math.ceil(remaining / 60)}min remaining)`
      );

      // Small delay to avoid rate limits
      await new Promise((resolve) => setTimeout(resolve, REQUEST_DELAY_MS));
    } catch (error) {
      console.error(`Error processing batch ${i / options.batchSize + 1}:`, error);
      // Continue with next batch
    }
  }

  const totalTime = (Date.now() - startTime) / 1000;
  console.log();
  console.log("Generation complete!");
  console.log(`  Total passages: ${processedPassages}`);
  console.log(`  Total training pairs: ${totalPairs}`);
  console.log(`  Time: ${(totalTime / 60).toFixed(1)} minutes`);
  console.log(`  Output: ${options.output}`);

  // Show sample output
  console.log();
  console.log("Sample output:");
  console.log("-".repeat(60));
  const content = fs.readFileSync(options.output, "utf-8");
  const lines = content.trim().split("\n").slice(0, 3);
  for (const line of lines) {
    const pair = JSON.parse(line);
    console.log(`Query (${pair.language}): ${pair.query.substring(0, 60)}...`);
    console.log(`Positive: ${pair.pos[0].substring(0, 50)}...`);
    console.log();
  }
}

main()
  .catch((e) => {
    console.error("Failed:", e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
