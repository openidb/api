/**
 * Generate Arabic Paraphrases for Same-Language Training
 *
 * Creates Arabic↔Arabic training pairs by generating Modern Standard Arabic (MSA)
 * paraphrases of Quranic and Hadith texts. This teaches the model to match:
 * - MSA style queries → Classical Arabic text
 * - Simplified Arabic → Original Arabic
 *
 * Critical for improving Arabic→Arabic search quality.
 *
 * Output format (JSONL):
 * {"query": "MSA paraphrase", "pos": ["original Arabic"], "neg": [], "source": "paraphrase", "pair_type": "msa_classical"}
 *
 * Usage:
 *   bun run training/scripts/generate-arabic-paraphrases.ts [options]
 *
 * Options:
 *   --source=<type>    Source: quran | hadith | all (default: all)
 *   --limit=<n>        Limit number of passages (default: all)
 *   --offset=<n>       Skip first N passages (default: 0)
 *   --batch-size=<n>   Passages per API call (default: 5)
 *   --output=<file>    Output file (default: arabic_paraphrases.jsonl)
 *   --dry-run          Show what would be done without API calls
 *   --pilot            Run on 500 passages only (for validation)
 */

import "dotenv/config";
import * as fs from "fs";
import * as path from "path";
import { prisma } from "../../lib/db";

// Configuration
const DATA_DIR = path.join(__dirname, "../data");
const DEFAULT_BATCH_SIZE = 5;
const OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions";
const MODEL = "google/gemini-2.0-flash-001";
const REQUEST_DELAY_MS = 100;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 2000;

interface Passage {
  id: string;
  text: string;
  source: "quran" | "hadith";
  metadata: {
    surahNumber?: number;
    ayahNumber?: number;
    surahName?: string;
    collection?: string;
    bookName?: string;
    hadithNumber?: string;
  };
}

interface Paraphrase {
  simplified: string;  // Simplified MSA version
  keywords: string;    // Key Arabic search terms
}

interface TrainingPair {
  query: string;
  pos: string[];
  neg: string[];
  source: string;
  pair_type: string;
  passage_id: string;
}

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
    output: (options["output"] as string) || path.join(DATA_DIR, "arabic_paraphrases.jsonl"),
    dryRun: options["dry-run"] === true,
    pilot: options["pilot"] === true,
  };
}

/**
 * Fetch Quran passages from database
 */
async function fetchQuranPassages(limit?: number, offset = 0): Promise<Passage[]> {
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
    .filter((a) => a.textPlain.length >= 30) // Longer minimum for paraphrasing
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
 * Fetch Hadith passages from database
 */
async function fetchHadithPassages(limit?: number, offset = 0): Promise<Passage[]> {
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
    .filter((h) => h.textPlain.length >= 50) // Longer minimum for paraphrasing
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
 * Build the prompt for Arabic paraphrase generation
 */
function buildPrompt(passages: Passage[]): string {
  const passageList = passages
    .map((p, i) => {
      let context = "";
      if (p.source === "quran") {
        context = `[قرآن - سورة ${p.metadata.surahName}، آية ${p.metadata.ayahNumber}]`;
      } else {
        context = `[حديث - ${p.metadata.collection}]`;
      }
      return `${i + 1}. ${context}\n${p.text}`;
    })
    .join("\n\n");

  return `أنت تساعد في إنشاء بيانات تدريب لمحرك بحث النصوص الإسلامية العربية.

لكل نص عربي أدناه، قم بإنشاء:
1. **إعادة صياغة مبسطة**: نسخة بالعربية الفصحى الحديثة (MSA) تحافظ على المعنى بكلمات أبسط وأكثر شيوعاً. يجب أن تكون طبيعية وسهلة الفهم.
2. **كلمات مفتاحية**: 3-5 كلمات عربية رئيسية يمكن للمستخدم البحث بها للوصول لهذا النص.

هام جداً:
- حافظ على المعنى الأصلي بدقة
- استخدم العربية الفصحى الحديثة (ليس العامية)
- لا تغير المعنى الديني أو تحرفه
- إعادة الصياغة يجب أن تكون جملة كاملة ومفيدة

النصوص العربية:
${passageList}

أرجع الإجابة بصيغة JSON مع كائن واحد لكل نص:

[
  {
    "passage_index": 1,
    "simplified": "النص المبسط بالعربية الفصحى الحديثة",
    "keywords": "كلمة1 كلمة2 كلمة3"
  }
]

مثال:
النص الأصلي: "إنما الأعمال بالنيات وإنما لكل امرئ ما نوى"
الإجابة:
{
  "passage_index": 1,
  "simplified": "تُقبل الأعمال حسب نية صاحبها، ولكل شخص ما قصده من عمله",
  "keywords": "النية الأعمال القصد الثواب"
}

هام: أرجع JSON صالح فقط، بدون أي نص آخر.`;
}

/**
 * Call OpenRouter API to generate paraphrases
 */
async function generateParaphrasesWithLLM(
  passages: Passage[],
  retries = 0
): Promise<Map<number, Paraphrase>> {
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
        temperature: 0.5, // Lower temperature for more consistent paraphrases
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

    // Parse JSON from response
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
      simplified: string;
      keywords: string;
    }>;

    // Convert to map
    const result = new Map<number, Paraphrase>();
    for (const item of parsed) {
      result.set(item.passage_index - 1, {
        simplified: item.simplified,
        keywords: item.keywords,
      });
    }

    return result;
  } catch (error) {
    if (retries < MAX_RETRIES) {
      console.warn(`Retrying after error (attempt ${retries + 1}):`, error);
      await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
      return generateParaphrasesWithLLM(passages, retries + 1);
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
    return passages.flatMap((p) => [
      {
        query: `[DRY RUN] Simplified paraphrase for ${p.id}`,
        pos: [p.text],
        neg: [],
        source: "paraphrase",
        pair_type: "msa_simplified",
        passage_id: p.id,
      },
      {
        query: `[DRY RUN] Keywords for ${p.id}`,
        pos: [p.text],
        neg: [],
        source: "paraphrase",
        pair_type: "keywords_ar",
        passage_id: p.id,
      },
    ]);
  }

  const paraphrasesMap = await generateParaphrasesWithLLM(passages);
  const pairs: TrainingPair[] = [];

  for (let i = 0; i < passages.length; i++) {
    const passage = passages[i];
    const paraphrase = paraphrasesMap.get(i);

    if (paraphrase) {
      // Simplified paraphrase → Original
      if (paraphrase.simplified && paraphrase.simplified.length > 10) {
        pairs.push({
          query: paraphrase.simplified,
          pos: [passage.text],
          neg: [],
          source: "paraphrase",
          pair_type: "msa_simplified",
          passage_id: passage.id,
        });
      }

      // Keywords → Original
      if (paraphrase.keywords && paraphrase.keywords.length > 5) {
        pairs.push({
          query: paraphrase.keywords,
          pos: [passage.text],
          neg: [],
          source: "paraphrase",
          pair_type: "keywords_ar",
          passage_id: passage.id,
        });
      }
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
  console.log("Arabic Paraphrase Generation for Same-Language Training");
  console.log("=".repeat(60));
  console.log();

  // Check API key
  if (!process.env.OPENROUTER_API_KEY && !options.dryRun) {
    console.error("ERROR: OPENROUTER_API_KEY environment variable not set.");
    process.exit(1);
  }

  // Determine limits
  let limit = options.limit;
  if (options.pilot) {
    limit = 500;
    console.log("PILOT MODE: Processing 500 passages only.\n");
  }

  // Fetch passages
  console.log("Fetching passages from database...");
  let passages: Passage[] = [];

  if (options.source === "quran" || options.source === "all") {
    const quranPassages = await fetchQuranPassages(
      options.source === "quran" ? limit : undefined,
      options.source === "quran" ? options.offset : 0
    );
    passages.push(...quranPassages);
    console.log(`  Quran: ${quranPassages.length} passages`);
  }

  if (options.source === "hadith" || options.source === "all") {
    const hadithPassages = await fetchHadithPassages(
      options.source === "hadith" ? limit : undefined,
      options.source === "hadith" ? options.offset : 0
    );
    passages.push(...hadithPassages);
    console.log(`  Hadith: ${hadithPassages.length} passages`);
  }

  // Apply overall limit if processing all
  if (options.source === "all" && limit) {
    passages = passages.slice(options.offset, options.offset + limit);
  }

  console.log(`Total passages to process: ${passages.length}\n`);

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
  const estimatedInputTokens = passages.length * 400;
  const estimatedOutputTokens = passages.length * 100;
  console.log("Cost estimate (Gemini 2.0 Flash):");
  console.log(`  Input tokens: ~${(estimatedInputTokens / 1000000).toFixed(2)}M`);
  console.log(`  Output tokens: ~${(estimatedOutputTokens / 1000000).toFixed(2)}M`);
  console.log(`  Estimated cost: ~$${((estimatedInputTokens * 0.1 + estimatedOutputTokens * 0.4) / 1000000).toFixed(2)}`);
  console.log();

  if (options.dryRun) {
    console.log("DRY RUN - No API calls will be made.");
    console.log(`Would generate ~${passages.length * 2} training pairs (simplified + keywords).`);

    // Show sample prompt
    console.log("\nSample prompt for first batch:");
    console.log("-".repeat(40));
    const sampleBatch = passages.slice(0, Math.min(options.batchSize, passages.length));
    console.log(buildPrompt(sampleBatch).substring(0, 1500) + "...");
    return;
  }

  // Initialize output file
  fs.writeFileSync(options.output, "");

  // Process in batches
  let totalPairs = 0;
  let processedPassages = 0;
  const startTime = Date.now();

  console.log("Generating paraphrases...");
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

      await new Promise((resolve) => setTimeout(resolve, REQUEST_DELAY_MS));
    } catch (error) {
      console.error(`Error processing batch ${i / options.batchSize + 1}:`, error);
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
  const lines = content.trim().split("\n").slice(0, 4);
  for (const line of lines) {
    const pair = JSON.parse(line);
    console.log(`Type: ${pair.pair_type}`);
    console.log(`Query: ${pair.query.substring(0, 60)}...`);
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
