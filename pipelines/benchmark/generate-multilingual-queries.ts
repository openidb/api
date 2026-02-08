/**
 * Generate multilingual crosslingual test queries.
 *
 * Takes the 22 English crosslingual queries from gold_standard_v2.jsonl
 * and translates them into 11 additional languages via LLM, producing
 * natural-sounding search queries a native speaker would type.
 *
 * Output: training/data/multilingual_crosslingual_v1.jsonl (264 entries = 22 × 12 languages)
 *
 * Usage:
 *   bun run scripts/benchmark-techniques/generate-multilingual-queries.ts [--force]
 */

import "../env";
import OpenAI from "openai";
import * as fs from "fs";
import * as path from "path";

const openai = new OpenAI({
  apiKey: process.env.OPENROUTER_API_KEY,
  baseURL: "https://openrouter.ai/api/v1",
});

const LLM_MODEL = "openai/gpt-4.1-mini";

// Target languages (excluding English which is already present)
const TARGET_LANGUAGES: Record<string, string> = {
  fr: "French",
  id: "Indonesian",
  ur: "Urdu",
  es: "Spanish",
  zh: "Chinese (Simplified)",
  pt: "Portuguese",
  ru: "Russian",
  ja: "Japanese",
  ko: "Korean",
  it: "Italian",
  bn: "Bengali",
};

const CACHE_DIR = path.join(process.cwd(), ".cache");
const CACHE_PATH = path.join(CACHE_DIR, "multilingual-queries.json");

interface GoldStandardEntry {
  query: string;
  relevant: string[];
  category: string;
  difficulty: string;
  language: string;
  notes?: string;
}

interface TranslationCache {
  [key: string]: string; // key = `${lang}:${query}`
}

function loadCache(): TranslationCache {
  try {
    if (fs.existsSync(CACHE_PATH)) {
      return JSON.parse(fs.readFileSync(CACHE_PATH, "utf-8"));
    }
  } catch {
    // ignore corrupt cache
  }
  return {};
}

function saveCache(cache: TranslationCache): void {
  if (!fs.existsSync(CACHE_DIR)) {
    fs.mkdirSync(CACHE_DIR, { recursive: true });
  }
  fs.writeFileSync(CACHE_PATH, JSON.stringify(cache, null, 2));
}

/**
 * Load crosslingual queries from gold standard file.
 */
function loadCrosslingualQueries(filepath: string): GoldStandardEntry[] {
  const content = fs.readFileSync(filepath, "utf-8");
  const lines = content.trim().split("\n").filter(Boolean);

  return lines
    .map((line) => JSON.parse(line) as GoldStandardEntry)
    .filter(
      (entry) =>
        entry.category === "quran_crosslingual" ||
        entry.category === "hadith_crosslingual"
    );
}

/**
 * Translate a batch of queries into a target language via LLM.
 */
async function translateBatch(
  queries: GoldStandardEntry[],
  langCode: string,
  langName: string,
  cache: TranslationCache
): Promise<Map<string, string>> {
  const results = new Map<string, string>();
  const uncached: GoldStandardEntry[] = [];

  // Check cache first
  for (const q of queries) {
    const key = `${langCode}:${q.query}`;
    if (cache[key]) {
      results.set(q.query, cache[key]);
    } else {
      uncached.push(q);
    }
  }

  if (uncached.length === 0) {
    console.log(`  ${langName}: all ${queries.length} queries cached`);
    return results;
  }

  console.log(
    `  ${langName}: ${results.size} cached, ${uncached.length} to translate...`
  );

  // Build numbered query list for the LLM
  const queryList = uncached
    .map((q, i) => `[${i + 1}] ${q.query}`)
    .join("\n");

  const prompt = `Translate these English search queries about Islamic texts (Quran and Hadith) into ${langName}.

IMPORTANT: Produce natural search queries that a native ${langName} speaker would actually type when searching. Do NOT translate word-for-word. Adapt the phrasing to be natural in ${langName}.

For religious terms (Allah, Quran, hadith, etc.), use the standard ${langName} forms commonly used by ${langName}-speaking Muslims.

Format: return ONLY the numbered translations, one per line:
[1] translated query
[2] translated query
...

Queries:
${queryList}`;

  try {
    const response = await openai.chat.completions.create({
      model: LLM_MODEL,
      messages: [{ role: "user", content: prompt }],
      temperature: 0.3,
      max_tokens: 4000,
    });

    const content = response.choices[0]?.message?.content || "";

    // Parse numbered responses
    for (let i = 0; i < uncached.length; i++) {
      const pattern = new RegExp(
        `\\[${i + 1}\\]\\s*(.+?)(?=\\n\\[${i + 2}\\]|\\n*$)`,
        "s"
      );
      const match = content.match(pattern);
      if (match) {
        const translated = match[1].trim();
        results.set(uncached[i].query, translated);
        cache[`${langCode}:${uncached[i].query}`] = translated;
      } else {
        console.warn(
          `  Warning: Could not parse translation [${i + 1}] for ${langName}`
        );
      }
    }

    console.log(
      `  ${langName}: translated ${results.size - (queries.length - uncached.length)}/${uncached.length} new queries`
    );
  } catch (err) {
    console.error(`  Error translating to ${langName}:`, err);
  }

  return results;
}

async function main() {
  const forceFlag = process.argv.includes("--force");

  // Load existing crosslingual queries
  const dataDir = path.join(__dirname, "../../training/data");
  const goldStandardPath = path.join(dataDir, "gold_standard_v2.jsonl");

  if (!fs.existsSync(goldStandardPath)) {
    console.error(`Gold standard not found: ${goldStandardPath}`);
    process.exit(1);
  }

  const crosslingualQueries = loadCrosslingualQueries(goldStandardPath);
  console.log(`Loaded ${crosslingualQueries.length} crosslingual queries`);

  const quranQueries = crosslingualQueries.filter((q) =>
    q.category.startsWith("quran")
  );
  const hadithQueries = crosslingualQueries.filter((q) =>
    q.category.startsWith("hadith")
  );
  console.log(`  Quran: ${quranQueries.length}, Hadith: ${hadithQueries.length}`);

  // Output path
  const outputPath = path.join(dataDir, "multilingual_crosslingual_v1.jsonl");

  if (fs.existsSync(outputPath) && !forceFlag) {
    console.log(`\nOutput already exists: ${outputPath}`);
    console.log("Use --force to regenerate.");
    process.exit(0);
  }

  // Load translation cache
  const cache = loadCache();
  const cachedCount = Object.keys(cache).length;
  console.log(`Translation cache: ${cachedCount} entries`);

  // Collect all output entries
  const outputEntries: GoldStandardEntry[] = [];

  // 1. Include original English queries
  for (const q of crosslingualQueries) {
    outputEntries.push({ ...q });
  }
  console.log(`\nIncluded ${crosslingualQueries.length} original English queries`);

  // 2. Translate to each target language
  console.log(
    `\nTranslating to ${Object.keys(TARGET_LANGUAGES).length} languages...`
  );

  for (const [langCode, langName] of Object.entries(TARGET_LANGUAGES)) {
    const translations = await translateBatch(
      crosslingualQueries,
      langCode,
      langName,
      cache
    );

    for (const q of crosslingualQueries) {
      const translated = translations.get(q.query);
      if (translated) {
        outputEntries.push({
          query: translated,
          relevant: q.relevant,
          category: q.category,
          difficulty: q.difficulty,
          language: langCode,
          notes: q.notes,
        });
      }
    }

    // Save cache after each language
    saveCache(cache);
  }

  // Write output JSONL
  const jsonlContent = outputEntries
    .map((entry) => JSON.stringify(entry))
    .join("\n")
    .concat("\n");

  fs.writeFileSync(outputPath, jsonlContent);
  console.log(`\nWrote ${outputEntries.length} entries to ${outputPath}`);

  // Summary
  const byLang = new Map<string, number>();
  for (const entry of outputEntries) {
    byLang.set(entry.language, (byLang.get(entry.language) || 0) + 1);
  }
  console.log("\nBy language:");
  for (const [lang, count] of [...byLang.entries()].sort()) {
    const name =
      lang === "en" ? "English" : TARGET_LANGUAGES[lang] || lang;
    console.log(`  ${lang} (${name}): ${count} queries`);
  }
}

main().catch((e) => {
  console.error("Failed:", e);
  process.exit(1);
});
