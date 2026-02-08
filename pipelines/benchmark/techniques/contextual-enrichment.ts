/**
 * Technique 3: Contextual Enrichment
 * LLM-generated 2-3 sentence contextual description prepended to text.
 * Uses gpt-oss-120b via OpenRouter with parallel batch calls.
 * Results are cached in SQLite to avoid redundant LLM calls.
 */

import OpenAI from "openai";
import type { RetrievalTechnique, QuranAyahData, HadithData } from "../types";
import {
  normalizeArabicText,
  truncateForEmbedding,
} from "../../../src/embeddings/gemini";
import {
  getCachedEnrichment,
  setCachedEnrichments,
} from "../utils/llm-enrichment-cache";

const TECHNIQUE_ID = "contextual";
const LLM_MODEL = "openai/gpt-oss-120b";
const BATCH_SIZE = 10;
const CONCURRENCY = 10; // Number of parallel LLM calls

const openai = new OpenAI({
  apiKey: process.env.OPENROUTER_API_KEY,
  baseURL: "https://openrouter.ai/api/v1",
});

// Pending batch for deferred processing
interface PendingItem {
  contentType: "quran" | "hadith";
  contentId: string;
  text: string;
  metadata: string; // For the LLM prompt
}

const pendingBatch: PendingItem[] = [];
const enrichmentResults = new Map<string, string>();

/**
 * Generate contextual enrichments for a batch of items via LLM.
 */
async function generateBatchEnrichments(items: PendingItem[]): Promise<void> {
  const itemDescriptions = items
    .map((item, i) => `[${i + 1}] ${item.metadata}\nText: ${item.text.substring(0, 300)}`)
    .join("\n\n");

  const prompt = `For each Islamic text item below, generate a brief 2-3 sentence contextual description in Arabic followed by English. Explain what it discusses, its key themes, and relevant Islamic concepts. This context will be prepended to the text to improve search retrieval.

Format your response as:
[1] Arabic context. English context.
[2] Arabic context. English context.
...

Items:
${itemDescriptions}`;

  try {
    const response = await openai.chat.completions.create({
      model: LLM_MODEL,
      messages: [{ role: "user", content: prompt }],
      temperature: 0.3,
      max_tokens: 3000,
    });

    const content = response.choices[0]?.message?.content || "";
    // Parse numbered responses
    const entries: Array<{
      techniqueId: string;
      contentType: "quran" | "hadith";
      contentId: string;
      enrichedText: string;
    }> = [];

    for (let i = 0; i < items.length; i++) {
      const pattern = new RegExp(`\\[${i + 1}\\]\\s*(.+?)(?=\\[${i + 2}\\]|$)`, "s");
      const match = content.match(pattern);
      const enrichment = match ? match[1].trim() : "";

      if (enrichment) {
        const key = `${items[i].contentType}:${items[i].contentId}`;
        enrichmentResults.set(key, enrichment);
        entries.push({
          techniqueId: TECHNIQUE_ID,
          contentType: items[i].contentType,
          contentId: items[i].contentId,
          enrichedText: enrichment,
        });
      }
    }

    // Cache all results at once
    if (entries.length > 0) {
      setCachedEnrichments(entries);
    }
  } catch (err) {
    console.error("[ContextualEnrichment] LLM batch failed:", err);
  }
}

/**
 * Flush any pending batch items using parallel LLM calls.
 */
export async function flushPendingEnrichments(): Promise<void> {
  if (pendingBatch.length === 0) {
    console.log("  [ContextualEnrichment] No pending items to enrich");
    return;
  }

  const batch = pendingBatch.splice(0, pendingBatch.length);
  const totalBatches = Math.ceil(batch.length / BATCH_SIZE);
  console.log(`  [ContextualEnrichment] Generating enrichments for ${batch.length} items (${totalBatches} LLM batches, ${CONCURRENCY} parallel)...`);

  // Split into sub-batches of BATCH_SIZE
  const subBatches: PendingItem[][] = [];
  for (let i = 0; i < batch.length; i += BATCH_SIZE) {
    subBatches.push(batch.slice(i, i + BATCH_SIZE));
  }

  let completed = 0;
  let nextIndex = 0;

  // Worker pool: each worker grabs the next available sub-batch
  const workers = Array.from(
    { length: Math.min(CONCURRENCY, subBatches.length) },
    async () => {
      while (nextIndex < subBatches.length) {
        const idx = nextIndex++;
        await generateBatchEnrichments(subBatches[idx]);
        completed += subBatches[idx].length;
        if (completed % 100 < BATCH_SIZE || completed === batch.length) {
          console.log(`  [ContextualEnrichment] ${completed}/${batch.length} enrichments generated (${Math.round((completed / batch.length) * 100)}%)`);
        }
      }
    }
  );

  await Promise.all(workers);
  console.log(`  [ContextualEnrichment] All ${batch.length} enrichments complete.`);
}

/**
 * Get or generate contextual enrichment for an item.
 */
function getEnrichment(
  contentType: "quran" | "hadith",
  contentId: string,
  text: string,
  metadata: string
): string | null {
  // Check in-memory results first
  const key = `${contentType}:${contentId}`;
  const inMemory = enrichmentResults.get(key);
  if (inMemory) return inMemory;

  // Check SQLite cache
  const cached = getCachedEnrichment(TECHNIQUE_ID, contentType, contentId);
  if (cached) {
    enrichmentResults.set(key, cached);
    return cached;
  }

  // Queue for batch processing
  pendingBatch.push({ contentType, contentId, text, metadata });
  return null;
}

export const contextualEnrichmentTechnique: RetrievalTechnique = {
  id: TECHNIQUE_ID,
  name: "Contextual Enrichment",
  description: "LLM-generated 2-3 sentence contextual description prepended to text",

  async prepareQuranText(ayah: QuranAyahData): Promise<string | null> {
    const contentId = `quran_${ayah.surahNumber}_${ayah.ayahNumber}`;
    const metadata = `Quran, Surah ${ayah.surahNameEnglish} (${ayah.surahNameArabic}), Ayah ${ayah.ayahNumber}`;

    const enrichment = getEnrichment("quran", contentId, ayah.textPlain, metadata);

    if (enrichment === null) {
      // Will be processed in batch later; return null to signal "not ready"
      return null;
    }

    const arabicText = normalizeArabicText(ayah.textPlain);
    return truncateForEmbedding(`${enrichment}\n${arabicText}`);
  },

  async prepareHadithText(hadith: HadithData): Promise<string | null> {
    const contentId = `hadith_${hadith.collectionSlug}_${hadith.hadithNumber}`;
    const metadata = `Hadith, ${hadith.collectionNameEnglish}, #${hadith.hadithNumber}`;

    const enrichment = getEnrichment("hadith", contentId, hadith.textPlain, metadata);

    if (enrichment === null) {
      return null;
    }

    const arabicText = normalizeArabicText(hadith.textPlain);
    return truncateForEmbedding(`${enrichment}\n${arabicText}`);
  },
};
