import type {
  RerankerType,
  RankedResult,
  AyahRankedResult,
  HadithRankedResult,
  UnifiedRefineResult,
} from "./types";
import { callOpenRouter } from "../../lib/openrouter";
import { RERANKER_TEXT_LIMIT, UNIFIED_RERANKER_TEXT_LIMIT, UNIFIED_RERANK_TIMEOUT_MS } from "./config";

export const RERANKER_CONFIG: Record<string, { model: string; timeoutMs: number }> = {
  "gpt-oss-20b": { model: "openai/gpt-oss-20b", timeoutMs: 20000 },
  "gpt-oss-120b": { model: "openai/gpt-oss-120b", timeoutMs: 20000 },
  "gemini-flash": { model: "google/gemini-3-flash-preview", timeoutMs: 15000 },
};

export function formatAyahForReranking(ayah: AyahRankedResult): string {
  const range = ayah.ayahEnd ? `${ayah.ayahNumber}-${ayah.ayahEnd}` : String(ayah.ayahNumber);
  return `[QURAN] ${ayah.surahNameArabic} (${ayah.surahNameEnglish}), Ayah ${range}
${ayah.text.slice(0, RERANKER_TEXT_LIMIT)}`;
}

export function formatHadithForReranking(hadith: HadithRankedResult): string {
  const chapter = hadith.chapterArabic ? ` - ${hadith.chapterArabic}` : '';
  return `[HADITH] ${hadith.collectionNameArabic} (${hadith.collectionNameEnglish}), ${hadith.bookNameArabic}${chapter}
${hadith.text.slice(0, RERANKER_TEXT_LIMIT)}`;
}

export function formatBookForReranking(result: RankedResult, bookTitle?: string, authorName?: string): string {
  const meta = bookTitle ? `[BOOK] ${bookTitle}${authorName ? ` - ${authorName}` : ''}, p.${result.pageNumber}` : `[BOOK] Page ${result.pageNumber}`;
  return `${meta}
${result.textSnippet.slice(0, RERANKER_TEXT_LIMIT)}`;
}

function buildRerankerPrompt(query: string, docsText: string): string {
  return `You are ranking Arabic/Islamic documents for a search query.

Query: "${query}"

Documents:
${docsText}

STEP 1: DETERMINE USER INTENT
Identify which type of search this is:

A) SPECIFIC SOURCE LOOKUP - User wants a particular Quran verse or hadith
   Indicators: Named verses, famous hadiths by title, surah/ayah references

B) QUESTION - User seeks an answer

C) TOPIC SEARCH - User wants content about a subject

STEP 2: RANK BY INTENT

**If SPECIFIC SOURCE LOOKUP (A):**
1. [QURAN] or [HADITH] containing the EXACT verse/hadith being searched (HIGHEST)
2. Related sources
3. [BOOK] with detailed tafsir/sharh
4. [BOOK] that quotes the source
5. Unrelated content (LOWEST)

**If QUESTION (B):**
1. Documents that directly ANSWER the question (highest)
2. Documents that explain/discuss the answer
3. Documents that mention the topic but don't answer
4. Unrelated documents (lowest)

**If TOPIC SEARCH (C):**
1. Documents primarily ABOUT the topic (highest)
2. Documents with significant discussion
3. Documents mentioning topic in context
4. Unrelated documents (lowest)

CROSS-LINGUAL MATCHING:
- Match English to Arabic and vice versa

Return ONLY a JSON array of document numbers by relevance: [3, 1, 5, 2, 4]`;
}

function parseLLMRanking<T>(content: string, results: T[], topN: number): T[] | null {
  const match = content.match(/\[[\d,\s]+\]/);
  if (!match) return null;

  const ranking: number[] = JSON.parse(match[0]);
  const reranked: T[] = [];

  for (const docNum of ranking.slice(0, topN)) {
    const idx = docNum - 1;
    if (idx >= 0 && idx < results.length && !reranked.includes(results[idx])) {
      reranked.push(results[idx]);
    }
  }

  for (const result of results) {
    if (reranked.length >= topN) break;
    if (!reranked.includes(result)) {
      reranked.push(result);
    }
  }

  return reranked.slice(0, topN);
}

async function rerankWithLLM<T>(
  query: string,
  results: T[],
  getText: (item: T) => string,
  topN: number,
  model: string,
  timeoutMs: number
): Promise<{ results: T[]; timedOut: boolean }> {
  if (results.length === 0) {
    return { results: results.slice(0, topN), timedOut: false };
  }

  try {
    const docsText = results
      .map((d, i) => `[${i + 1}] ${getText(d).slice(0, RERANKER_TEXT_LIMIT)}`)
      .join("\n\n");

    const prompt = buildRerankerPrompt(query, docsText);

    const result = await callOpenRouter({
      model,
      messages: [{ role: "user", content: prompt }],
      temperature: 0,
      timeoutMs,
    });

    if (!result) {
      return { results: results.slice(0, topN), timedOut: false };
    }

    const reranked = parseLLMRanking(result.content, results, topN);
    if (!reranked) {
      console.warn(`[Reranker] ${model} returned invalid format, using original order`);
      return { results: results.slice(0, topN), timedOut: false };
    }

    return { results: reranked, timedOut: false };
  } catch (err) {
    const timedOut = err instanceof Error && err.name === 'AbortError';
    if (timedOut) {
      console.warn(`[Reranker] ${model} timed out after ${timeoutMs}ms, using RRF order`);
    } else {
      console.warn(`[Reranker] ${model} failed, using original order:`, err);
    }
    return { results: results.slice(0, topN), timedOut };
  }
}

export async function rerank<T>(
  query: string,
  results: T[],
  getText: (item: T) => string,
  topN: number,
  reranker: RerankerType
): Promise<{ results: T[]; timedOut: boolean }> {
  const config = RERANKER_CONFIG[reranker];
  if (results.length === 0 || reranker === "none" || !config) {
    return { results: results.slice(0, topN), timedOut: false };
  }

  return rerankWithLLM(query, results, getText, topN, config.model, config.timeoutMs);
}

function fallbackRefineResult(
  books: RankedResult[],
  ayahs: AyahRankedResult[],
  hadiths: HadithRankedResult[],
  limits: { books: number; ayahs: number; hadiths: number },
  timedOut = false,
) {
  return {
    books: books.slice(0, limits.books),
    ayahs: ayahs.slice(0, limits.ayahs),
    hadiths: hadiths.slice(0, limits.hadiths),
    timedOut,
  };
}

export async function rerankUnifiedRefine(
  query: string,
  ayahs: AyahRankedResult[],
  hadiths: HadithRankedResult[],
  books: RankedResult[],
  bookMetaMap: Map<string, { titleArabic: string; author: { nameArabic: string } }>,
  limits: { books: number; ayahs: number; hadiths: number },
  reranker: RerankerType
): Promise<{
  books: RankedResult[];
  ayahs: AyahRankedResult[];
  hadiths: HadithRankedResult[];
  timedOut: boolean;
}> {
  if (reranker === "none") {
    return fallbackRefineResult(books, ayahs, hadiths, limits);
  }

  const unified: UnifiedRefineResult[] = [];

  books.slice(0, limits.books).forEach((b, i) => {
    const book = bookMetaMap.get(b.bookId);
    unified.push({
      type: 'book',
      index: i,
      content: formatBookForReranking(b, book?.titleArabic, book?.author.nameArabic),
      originalScore: b.semanticScore || b.fusedScore || 0
    });
  });

  ayahs.slice(0, limits.ayahs).forEach((a, i) => {
    unified.push({
      type: 'ayah',
      index: i,
      content: formatAyahForReranking(a),
      originalScore: a.semanticScore || a.score
    });
  });

  hadiths.slice(0, limits.hadiths).forEach((h, i) => {
    unified.push({
      type: 'hadith',
      index: i,
      content: formatHadithForReranking(h),
      originalScore: h.semanticScore || h.score
    });
  });

  if (unified.length < 3) {
    return fallbackRefineResult(books, ayahs, hadiths, limits);
  }

  const TIMEOUT_MS = UNIFIED_RERANK_TIMEOUT_MS;

  try {
    const docsText = unified
      .map((d, i) => `[${i + 1}] ${d.content.slice(0, UNIFIED_RERANKER_TEXT_LIMIT)}`)
      .join("\n\n");

    const prompt = `You are ranking a MIXED set of Arabic/Islamic documents for a search query.
The set contains [BOOK] excerpts, [QURAN] verses, and [HADITH] narrations.

Query: "${query}"

Documents:
${docsText}

RANKING PRIORITY:
1. SPECIFIC SOURCE LOOKUP: The ACTUAL source should rank HIGHEST
2. QUESTION: Documents that directly ANSWER rank highest
3. TOPIC SEARCH: Primary sources directly about the topic rank highest

FILTERING: Only include documents that actually address the query topic.

Return ONLY a JSON array of document numbers by relevance (best first).
If no documents are relevant, return an empty array []:
[3, 1, 5, 2, ...]`;

    const config = RERANKER_CONFIG[reranker];
    const model = config?.model ?? "google/gemini-3-flash-preview";

    const result = await callOpenRouter({
      model,
      messages: [{ role: "user", content: prompt }],
      temperature: 0,
      timeoutMs: TIMEOUT_MS,
    });

    if (!result) {
      return fallbackRefineResult(books, ayahs, hadiths, limits);
    }

    const content = result.content;

    const match = content.match(/\[[\d,\s]*\]/);
    if (!match) {
      console.warn("[Unified Refine Rerank] Invalid format, keeping original order");
      return fallbackRefineResult(books, ayahs, hadiths, limits);
    }

    const ranking: number[] = JSON.parse(match[0]);

    const rerankedBooks: RankedResult[] = [];
    const rerankedAyahs: AyahRankedResult[] = [];
    const rerankedHadiths: HadithRankedResult[] = [];

    for (const docNum of ranking) {
      const idx = docNum - 1;
      if (idx < 0 || idx >= unified.length) continue;

      const doc = unified[idx];
      const rank = rerankedBooks.length + rerankedAyahs.length + rerankedHadiths.length + 1;

      if (doc.type === 'book' && rerankedBooks.length < limits.books) {
        const book = books[doc.index];
        rerankedBooks.push({ ...book, semanticScore: 1 - (rank / 100) });
      } else if (doc.type === 'ayah' && rerankedAyahs.length < limits.ayahs) {
        const ayah = ayahs[doc.index];
        rerankedAyahs.push({ ...ayah, rank, score: 1 - (rank / 100) });
      } else if (doc.type === 'hadith' && rerankedHadiths.length < limits.hadiths) {
        const hadith = hadiths[doc.index];
        rerankedHadiths.push({ ...hadith, rank, score: 1 - (rank / 100) });
      }
    }

    return { books: rerankedBooks, ayahs: rerankedAyahs, hadiths: rerankedHadiths, timedOut: false };

  } catch (err) {
    const timedOut = err instanceof Error && err.name === 'AbortError';
    if (timedOut) {
      console.warn(`[Unified Refine Rerank] Timed out after ${TIMEOUT_MS}ms, using RRF order`);
    } else {
      console.warn("[Unified Refine Rerank] Error, keeping original order:", err);
    }
    return fallbackRefineResult(books, ayahs, hadiths, limits, timedOut);
  }
}
