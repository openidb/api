import type {
  RerankerType,
  RankedResult,
  AyahRankedResult,
  HadithRankedResult,
  UnifiedRefineResult,
} from "./types";

const RERANKER_MODELS: Record<string, string> = {
  "gpt-oss-20b": "openai/gpt-oss-20b",
  "gpt-oss-120b": "openai/gpt-oss-120b",
  "gemini-flash": "google/gemini-3-flash-preview",
};

async function fetchWithTimeout(
  url: string,
  options: RequestInit,
  timeoutMs: number = 15000
): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timeoutId);
  }
}

export function formatAyahForReranking(ayah: AyahRankedResult): string {
  const range = ayah.ayahEnd ? `${ayah.ayahNumber}-${ayah.ayahEnd}` : String(ayah.ayahNumber);
  return `[QURAN] ${ayah.surahNameArabic} (${ayah.surahNameEnglish}), Ayah ${range}
${ayah.text.slice(0, 800)}`;
}

export function formatHadithForReranking(hadith: HadithRankedResult): string {
  const chapter = hadith.chapterArabic ? ` - ${hadith.chapterArabic}` : '';
  return `[HADITH] ${hadith.collectionNameArabic} (${hadith.collectionNameEnglish}), ${hadith.bookNameArabic}${chapter}
${hadith.text.slice(0, 800)}`;
}

export function formatBookForReranking(result: RankedResult, bookTitle?: string, authorName?: string): string {
  const meta = bookTitle ? `[BOOK] ${bookTitle}${authorName ? ` - ${authorName}` : ''}, p.${result.pageNumber}` : `[BOOK] Page ${result.pageNumber}`;
  return `${meta}
${result.textSnippet.slice(0, 800)}`;
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
  if (results.length === 0 || !process.env.OPENROUTER_API_KEY) {
    return { results: results.slice(0, topN), timedOut: false };
  }

  try {
    const docsText = results
      .map((d, i) => `[${i + 1}] ${getText(d).slice(0, 800)}`)
      .join("\n\n");

    const prompt = buildRerankerPrompt(query, docsText);

    const response = await fetchWithTimeout("https://openrouter.ai/api/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${process.env.OPENROUTER_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model,
        messages: [{ role: "user", content: prompt }],
        temperature: 0,
      }),
    }, timeoutMs);

    if (!response.ok) {
      throw new Error(`LLM reranking failed: ${response.statusText}`);
    }

    const data = await response.json();
    const content = data.choices?.[0]?.message?.content || "[]";

    const reranked = parseLLMRanking(content, results, topN);
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
  if (results.length === 0 || reranker === "none") {
    return { results: results.slice(0, topN), timedOut: false };
  }

  switch (reranker) {
    case "gpt-oss-20b":
      return rerankWithLLM(query, results, getText, topN, "openai/gpt-oss-20b", 20000);
    case "gpt-oss-120b":
      return rerankWithLLM(query, results, getText, topN, "openai/gpt-oss-120b", 20000);
    case "gemini-flash":
      return rerankWithLLM(query, results, getText, topN, "google/gemini-3-flash-preview", 15000);
    default:
      return { results: results.slice(0, topN), timedOut: false };
  }
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
    return {
      books: books.slice(0, limits.books),
      ayahs: ayahs.slice(0, limits.ayahs),
      hadiths: hadiths.slice(0, limits.hadiths),
      timedOut: false
    };
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
    return {
      books: books.slice(0, limits.books),
      ayahs: ayahs.slice(0, limits.ayahs),
      hadiths: hadiths.slice(0, limits.hadiths),
      timedOut: false
    };
  }

  const TIMEOUT_MS = 25000;

  try {
    const docsText = unified
      .map((d, i) => `[${i + 1}] ${d.content.slice(0, 600)}`)
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

    const model = RERANKER_MODELS[reranker] ?? "google/gemini-3-flash-preview";

    const response = await fetchWithTimeout("https://openrouter.ai/api/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${process.env.OPENROUTER_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model,
        messages: [{ role: "user", content: prompt }],
        temperature: 0,
      }),
    }, TIMEOUT_MS);

    if (!response.ok) {
      console.warn(`[Unified Refine Rerank] API error: ${response.statusText}`);
      return {
        books: books.slice(0, limits.books),
        ayahs: ayahs.slice(0, limits.ayahs),
        hadiths: hadiths.slice(0, limits.hadiths),
        timedOut: false
      };
    }

    const data = await response.json();
    const content = data.choices?.[0]?.message?.content || "[]";

    const match = content.match(/\[[\d,\s]*\]/);
    if (!match) {
      console.warn("[Unified Refine Rerank] Invalid format, keeping original order");
      return {
        books: books.slice(0, limits.books),
        ayahs: ayahs.slice(0, limits.ayahs),
        hadiths: hadiths.slice(0, limits.hadiths),
        timedOut: false
      };
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
    return {
      books: books.slice(0, limits.books),
      ayahs: ayahs.slice(0, limits.ayahs),
      hadiths: hadiths.slice(0, limits.hadiths),
      timedOut
    };
  }
}
