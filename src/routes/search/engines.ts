import {
  qdrant,
  QDRANT_COLLECTION,
  QDRANT_AUTHORS_COLLECTION,
  QDRANT_QURAN_COLLECTION,
  QDRANT_HADITH_COLLECTION,
  QDRANT_QURAN_JINA_COLLECTION,
  QDRANT_HADITH_JINA_COLLECTION,
  QDRANT_PAGES_JINA_COLLECTION,
} from "../../qdrant";
import {
  generateEmbedding,
  generateJinaEmbedding,
  normalizeArabicText,
} from "../../embeddings";
import type { EmbeddingModel } from "./types";
import { prisma } from "../../db";
import { normalizeBM25Score } from "../../search/bm25";
import {
  keywordSearchAyahsES,
  keywordSearchHadithsES,
} from "../../search/elasticsearch-search";
import { generatePageReferenceUrl, generateQuranUrl } from "../../utils/source-urls";
import { EXCLUDED_BOOK_IDS, AUTHOR_SCORE_THRESHOLD, DEFAULT_AYAH_SIMILARITY_CUTOFF, FETCH_LIMIT_CAP, AYAH_PRE_RERANK_CAP, HADITH_PRE_RERANK_CAP } from "./config";
import { shouldSkipSemanticSearch, getDynamicSimilarityThreshold } from "./query-utils";
import { mergeWithRRFGeneric } from "./fusion";
import { rerank } from "./rerankers";
import { formatAyahForReranking, formatHadithForReranking } from "./rerankers";
import type {
  RerankerType,
  RankedResult,
  AuthorResult,
  AyahResult,
  AyahRankedResult,
  AyahSearchMeta,
  AyahSemanticSearchResult,
  HadithResult,
  HadithRankedResult,
} from "./types";

/**
 * Get embedding function and collection names based on embedding model
 */
function getEmbeddingConfig(model: EmbeddingModel = "gemini") {
  if (model === "jina") {
    return {
      generateEmbeddingFn: (text: string) => generateJinaEmbedding(text, "retrieval.query"),
      pagesCollection: QDRANT_PAGES_JINA_COLLECTION,
      quranCollection: QDRANT_QURAN_JINA_COLLECTION,
      hadithCollection: QDRANT_HADITH_JINA_COLLECTION,
    };
  }
  return {
    generateEmbeddingFn: generateEmbedding,
    pagesCollection: QDRANT_COLLECTION,
    quranCollection: QDRANT_QURAN_COLLECTION,
    hadithCollection: QDRANT_HADITH_COLLECTION,
  };
}

/**
 * Perform semantic search for books using Qdrant
 */
export async function semanticSearch(
  query: string,
  limit: number,
  bookId: string | null,
  similarityCutoff: number = 0.25,
  precomputedEmbedding?: number[],
  embeddingModel: EmbeddingModel = "gemini",
): Promise<RankedResult[]> {
  if (shouldSkipSemanticSearch(query)) {
    return [];
  }

  const config = getEmbeddingConfig(embeddingModel);
  const normalizedQuery = normalizeArabicText(query);
  const effectiveCutoff = getDynamicSimilarityThreshold(query, similarityCutoff);
  const queryEmbedding = precomputedEmbedding ?? await config.generateEmbeddingFn(normalizedQuery);

  const filter = bookId
    ? { must: [{ key: "bookId", match: { value: bookId } }] }
    : undefined;

  const searchResults = await qdrant.search(config.pagesCollection, {
    vector: queryEmbedding,
    limit: limit,
    filter: filter,
    with_payload: {
      include: ["bookId", "pageNumber", "volumeNumber", "textSnippet"],
    },
    score_threshold: effectiveCutoff,
  });

  return searchResults
    .map((result) => {
      const payload = result.payload as {
        bookId: string;
        pageNumber: number;
        volumeNumber: number;
        textSnippet: string;
      };

      return {
        bookId: payload.bookId,
        pageNumber: payload.pageNumber,
        volumeNumber: payload.volumeNumber,
        textSnippet: payload.textSnippet,
        highlightedSnippet: payload.textSnippet,
        semanticScore: result.score,
        referenceUrl: generatePageReferenceUrl(payload.bookId, payload.pageNumber),
      };
    })
    .filter(r => !EXCLUDED_BOOK_IDS.has(r.bookId))
    .map((r, index) => ({ ...r, semanticRank: index + 1 }));
}

/**
 * Search for authors using semantic search (Qdrant) with keyword fallback
 */
export async function searchAuthors(query: string, limit: number = 5): Promise<AuthorResult[]> {
  try {
    const normalizedQuery = normalizeArabicText(query);
    const queryEmbedding = await generateEmbedding(normalizedQuery);

    const searchResults = await qdrant.search(QDRANT_AUTHORS_COLLECTION, {
      vector: queryEmbedding,
      limit: limit,
      with_payload: {
        include: ["authorId", "nameArabic", "nameLatin", "deathDateHijri", "deathDateGregorian", "booksCount"],
      },
      score_threshold: AUTHOR_SCORE_THRESHOLD,
    });

    if (searchResults.length > 0) {
      return searchResults.map((result) => {
        const payload = result.payload as {
          authorId: string;
          nameArabic: string;
          nameLatin: string | null;
          deathDateHijri: string | null;
          deathDateGregorian: string | null;
          booksCount: number;
        };

        return {
          id: payload.authorId,
          nameArabic: payload.nameArabic,
          nameLatin: payload.nameLatin,
          deathDateHijri: payload.deathDateHijri,
          deathDateGregorian: payload.deathDateGregorian,
          booksCount: payload.booksCount,
        };
      });
    }
  } catch (err) {
    console.warn("Semantic author search failed, falling back to keyword:", err);
  }

  const authors = await prisma.author.findMany({
    where: {
      OR: [
        { nameArabic: { contains: query, mode: "insensitive" } },
        { nameLatin: { contains: query, mode: "insensitive" } },
      ],
    },
    select: {
      id: true,
      nameArabic: true,
      nameLatin: true,
      deathDateHijri: true,
      deathDateGregorian: true,
      _count: {
        select: { books: true },
      },
    },
    take: limit,
    orderBy: {
      books: { _count: "desc" },
    },
  });

  return authors.map((author) => ({
    id: author.id,
    nameArabic: author.nameArabic,
    nameLatin: author.nameLatin,
    deathDateHijri: author.deathDateHijri,
    deathDateGregorian: author.deathDateGregorian,
    booksCount: author._count.books,
  }));
}

/**
 * Search for Quran ayahs using semantic search
 */
export async function searchAyahsSemantic(
  query: string,
  limit: number = 10,
  similarityCutoff: number = DEFAULT_AYAH_SIMILARITY_CUTOFF,
  precomputedEmbedding?: number[],
  embeddingModel: EmbeddingModel = "gemini",
): Promise<AyahSemanticSearchResult> {
  const emConfig = getEmbeddingConfig(embeddingModel);
  const defaultMeta: AyahSearchMeta = {
    collection: emConfig.quranCollection,
    usedFallback: false,
    embeddingTechnique: "metadata-translation",
  };

  try {
    if (shouldSkipSemanticSearch(query)) {
      return { results: [], meta: defaultMeta };
    }

    const normalizedQuery = normalizeArabicText(query);
    const effectiveCutoff = getDynamicSimilarityThreshold(query, similarityCutoff);
    const queryEmbedding = precomputedEmbedding ?? await emConfig.generateEmbeddingFn(normalizedQuery);

    const searchResults = await qdrant.search(emConfig.quranCollection, {
      vector: queryEmbedding,
      limit: limit,
      with_payload: true,
      score_threshold: effectiveCutoff,
    });

    const meta: AyahSearchMeta = {
      collection: emConfig.quranCollection,
      usedFallback: false,
      embeddingTechnique: "metadata-translation",
    };

    const results = searchResults.map((result, index) => {
      const payload = result.payload as {
        surahNumber: number;
        ayahNumber: number;
        surahNameArabic: string;
        surahNameEnglish: string;
        text: string;
        textPlain: string;
        juzNumber: number;
        pageNumber: number;
        embeddedText?: string;
        embeddingModel?: string;
      };

      return {
        score: result.score,
        semanticScore: result.score,
        surahNumber: payload.surahNumber,
        ayahNumber: payload.ayahNumber,
        surahNameArabic: payload.surahNameArabic,
        surahNameEnglish: payload.surahNameEnglish,
        text: payload.text,
        juzNumber: payload.juzNumber,
        pageNumber: payload.pageNumber,
        quranComUrl: generateQuranUrl(payload.surahNumber, payload.ayahNumber),
        semanticRank: index + 1,
      };
    });

    return { results, meta };
  } catch (err) {
    console.error("[searchAyahsSemantic] ERROR:", err);
    return { results: [], meta: { collection: emConfig.quranCollection, usedFallback: true } };
  }
}

/**
 * Generic hybrid search: parallel semantic+keyword -> RRF merge -> slice -> rerank -> map scores
 */
async function hybridSearchWithRerank<T extends { semanticRank?: number; keywordRank?: number; semanticScore?: number; score?: number; tsRank?: number; bm25Score?: number }>(opts: {
  query: string;
  limit: number;
  reranker: RerankerType;
  preRerankLimit: number;
  postRerankLimit: number;
  preRerankCap: number;
  semanticSearch: (fetchLimit: number) => Promise<T[]>;
  keywordSearch: (fetchLimit: number) => Promise<T[]>;
  getKey: (item: T) => string;
  formatForReranking: (item: T) => string;
}) {
  const fetchLimit = Math.min(opts.preRerankLimit, FETCH_LIMIT_CAP);

  const [semanticResults, keywordResults] = await Promise.all([
    opts.semanticSearch(fetchLimit).catch(err => { console.error("[SearchEngine] hybrid semantic:", err.message); return [] as T[]; }),
    opts.keywordSearch(fetchLimit).catch(err => { console.error("[SearchEngine] hybrid keyword:", err.message); return [] as T[]; }),
  ]);

  const merged = mergeWithRRFGeneric(semanticResults, keywordResults, opts.getKey, opts.query);
  const candidates = merged.slice(0, Math.min(opts.preRerankLimit, opts.preRerankCap));
  const finalLimit = Math.min(opts.postRerankLimit, opts.limit);

  const { results: finalResults } = await rerank(
    opts.query, candidates, opts.formatForReranking, finalLimit, opts.reranker
  );

  return finalResults.map((result, index) => ({
    ...result,
    score: result.fusedScore ?? result.semanticScore ?? result.rrfScore,
    rank: index + 1,
  }));
}

/**
 * Hybrid search for Quran ayahs using RRF fusion + reranking
 */
export async function searchAyahsHybrid(
  query: string,
  limit: number = 10,
  options: { reranker?: RerankerType; preRerankLimit?: number; postRerankLimit?: number; similarityCutoff?: number; fuzzyFallback?: boolean; precomputedEmbedding?: number[]; embeddingModel?: EmbeddingModel } = {}
): Promise<AyahResult[]> {
  const { reranker = "none", preRerankLimit = 60, postRerankLimit = limit, similarityCutoff = 0.6, fuzzyFallback = true, precomputedEmbedding, embeddingModel = "gemini" } = options;

  return hybridSearchWithRerank({
    query, limit, reranker, preRerankLimit, postRerankLimit, preRerankCap: AYAH_PRE_RERANK_CAP,
    semanticSearch: (fetchLimit) =>
      searchAyahsSemantic(query, fetchLimit, similarityCutoff, precomputedEmbedding, embeddingModel)
        .then(r => r.results)
        .catch(err => { console.error("[SearchEngine] ayah semantic fallback:", err.message); return [] as AyahRankedResult[]; }),
    keywordSearch: (fetchLimit) => keywordSearchAyahsES(query, fetchLimit, { fuzzyFallback }),
    getKey: (a) => `${a.surahNumber}-${a.ayahNumber}`,
    formatForReranking: (a) => formatAyahForReranking(a),
  }) as Promise<AyahResult[]>;
}

/**
 * Search for Hadiths using semantic search
 */
export async function searchHadithsSemantic(
  query: string,
  limit: number = 10,
  similarityCutoff: number = 0.25,
  precomputedEmbedding?: number[],
  embeddingModel: EmbeddingModel = "gemini",
): Promise<HadithRankedResult[]> {
  try {
    if (shouldSkipSemanticSearch(query)) {
      return [];
    }

    const emConfig = getEmbeddingConfig(embeddingModel);
    const normalizedQuery = normalizeArabicText(query);
    const effectiveCutoff = getDynamicSimilarityThreshold(query, similarityCutoff);
    const queryEmbedding = precomputedEmbedding ?? await emConfig.generateEmbeddingFn(normalizedQuery);

    const searchResults = await qdrant.search(emConfig.hadithCollection, {
      vector: queryEmbedding,
      limit: limit,
      with_payload: true,
      score_threshold: effectiveCutoff,
      filter: {
        must_not: [{ key: "isChainVariation", match: { value: true } }],
      },
    });

    if (searchResults.length === 0) {
      return [];
    }

    const payloads = searchResults.map((result) => result.payload as {
      collectionSlug: string;
      collectionNameArabic: string;
      collectionNameEnglish: string;
      bookNumber: number;
      bookNameArabic: string;
      bookNameEnglish: string;
      hadithNumber: string;
      text: string;
      textPlain: string;
      chapterArabic: string | null;
      chapterEnglish: string | null;
      sunnahUrl?: string;
      sunnahComUrl?: string;
      bookId?: number;
    });

    const bookIdMap = new Map<string, number>();
    const missingBookIds = payloads.filter((p) => !p.bookId);

    if (missingBookIds.length > 0) {
      const uniqueKeys = new Set(missingBookIds.map((p) => `${p.collectionSlug}|${p.bookNumber}`));
      const lookupPairs = Array.from(uniqueKeys).map((key) => {
        const [slug, num] = key.split("|");
        return { slug, bookNumber: parseInt(num, 10) };
      });

      const books = await prisma.hadithBook.findMany({
        where: {
          OR: lookupPairs.map((p) => ({
            collection: { slug: p.slug },
            bookNumber: p.bookNumber,
          })),
        },
        select: {
          id: true,
          bookNumber: true,
          collection: { select: { slug: true } },
        },
      });

      for (const book of books) {
        bookIdMap.set(`${book.collection.slug}|${book.bookNumber}`, book.id);
      }
    }

    return searchResults.map((result, index) => {
      const payload = payloads[index];
      const bookId = payload.bookId || bookIdMap.get(`${payload.collectionSlug}|${payload.bookNumber}`) || 0;

      return {
        score: result.score,
        semanticScore: result.score,
        bookId,
        collectionSlug: payload.collectionSlug,
        collectionNameArabic: payload.collectionNameArabic,
        collectionNameEnglish: payload.collectionNameEnglish,
        bookNumber: payload.bookNumber,
        bookNameArabic: payload.bookNameArabic,
        bookNameEnglish: payload.bookNameEnglish,
        hadithNumber: payload.hadithNumber,
        text: payload.text,
        chapterArabic: payload.chapterArabic,
        chapterEnglish: payload.chapterEnglish,
        sunnahComUrl: (payload.sunnahUrl || payload.sunnahComUrl || '').replace(/(\d)[A-Z]+$/, '$1'),
        semanticRank: index + 1,
      };
    });
  } catch (err) {
    console.warn("Hadith semantic search failed:", err);
    return [];
  }
}

/**
 * Hybrid search for Hadiths using RRF fusion + reranking
 */
export async function searchHadithsHybrid(
  query: string,
  limit: number = 10,
  options: { reranker?: RerankerType; preRerankLimit?: number; postRerankLimit?: number; similarityCutoff?: number; fuzzyFallback?: boolean; precomputedEmbedding?: number[]; embeddingModel?: EmbeddingModel } = {}
): Promise<HadithResult[]> {
  const { reranker = "none", preRerankLimit = 60, postRerankLimit = limit, similarityCutoff = 0.6, fuzzyFallback = true, precomputedEmbedding, embeddingModel = "gemini" } = options;

  return hybridSearchWithRerank({
    query, limit, reranker, preRerankLimit, postRerankLimit, preRerankCap: HADITH_PRE_RERANK_CAP,
    semanticSearch: (fetchLimit) =>
      searchHadithsSemantic(query, fetchLimit, similarityCutoff, precomputedEmbedding, embeddingModel),
    keywordSearch: (fetchLimit) => keywordSearchHadithsES(query, fetchLimit, { fuzzyFallback }),
    getKey: (h) => `${h.collectionSlug}-${h.hadithNumber}`,
    formatForReranking: (h) => formatHadithForReranking(h),
  });
}
