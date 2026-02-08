import {
  qdrant,
  QDRANT_COLLECTION,
  QDRANT_AUTHORS_COLLECTION,
  QDRANT_QURAN_COLLECTION,
  QDRANT_HADITH_COLLECTION,
} from "../../qdrant";
import {
  generateEmbedding,
  normalizeArabicText,
  type EmbeddingModel,
} from "../../embeddings";
import { prisma } from "../../db";
import { normalizeBM25Score } from "../../search/bm25";
import {
  keywordSearchAyahsES,
  keywordSearchHadithsES,
} from "../../search/elasticsearch-search";
import { generateShamelaPageUrl, generateQuranComUrl } from "../../utils/source-urls";
import { MIN_CHARS_FOR_SEMANTIC, EXCLUDED_BOOK_IDS } from "./config";
import { hasQuotedPhrases, getDynamicSimilarityThreshold } from "./query-utils";
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
 * Perform semantic search for books using Qdrant
 */
export async function semanticSearch(
  query: string,
  limit: number,
  bookId: string | null,
  similarityCutoff: number = 0.25,
  precomputedEmbedding?: number[],
  collection: string = QDRANT_COLLECTION,
  embeddingModel: EmbeddingModel = "gemini"
): Promise<RankedResult[]> {
  if (hasQuotedPhrases(query)) {
    return [];
  }

  const normalizedQuery = normalizeArabicText(query);

  if (normalizedQuery.replace(/\s/g, '').length < MIN_CHARS_FOR_SEMANTIC) {
    return [];
  }

  const effectiveCutoff = getDynamicSimilarityThreshold(query, similarityCutoff);
  const queryEmbedding = precomputedEmbedding ?? await generateEmbedding(normalizedQuery, embeddingModel);

  const filter = bookId
    ? { must: [{ key: "bookId", match: { value: bookId } }] }
    : undefined;

  const searchResults = await qdrant.search(collection, {
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
        shamelaUrl: generateShamelaPageUrl(payload.bookId, payload.pageNumber),
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
      score_threshold: 0.3,
    });

    if (searchResults.length > 0) {
      return searchResults.map((result) => {
        const payload = result.payload as {
          authorId: string;
          nameArabic: string;
          nameLatin: string;
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
  similarityCutoff: number = 0.28,
  precomputedEmbedding?: number[],
  quranCollectionOverride?: string,
  embeddingModel: EmbeddingModel = "gemini"
): Promise<AyahSemanticSearchResult> {
  const collection = quranCollectionOverride || QDRANT_QURAN_COLLECTION;

  const defaultMeta: AyahSearchMeta = {
    collection,
    usedFallback: false,
    embeddingTechnique: !quranCollectionOverride ? "metadata-translation" : undefined,
  };

  try {
    if (hasQuotedPhrases(query)) {
      return { results: [], meta: defaultMeta };
    }

    const normalizedQuery = normalizeArabicText(query);

    if (normalizedQuery.replace(/\s/g, '').length < MIN_CHARS_FOR_SEMANTIC) {
      return { results: [], meta: defaultMeta };
    }

    const effectiveCutoff = getDynamicSimilarityThreshold(query, similarityCutoff);
    const queryEmbedding = precomputedEmbedding ?? await generateEmbedding(normalizedQuery, embeddingModel);

    const searchResults = await qdrant.search(collection, {
      vector: queryEmbedding,
      limit: limit,
      with_payload: true,
      score_threshold: effectiveCutoff,
    });

    const meta: AyahSearchMeta = {
      collection,
      usedFallback: false,
      embeddingTechnique: collection === QDRANT_QURAN_COLLECTION ? "metadata-translation" : undefined,
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
        quranComUrl: generateQuranComUrl(payload.surahNumber, payload.ayahNumber),
        semanticRank: index + 1,
      };
    });

    return { results, meta };
  } catch (err) {
    console.error("[searchAyahsSemantic] ERROR:", err);
    return { results: [], meta: { collection: QDRANT_QURAN_COLLECTION, usedFallback: true } };
  }
}

/**
 * Hybrid search for Quran ayahs using RRF fusion + reranking
 */
export async function searchAyahsHybrid(
  query: string,
  limit: number = 10,
  options: { reranker?: RerankerType; preRerankLimit?: number; postRerankLimit?: number; similarityCutoff?: number; fuzzyFallback?: boolean; precomputedEmbedding?: number[]; quranCollection?: string; embeddingModel?: EmbeddingModel } = {}
): Promise<AyahResult[]> {
  const { reranker = "none", preRerankLimit = 60, postRerankLimit = limit, similarityCutoff = 0.6, fuzzyFallback = true, precomputedEmbedding, quranCollection, embeddingModel = "gemini" } = options;

  const fetchLimit = Math.min(preRerankLimit, 100);
  const collectionToUse = quranCollection || QDRANT_QURAN_COLLECTION;
  const defaultMeta: AyahSearchMeta = { collection: collectionToUse, usedFallback: false, embeddingTechnique: "metadata-translation" };

  const [semanticSearchResult, keywordResults] = await Promise.all([
    searchAyahsSemantic(query, fetchLimit, similarityCutoff, precomputedEmbedding, quranCollection, embeddingModel).catch(() => ({ results: [] as AyahRankedResult[], meta: defaultMeta })),
    keywordSearchAyahsES(query, fetchLimit, { fuzzyFallback }).catch(() => []),
  ]);

  const merged = mergeWithRRFGeneric(
    semanticSearchResult.results,
    keywordResults,
    (a) => `${a.surahNumber}-${a.ayahNumber}`,
    query
  );

  const candidates = merged.slice(0, Math.min(preRerankLimit, 60));
  const finalLimit = Math.min(postRerankLimit, limit);

  const { results: finalResults } = await rerank(
    query,
    candidates,
    (a) => formatAyahForReranking(a),
    finalLimit,
    reranker
  );

  return finalResults.map((result, index) => {
    const r = result as typeof result & { fusedScore?: number; semanticScore?: number };
    return {
      ...result,
      score: r.fusedScore ?? r.semanticScore ?? result.rrfScore,
      fusedScore: r.fusedScore,
      semanticScore: r.semanticScore,
      rank: index + 1,
    };
  }) as AyahResult[];
}

/**
 * Search for Hadiths using semantic search
 */
export async function searchHadithsSemantic(
  query: string,
  limit: number = 10,
  similarityCutoff: number = 0.25,
  precomputedEmbedding?: number[],
  hadithCollectionOverride?: string,
  embeddingModel: EmbeddingModel = "gemini"
): Promise<HadithRankedResult[]> {
  try {
    if (hasQuotedPhrases(query)) {
      return [];
    }

    const normalizedQuery = normalizeArabicText(query);

    if (normalizedQuery.replace(/\s/g, '').length < MIN_CHARS_FOR_SEMANTIC) {
      return [];
    }

    const effectiveCutoff = getDynamicSimilarityThreshold(query, similarityCutoff);
    const queryEmbedding = precomputedEmbedding ?? await generateEmbedding(normalizedQuery, embeddingModel);

    const hadithCollection = hadithCollectionOverride || QDRANT_HADITH_COLLECTION;
    const searchResults = await qdrant.search(hadithCollection, {
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
      sunnahComUrl: string;
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
        sunnahComUrl: payload.sunnahComUrl.replace(/(\d)[A-Z]+$/, '$1'),
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
  options: { reranker?: RerankerType; preRerankLimit?: number; postRerankLimit?: number; similarityCutoff?: number; fuzzyFallback?: boolean; precomputedEmbedding?: number[]; hadithCollection?: string; embeddingModel?: EmbeddingModel } = {}
): Promise<HadithResult[]> {
  const { reranker = "none", preRerankLimit = 60, postRerankLimit = limit, similarityCutoff = 0.6, fuzzyFallback = true, precomputedEmbedding, hadithCollection, embeddingModel = "gemini" } = options;

  const fetchLimit = Math.min(preRerankLimit, 100);

  const [semanticResults, keywordResults] = await Promise.all([
    searchHadithsSemantic(query, fetchLimit, similarityCutoff, precomputedEmbedding, hadithCollection, embeddingModel).catch(() => []),
    keywordSearchHadithsES(query, fetchLimit, { fuzzyFallback }).catch(() => []),
  ]);

  const merged = mergeWithRRFGeneric(
    semanticResults,
    keywordResults,
    (h) => `${h.collectionSlug}-${h.hadithNumber}`,
    query
  );

  const candidates = merged.slice(0, Math.min(preRerankLimit, 75));
  const finalLimit = Math.min(postRerankLimit, limit);

  const { results: finalResults } = await rerank(
    query,
    candidates,
    (h) => formatHadithForReranking(h),
    finalLimit,
    reranker
  );

  return finalResults.map((result, index) => {
    const r = result as typeof result & { fusedScore?: number; semanticScore?: number };
    return {
      ...result,
      score: r.fusedScore ?? r.semanticScore ?? result.rrfScore,
      fusedScore: r.fusedScore,
      semanticScore: r.semanticScore,
      rank: index + 1,
    };
  });
}
