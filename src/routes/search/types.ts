export type RerankerType = "gpt-oss-20b" | "gpt-oss-120b" | "gemini-flash" | "jina" | "none";

export type EmbeddingModel = "gemini" | "jina";

export type SearchMode = "hybrid" | "semantic" | "keyword";

export type SearchStrategy = 'semantic_only' | 'hybrid';

export interface SearchResult {
  score: number;
  semanticScore?: number;
  rank?: number;
  bookId: string;
  pageNumber: number;
  volumeNumber: number;
  textSnippet: string;
  highlightedSnippet: string;
  matchType: "semantic" | "keyword" | "both";
  urlPageIndex?: string;
  referenceUrl: string;
  contentTranslation?: string | null;
  book: {
    id: string;
    titleArabic: string;
    titleLatin: string | null;
    titleTranslated?: string | null;
    filename: string;
    author: {
      nameArabic: string;
      nameLatin: string | null;
    };
  } | null;
}

export interface AyahResult {
  score: number;
  semanticScore?: number;
  rank?: number;
  surahNumber: number;
  ayahNumber: number;
  ayahEnd?: number;
  ayahNumbers?: number[];
  surahNameArabic: string;
  surahNameEnglish: string;
  text: string;
  translation?: string;
  juzNumber: number;
  pageNumber: number;
  quranComUrl: string;
  isChunk?: boolean;
  wordCount?: number;
}

export interface HadithResult {
  score: number;
  semanticScore?: number;
  rank?: number;
  bookId: number;
  collectionSlug: string;
  collectionNameArabic: string;
  collectionNameEnglish: string;
  bookNumber: number;
  bookNameArabic: string;
  bookNameEnglish: string;
  hadithNumber: string;
  text: string;
  chapterArabic: string | null;
  chapterEnglish: string | null;
  sunnahComUrl: string;
  translation?: string;
}

export interface AuthorResult {
  id: string;
  nameArabic: string;
  nameLatin: string | null;
  deathDateHijri: string | null;
  deathDateGregorian: string | null;
  booksCount: number;
}

export interface RankedResult {
  bookId: string;
  pageNumber: number;
  volumeNumber: number;
  textSnippet: string;
  highlightedSnippet: string;
  semanticRank?: number;
  keywordRank?: number;
  semanticScore?: number;
  keywordScore?: number;
  tsRank?: number;
  bm25Score?: number;
  fusedScore?: number;
  urlPageIndex?: string;
  referenceUrl?: string;
  contentTranslation?: string | null;
}

export interface HadithRankedResult extends HadithResult {
  semanticRank?: number;
  keywordRank?: number;
  tsRank?: number;
  bm25Score?: number;
}

export interface AyahRankedResult extends AyahResult {
  semanticRank?: number;
  keywordRank?: number;
  tsRank?: number;
  bm25Score?: number;
}

export interface AyahSearchMeta {
  collection: string;
  usedFallback: boolean;
  embeddingTechnique?: string;
}

export interface AyahSemanticSearchResult {
  results: AyahRankedResult[];
  meta: AyahSearchMeta;
}

export interface TopResultBreakdown {
  rank: number;
  type: 'book' | 'quran' | 'hadith';
  title: string;
  keywordScore: number | null;
  semanticScore: number | null;
  finalScore: number;
  matchType: 'semantic' | 'keyword' | 'both';
}

export interface ExpandedQueryStats {
  query: string;
  weight: number;
  docsRetrieved: number;
  books: number;
  ayahs: number;
  hadiths: number;
  searchTimeMs: number;
}

export interface DatabaseStats {
  totalBooks: number;
  totalPages: number;
  totalHadiths: number;
  totalAyahs: number;
}

export interface SearchDebugStats {
  databaseStats: DatabaseStats;
  searchParams: {
    mode: string;
    cutoff: number;
    totalAboveCutoff: number;
    totalShown: number;
  };
  algorithm: {
    fusionMethod: string;
    fusionWeights: { semantic: number; keyword: number };
    keywordEngine: string;
    bm25Params: { k1: number; b: number; normK: number };
    rrfK: number;
    embeddingModel: string;
    embeddingDimensions: number;
    rerankerModel: string | null;
    queryExpansionModel: string | null;
    quranCollection: string;
    quranCollectionFallback: boolean;
    embeddingTechnique?: string;
  };
  topResultsBreakdown: TopResultBreakdown[];
  refineStats?: {
    expandedQueries: ExpandedQueryStats[];
    originalQueryDocs: number;
    timing: {
      queryExpansion: number;
      parallelSearches: number;
      merge: number;
      rerank: number;
      total: number;
    };
    candidates: {
      totalBeforeMerge: number;
      afterMerge: { books: number; ayahs: number; hadiths: number };
      sentToReranker: number;
    };
    queryExpansionCached: boolean;
  };
  rerankerTimedOut?: boolean;
  timing?: {
    total: number;
    embedding: number;
    semantic: { books: number; ayahs: number; hadiths: number };
    keyword: { books: number; ayahs: number; hadiths: number };
    merge: number;
    authorSearch: number;
    rerank?: number;
    translations: number;
    bookMetadata: number;
  };
}

export interface ParsedQuery {
  phrases: string[];
  terms: string[];
}

export interface ExpandedQuery {
  query: string;
  weight: number;
  reason: string;
}

export interface UnifiedRefineResult {
  type: 'book' | 'ayah' | 'hadith';
  index: number;
  content: string;
  originalScore: number;
}
