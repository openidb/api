import type { Context } from "hono";
import type { RerankerType, SearchMode } from "./types";
import type { EmbeddingModel } from "../../embeddings";
import { getCollections } from "../../qdrant";
import {
  DEFAULT_SEARCH_LIMIT, MAX_SEARCH_LIMIT, MAX_QUERY_LENGTH,
  DEFAULT_SIMILARITY_CUTOFF, REFINE_SIMILARITY_CUTOFF,
  DEFAULT_BOOK_LIMIT, MAX_BOOK_LIMIT, MIN_BOOK_LIMIT,
} from "./config";

export interface SearchParams {
  query: string;
  limit: number;
  bookId: string | null;
  mode: SearchMode;
  includeQuran: boolean;
  includeHadith: boolean;
  includeBooks: boolean;
  reranker: RerankerType;
  similarityCutoff: number;
  bookLimit: number;
  fuzzyEnabled: boolean;
  quranTranslation: string;
  hadithTranslation: string;
  bookTitleLang: string | undefined;
  bookContentTranslation: string;
  refine: boolean;
  refineSimilarityCutoff: number;
  refineOriginalWeight: number;
  refineExpandedWeight: number;
  refineBookPerQuery: number;
  refineAyahPerQuery: number;
  refineHadithPerQuery: number;
  refineBookRerank: number;
  refineAyahRerank: number;
  refineHadithRerank: number;
  queryExpansionModel: string;
  includeGraph: boolean;
  embeddingModel: EmbeddingModel;
  pageCollection: string;
  quranCollection: string;
  hadithCollection: string;
  embeddingDimensions: number;
}

export function parseSearchParams(c: Context): SearchParams | { error: string; status: number } {
  const query = c.req.query("q");
  if (!query || query.trim().length === 0) {
    return { error: "Query parameter 'q' is required", status: 400 };
  }
  if (query.length > MAX_QUERY_LENGTH) {
    return { error: `Query too long (max ${MAX_QUERY_LENGTH} characters)`, status: 400 };
  }

  const modeParam = c.req.query("mode") as SearchMode | undefined;
  const mode: SearchMode = modeParam || "hybrid";
  if (!["hybrid", "semantic", "keyword"].includes(mode)) {
    return { error: "Invalid mode. Must be 'hybrid', 'semantic', or 'keyword'", status: 400 };
  }

  const rerankerParam = c.req.query("reranker") as RerankerType | undefined;
  const reranker: RerankerType = rerankerParam && ["gpt-oss-20b", "gpt-oss-120b", "gemini-flash", "none"].includes(rerankerParam)
    ? rerankerParam : "none";

  const refine = c.req.query("refine") === "true";
  const embeddingModelParam = c.req.query("embeddingModel") as EmbeddingModel | undefined;
  const embeddingModel: EmbeddingModel = embeddingModelParam === "bge-m3" ? "bge-m3" : "gemini";
  const { pages: pageCollection, quran: quranCollection, hadith: hadithCollection, dimensions: embeddingDimensions } = getCollections(embeddingModel);

  return {
    query,
    limit: Math.min(Math.max(parseInt(c.req.query("limit") || String(DEFAULT_SEARCH_LIMIT), 10), 1), MAX_SEARCH_LIMIT),
    bookId: c.req.query("bookId") || null,
    mode,
    includeQuran: c.req.query("includeQuran") !== "false",
    includeHadith: c.req.query("includeHadith") !== "false",
    includeBooks: c.req.query("includeBooks") !== "false",
    reranker,
    similarityCutoff: parseFloat(c.req.query("similarityCutoff") || String(DEFAULT_SIMILARITY_CUTOFF)),
    bookLimit: Math.min(Math.max(parseInt(c.req.query("bookLimit") || String(DEFAULT_BOOK_LIMIT), 10), MIN_BOOK_LIMIT), MAX_BOOK_LIMIT),
    fuzzyEnabled: c.req.query("fuzzy") !== "false",
    quranTranslation: c.req.query("quranTranslation") || "none",
    hadithTranslation: c.req.query("hadithTranslation") || "none",
    bookTitleLang: c.req.query("bookTitleLang"),
    bookContentTranslation: c.req.query("bookContentTranslation") || "none",
    refine,
    refineSimilarityCutoff: refine ? parseFloat(c.req.query("refineSimilarityCutoff") || String(REFINE_SIMILARITY_CUTOFF)) : REFINE_SIMILARITY_CUTOFF,
    refineOriginalWeight: Math.min(Math.max(parseFloat(c.req.query("refineOriginalWeight") || "1.0"), 0.5), 1.0),
    refineExpandedWeight: Math.min(Math.max(parseFloat(c.req.query("refineExpandedWeight") || "0.7"), 0.3), 1.0),
    refineBookPerQuery: Math.min(Math.max(parseInt(c.req.query("refineBookPerQuery") || "30", 10), 10), 60),
    refineAyahPerQuery: Math.min(Math.max(parseInt(c.req.query("refineAyahPerQuery") || "30", 10), 10), 60),
    refineHadithPerQuery: Math.min(Math.max(parseInt(c.req.query("refineHadithPerQuery") || "30", 10), 10), 60),
    refineBookRerank: Math.min(Math.max(parseInt(c.req.query("refineBookRerank") || "20", 10), 5), 40),
    refineAyahRerank: Math.min(Math.max(parseInt(c.req.query("refineAyahRerank") || "12", 10), 5), 25),
    refineHadithRerank: Math.min(Math.max(parseInt(c.req.query("refineHadithRerank") || "15", 10), 5), 25),
    queryExpansionModel: c.req.query("queryExpansionModel") || "gemini-flash",
    includeGraph: c.req.query("includeGraph") !== "false",
    embeddingModel,
    pageCollection,
    quranCollection,
    hadithCollection,
    embeddingDimensions,
  };
}
