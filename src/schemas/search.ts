import { z } from "@hono/zod-openapi";
import {
  DEFAULT_SEARCH_LIMIT, MAX_SEARCH_LIMIT, MAX_QUERY_LENGTH,
  DEFAULT_SIMILARITY_CUTOFF, REFINE_SIMILARITY_CUTOFF,
  DEFAULT_BOOK_LIMIT, MAX_BOOK_LIMIT, MIN_BOOK_LIMIT,
} from "../routes/search/config";
import { SourceSchema } from "./common";

export const SearchQuery = z.object({
  q: z.string().min(1).max(MAX_QUERY_LENGTH).openapi({ example: "الصلاة", description: "Search query" }),
  limit: z.coerce.number().int().min(1).max(MAX_SEARCH_LIMIT).default(DEFAULT_SEARCH_LIMIT).openapi({ example: 20 }),
  bookId: z.string().optional().openapi({ description: "Filter to a specific book" }),
  mode: z.enum(["hybrid", "semantic", "keyword"]).default("hybrid").openapi({ example: "hybrid" }),
  includeQuran: z.enum(["true", "false"]).default("true"),
  includeHadith: z.enum(["true", "false"]).default("true"),
  includeBooks: z.enum(["true", "false"]).default("false"),
  reranker: z.enum(["gpt-oss-20b", "gpt-oss-120b", "gemini-flash", "jina", "none"]).default("none"),
  embeddingModel: z.enum(["gemini", "jina"]).default("gemini"),
  similarityCutoff: z.coerce.number().min(0).max(1).default(DEFAULT_SIMILARITY_CUTOFF),
  bookLimit: z.coerce.number().int().min(MIN_BOOK_LIMIT).max(MAX_BOOK_LIMIT).default(DEFAULT_BOOK_LIMIT),
  fuzzy: z.enum(["true", "false"]).default("true"),
  quranTranslation: z.string().default("none"),
  hadithTranslation: z.string().default("none"),
  bookTitleLang: z.string().optional(),
  bookContentTranslation: z.string().default("none"),
  refine: z.enum(["true", "false"]).default("false"),
  refineSimilarityCutoff: z.coerce.number().min(0).max(1).default(REFINE_SIMILARITY_CUTOFF),
  refineOriginalWeight: z.coerce.number().min(0.5).max(1.0).default(1.0),
  refineExpandedWeight: z.coerce.number().min(0.3).max(1.0).default(1.0),
  refineBookPerQuery: z.coerce.number().int().min(10).max(60).default(30),
  refineAyahPerQuery: z.coerce.number().int().min(10).max(60).default(30),
  refineHadithPerQuery: z.coerce.number().int().min(10).max(60).default(30),
  refineBookRerank: z.coerce.number().int().min(5).max(40).default(20),
  refineAyahRerank: z.coerce.number().int().min(5).max(25).default(12),
  refineHadithRerank: z.coerce.number().int().min(5).max(25).default(15),
  queryExpansionModel: z.string().default("gemini-flash"),
  includeGraph: z.enum(["true", "false"]).default("false"),
});

export const SearchResponse = z.object({
  query: z.string(),
  mode: z.string(),
  count: z.number(),
  results: z.array(z.any()),
  authors: z.array(z.any()),
  ayahs: z.array(z.any()),
  hadiths: z.array(z.any()),
  debugStats: z.any().optional(),
  graphContext: z.any().optional(),
  refined: z.boolean().optional(),
  expandedQueries: z.array(z.any()).optional(),
  _sources: z.array(SourceSchema).optional(),
}).openapi("SearchResponse");
