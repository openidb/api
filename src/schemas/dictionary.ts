import { z } from "@hono/zod-openapi";
import { SourceSchema } from "./common";

export const WordParam = z.object({
  word: z.string().min(1).max(100).openapi({ example: "كتاب" }),
});

export const DictionarySourceSchema = z.object({
  id: z.number(),
  slug: z.string(),
  nameArabic: z.string(),
  nameEnglish: z.string(),
  author: z.string().nullable(),
  bookId: z.string().nullable(),
}).openapi("DictionarySource");

export const DefinitionSchema = z.object({
  id: z.number(),
  source: DictionarySourceSchema,
  root: z.string(),
  headword: z.string(),
  definition: z.string(),
  definitionHtml: z.string().nullable(),
  matchType: z.enum(["exact", "root"]),
  precision: z.enum(["sub_entry", "excerpt", "full"]),
  bookId: z.string().nullable(),
  startPage: z.number().nullable(),
  endPage: z.number().nullable(),
}).openapi("Definition");

export const RootResolutionSchema = z.object({
  root: z.string(),
  confidence: z.enum(["high", "medium", "low"]),
  tier: z.enum(["direct", "stripped", "stemmed", "verb_stem", "pattern", "stem_pattern"]),
}).openapi("RootResolution");

export const LookupResponse = z.object({
  word: z.string(),
  wordNormalized: z.string(),
  resolvedRoots: z.array(RootResolutionSchema).optional(),
  definitions: z.array(DefinitionSchema),
  matchStrategy: z.enum(["exact_vocalized", "exact", "exact_stripped", "root_resolved", "none"]),
  _sources: z.array(SourceSchema),
}).openapi("LookupResponse");

export const SourceListResponse = z.object({
  sources: z.array(DictionarySourceSchema),
  _sources: z.array(SourceSchema),
}).openapi("DictionarySourceListResponse");

// Root param for /root/{root} endpoint
export const RootParam = z.object({
  root: z.string().min(1).max(10).openapi({ example: "كتب" }),
});

export const DerivedFormSchema = z.object({
  word: z.string(),
  vocalized: z.string().nullable(),
  pattern: z.string().nullable(),
  wordType: z.string().nullable(),
  definition: z.string().nullable(),
  partOfSpeech: z.string().nullable(),
  source: z.string().nullable(),
}).openapi("DerivedForm");

export const RootFamilyResponse = z.object({
  root: z.string(),
  rootNormalized: z.string(),
  derivedForms: z.array(DerivedFormSchema),
  dictionaryEntries: z.array(DefinitionSchema.omit({ matchType: true })),
  _sources: z.array(SourceSchema),
}).openapi("RootFamilyResponse");

export const ResolveResponse = z.object({
  word: z.string(),
  wordNormalized: z.string(),
  resolutions: z.array(RootResolutionSchema),
  _sources: z.array(SourceSchema),
}).openapi("ResolveResponse");
