import { OpenAPIHono, createRoute } from "@hono/zod-openapi";
import { prisma } from "../db";
import { normalizeArabic, normalizeArabicLight, hasTashkeel, stripDefiniteArticle, resolveRoot, extractRelevantExcerpt } from "../utils/arabic-text";
import { SOURCES } from "../utils/source-urls";
import { ErrorResponse } from "../schemas/common";
import {
  WordParam,
  RootParam,
  LookupResponse,
  SourceListResponse,
  DictionarySourceSchema,
  RootFamilyResponse,
  ResolveResponse,
} from "../schemas/dictionary";

const LOOKUP_LIMIT = 20;

const SOURCE_SELECT = {
  id: true, slug: true, nameArabic: true, nameEnglish: true, author: true, bookId: true,
} as const;

interface Definition {
  id: number;
  source: { id: number; slug: string; nameArabic: string; nameEnglish: string; author: string | null; bookId: string | null };
  root: string;
  headword: string;
  definition: string;
  definitionHtml: string | null;
  matchType: "exact" | "root";
  precision: "sub_entry" | "excerpt" | "full";
  bookId: string | null;
  startPage: number | null;
  endPage: number | null;
}

// GET /lookup/:word
const lookupRoute = createRoute({
  method: "get",
  path: "/lookup/{word}",
  tags: ["Dictionary"],
  summary: "Look up a word in the Arabic dictionary",
  request: { params: WordParam },
  responses: {
    200: {
      content: { "application/json": { schema: LookupResponse } },
      description: "Dictionary lookup results",
    },
    400: {
      content: { "application/json": { schema: ErrorResponse } },
      description: "Invalid word parameter",
    },
  },
});

// GET /sources
const sourcesRoute = createRoute({
  method: "get",
  path: "/sources",
  tags: ["Dictionary"],
  summary: "List available dictionary sources",
  responses: {
    200: {
      content: { "application/json": { schema: SourceListResponse } },
      description: "List of dictionary sources",
    },
  },
});

export const dictionaryRoutes = new OpenAPIHono();

/**
 * Look up roots for a normalized word from the arabic_roots table.
 * Returns distinct root strings, or empty array if not found.
 */
async function lookupRoots(wordNormalized: string): Promise<string[]> {
  const rows = await prisma.arabicRoot.findMany({
    where: { word: wordNormalized },
    select: { root: true },
    distinct: ["root"],
  });
  return rows.map((r) => r.root);
}

/**
 * Check if a root string exists as a dictionary headword or in the root table.
 */
async function rootExists(root: string): Promise<boolean> {
  const [subEntry, rootEntry] = await Promise.all([
    prisma.dictionarySubEntry.findFirst({
      where: { rootNormalized: root },
      select: { id: true },
    }),
    prisma.arabicRoot.findFirst({
      where: { root },
      select: { id: true },
    }),
  ]);
  return !!(subEntry || rootEntry);
}

/**
 * Find sub-entries matching a headword (exact normalized match).
 */
async function findSubEntriesByHeadword(headwordNormalized: string, limit: number) {
  return prisma.dictionarySubEntry.findMany({
    where: { headwordNormalized },
    include: {
      source: { select: SOURCE_SELECT },
      entry: { select: { bookId: true, startPage: true, endPage: true } },
    },
    take: limit,
  });
}

/**
 * Find sub-entries matching any of the given roots.
 */
async function findSubEntriesByRoots(roots: string[], limit: number) {
  return prisma.dictionarySubEntry.findMany({
    where: { rootNormalized: { in: roots } },
    include: {
      source: { select: SOURCE_SELECT },
      entry: { select: { bookId: true, startPage: true, endPage: true } },
    },
    take: limit,
  });
}

/**
 * Find full dictionary entries matching any of the given roots.
 */
async function findFullEntriesByRoots(roots: string[], limit: number) {
  return prisma.dictionaryEntry.findMany({
    where: { rootNormalized: { in: roots } },
    include: { source: { select: SOURCE_SELECT } },
    take: limit,
  });
}

/**
 * Convert a sub-entry DB row to a Definition.
 */
function subEntryToDefinition(
  sub: Awaited<ReturnType<typeof findSubEntriesByHeadword>>[number],
  matchType: "exact" | "root",
): Definition {
  return {
    id: sub.id,
    source: sub.source,
    root: sub.root,
    headword: sub.headword,
    definition: sub.definitionPlain,
    definitionHtml: sub.definitionHtml,
    matchType,
    precision: "sub_entry",
    bookId: sub.bookId ?? sub.entry?.bookId ?? null,
    startPage: sub.pageNumber ?? sub.entry?.startPage ?? null,
    endPage: sub.pageNumber ?? sub.entry?.endPage ?? null,
  };
}

/**
 * Convert a full entry DB row to a Definition, using excerpt extraction when appropriate.
 */
function fullEntryToDefinition(
  entry: Awaited<ReturnType<typeof findFullEntriesByRoots>>[number],
  matchType: "exact" | "root",
  searchWord?: string,
): Definition {
  let definition: string;
  let definitionHtml: string | null;
  let precision: "sub_entry" | "excerpt" | "full";

  // For short entries (< 300 chars), use the full text
  if (entry.definitionPlain.length <= 300) {
    definition = entry.definitionPlain;
    definitionHtml = entry.definitionHtml;
    precision = "full";
  } else if (searchWord) {
    // Try to extract a relevant excerpt
    const excerpt = extractRelevantExcerpt(entry.definitionPlain, searchWord);
    if (excerpt) {
      definition = excerpt;
      definitionHtml = null;
      precision = "excerpt";
    } else {
      // Truncate
      definition = entry.definitionPlain.slice(0, 300) + "...";
      definitionHtml = null;
      precision = "excerpt";
    }
  } else {
    definition = entry.definitionPlain.slice(0, 300) + "...";
    definitionHtml = null;
    precision = "excerpt";
  }

  return {
    id: entry.id,
    source: entry.source,
    root: entry.root,
    headword: entry.headword,
    definition,
    definitionHtml,
    matchType,
    precision,
    bookId: entry.bookId,
    startPage: entry.startPage,
    endPage: entry.endPage,
  };
}

/**
 * Deduplicate definitions: prefer sub_entry over excerpt/full from same source.
 * Returns at most one result per source.
 */
function dedup(definitions: Definition[]): Definition[] {
  const bySource = new Map<number, Definition>();

  for (const def of definitions) {
    const existing = bySource.get(def.source.id);
    if (!existing) {
      bySource.set(def.source.id, def);
      continue;
    }
    // Prefer sub_entry precision
    const precisionRank = { sub_entry: 0, full: 1, excerpt: 2 };
    if (precisionRank[def.precision] < precisionRank[existing.precision]) {
      bySource.set(def.source.id, def);
    }
  }

  return [...bySource.values()];
}

dictionaryRoutes.openapi(lookupRoute, async (c) => {
  const { word } = c.req.valid("param");
  const wordNormalized = normalizeArabic(word);
  const stripped = stripDefiniteArticle(word);

  let definitions: Definition[] = [];
  let matchStrategy: "exact_vocalized" | "exact" | "exact_stripped" | "root_resolved" | "none" = "none";
  let resolvedRoots: Array<{ root: string; confidence: "high" | "medium" | "low"; tier: string }> | undefined;

  // Step 0: Vocalized match (only when input has tashkeel)
  if (hasTashkeel(word)) {
    const vocalized = normalizeArabicLight(word);
    const vocalizedSubs = await prisma.dictionarySubEntry.findMany({
      where: { headwordVocalized: vocalized },
      include: {
        source: { select: SOURCE_SELECT },
        entry: { select: { bookId: true, startPage: true, endPage: true } },
      },
      take: LOOKUP_LIMIT,
    });
    if (vocalizedSubs.length > 0) {
      definitions = vocalizedSubs.map((s) => subEntryToDefinition(s, "exact"));
      matchStrategy = "exact_vocalized";
    }
    if (definitions.length === 0) {
      const vocalizedEntries = await prisma.dictionaryEntry.findMany({
        where: { headwordVocalized: vocalized },
        include: { source: { select: SOURCE_SELECT } },
        take: LOOKUP_LIMIT,
      });
      if (vocalizedEntries.length > 0) {
        definitions = vocalizedEntries.map((e) => fullEntryToDefinition(e, "exact", word));
        matchStrategy = "exact_vocalized";
      }
    }
  }

  // Step 1: Exact sub-entry match on headwordNormalized
  if (definitions.length === 0) {
    const exactSubs = await findSubEntriesByHeadword(wordNormalized, LOOKUP_LIMIT);
    if (exactSubs.length > 0) {
      definitions = exactSubs.map((s) => subEntryToDefinition(s, "exact"));
      matchStrategy = "exact";
    }
  }

  // Step 2: Stripped sub-entry match (strip ال)
  if (definitions.length === 0 && stripped !== wordNormalized) {
    const strippedSubs = await findSubEntriesByHeadword(stripped, LOOKUP_LIMIT);
    if (strippedSubs.length > 0) {
      definitions = strippedSubs.map((s) => subEntryToDefinition(s, "exact"));
      matchStrategy = "exact_stripped";
    }
  }

  // Step 3: Exact full-entry match on headwordNormalized
  if (definitions.length === 0) {
    const exactFull = await prisma.dictionaryEntry.findMany({
      where: { headwordNormalized: wordNormalized },
      include: { source: { select: SOURCE_SELECT } },
      take: LOOKUP_LIMIT,
    });
    if (exactFull.length > 0) {
      definitions = exactFull.map((e) => fullEntryToDefinition(e, "exact", word));
      matchStrategy = "exact";
    }
  }

  // Step 4: Stripped full-entry match
  if (definitions.length === 0 && stripped !== wordNormalized) {
    const strippedFull = await prisma.dictionaryEntry.findMany({
      where: { headwordNormalized: stripped },
      include: { source: { select: SOURCE_SELECT } },
      take: LOOKUP_LIMIT,
    });
    if (strippedFull.length > 0) {
      definitions = strippedFull.map((e) => fullEntryToDefinition(e, "exact", word));
      matchStrategy = "exact_stripped";
    }
  }

  // Step 5: Root resolution
  if (definitions.length === 0) {
    const resolutions = await resolveRoot(word, lookupRoots, rootExists);
    if (resolutions.length > 0) {
      resolvedRoots = resolutions;
      const roots = resolutions.map((r) => r.root);

      // 5a: Sub-entries by root
      const rootSubs = await findSubEntriesByRoots(roots, LOOKUP_LIMIT);
      if (rootSubs.length > 0) {
        definitions = rootSubs.map((s) => subEntryToDefinition(s, "root"));
        matchStrategy = "root_resolved";
      }

      // 5b: Full entries by root (only if no sub-entries found)
      if (definitions.length === 0) {
        const rootFull = await findFullEntriesByRoots(roots, LOOKUP_LIMIT);
        if (rootFull.length > 0) {
          definitions = rootFull.map((e) => fullEntryToDefinition(e, "root", word));
          matchStrategy = "root_resolved";
        }
      }
    }
  }

  // Dedup: one best result per source
  definitions = dedup(definitions);

  c.header("Cache-Control", "public, max-age=3600");
  return c.json(
    {
      word,
      wordNormalized,
      resolvedRoots,
      definitions,
      matchStrategy,
      _sources: SOURCES.turath,
    },
    200,
  );
});

dictionaryRoutes.openapi(sourcesRoute, async (c) => {
  const sources = await prisma.dictionarySource.findMany({
    select: { id: true, slug: true, nameArabic: true, nameEnglish: true, author: true, bookId: true },
  });

  c.header("Cache-Control", "public, max-age=3600");
  return c.json({ sources, _sources: SOURCES.turath }, 200);
});

// GET /root/:root — word family: all derived forms + dictionary entries for a root
const rootFamilyRoute = createRoute({
  method: "get",
  path: "/root/{root}",
  tags: ["Dictionary"],
  summary: "Get all derived forms and dictionary entries for an Arabic root",
  request: { params: RootParam },
  responses: {
    200: {
      content: { "application/json": { schema: RootFamilyResponse } },
      description: "Root family with derived forms and dictionary entries",
    },
  },
});

dictionaryRoutes.openapi(rootFamilyRoute, async (c) => {
  const { root } = c.req.valid("param");
  const rootNormalized = normalizeArabic(root);

  // Get all derived forms from arabic_roots table
  const derivedForms = await prisma.arabicRoot.findMany({
    where: { root: rootNormalized },
    orderBy: [{ partOfSpeech: "asc" }, { word: "asc" }],
    take: 200,
  });

  // Get sub-entries for this root (preferred — word-level definitions)
  const subEntries = await prisma.dictionarySubEntry.findMany({
    where: { rootNormalized },
    include: {
      source: { select: SOURCE_SELECT },
      entry: { select: { bookId: true, startPage: true, endPage: true } },
    },
    orderBy: [{ sourceId: "asc" }, { position: "asc" }],
    take: 100,
  });

  // Get full dictionary entries for this root (for sources without sub-entries)
  const fullEntries = await prisma.dictionaryEntry.findMany({
    where: { rootNormalized },
    include: { source: { select: SOURCE_SELECT } },
    take: LOOKUP_LIMIT,
  });

  // Merge: prefer sub-entries, fall back to full entries for sources without sub-entries
  const sourcesWithSubs = new Set(subEntries.map((s) => s.source.id));
  const dictionaryEntries: Array<Omit<Definition, "matchType">> = [];

  for (const sub of subEntries) {
    dictionaryEntries.push({
      id: sub.id,
      source: sub.source,
      root: sub.root,
      headword: sub.headword,
      definition: sub.definitionPlain,
      definitionHtml: sub.definitionHtml,
      precision: "sub_entry",
      bookId: sub.bookId ?? sub.entry?.bookId ?? null,
      startPage: sub.pageNumber ?? sub.entry?.startPage ?? null,
      endPage: sub.pageNumber ?? sub.entry?.endPage ?? null,
    });
  }

  for (const entry of fullEntries) {
    if (sourcesWithSubs.has(entry.source.id)) continue;
    dictionaryEntries.push({
      id: entry.id,
      source: entry.source,
      root: entry.root,
      headword: entry.headword,
      definition: entry.definitionPlain.length <= 300
        ? entry.definitionPlain
        : entry.definitionPlain.slice(0, 300) + "...",
      definitionHtml: entry.definitionPlain.length <= 300 ? entry.definitionHtml : null,
      precision: entry.definitionPlain.length <= 300 ? "full" : "excerpt",
      bookId: entry.bookId,
      startPage: entry.startPage,
      endPage: entry.endPage,
    });
  }

  c.header("Cache-Control", "public, max-age=3600");
  return c.json(
    {
      root,
      rootNormalized,
      derivedForms: derivedForms.map((f) => ({
        word: f.word,
        vocalized: f.vocalized,
        pattern: f.pattern,
        wordType: f.wordType,
        definition: f.definition,
        partOfSpeech: f.partOfSpeech,
        source: f.source,
      })),
      dictionaryEntries,
      _sources: SOURCES.turath,
    },
    200,
  );
});

// GET /resolve/:word — resolve any word to its root
const resolveRoute = createRoute({
  method: "get",
  path: "/resolve/{word}",
  tags: ["Dictionary"],
  summary: "Resolve an Arabic word to its root using multi-tier algorithm",
  request: { params: WordParam },
  responses: {
    200: {
      content: { "application/json": { schema: ResolveResponse } },
      description: "Root resolution results",
    },
  },
});

dictionaryRoutes.openapi(resolveRoute, async (c) => {
  const { word } = c.req.valid("param");
  const wordNormalized = normalizeArabic(word);

  const resolutions = await resolveRoot(word, lookupRoots, rootExists);

  c.header("Cache-Control", "public, max-age=3600");
  return c.json(
    {
      word,
      wordNormalized,
      resolutions,
      _sources: SOURCES.turath,
    },
    200,
  );
});
