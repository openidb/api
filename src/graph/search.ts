/**
 * Neo4j Graph Search + Source Resolution
 *
 * Provides graph-augmented search:
 * 1. searchEntities() — full-text entity search + 1-hop traversal (<20ms)
 * 2. resolveSources() — batch-resolves source refs from PostgreSQL
 */

import { neo4jDriver } from "./driver";
import { prisma } from "../db";

// ============================================================================
// Types
// ============================================================================

export interface SourceRef {
  type: "quran" | "hadith" | "tafsir" | "book";
  ref: string;
}

export interface ResolvedSource extends SourceRef {
  text: string;
  metadata: {
    label: string;
    labelEnglish: string;
  };
}

export interface GraphEntity {
  id: string;
  type: string;
  nameArabic: string;
  nameEnglish: string;
  descriptionArabic: string;
  descriptionEnglish: string;
  score: number;
  sources: SourceRef[];
  relationships: {
    type: string;
    targetId: string;
    targetNameArabic: string;
    targetNameEnglish: string;
    description: string;
    sources: SourceRef[];
  }[];
  mentionedIn: {
    ayahGroupId: string;
    role: string;
    context: string;
  }[];
}

export interface GraphSearchResult {
  entities: GraphEntity[];
  allSourceRefs: SourceRef[];
  timingMs: number;
}

export interface GraphContextEntity {
  id: string;
  type: string;
  nameArabic: string;
  nameEnglish: string;
  descriptionArabic: string;
  descriptionEnglish: string;
  sources: ResolvedSource[];
  relationships: {
    type: string;
    targetNameArabic: string;
    targetNameEnglish: string;
    description: string;
    sources: ResolvedSource[];
  }[];
  mentionedIn: {
    surahNumber: number;
    surahNameArabic: string;
    surahNameEnglish: string;
    ayahStart: number;
    ayahEnd: number;
    textUthmani: string;
    role: string;
    context: string;
  }[];
}

export interface GraphContext {
  entities: GraphContextEntity[];
  coverage: "partial" | "full";
  timingMs: number;
}

// ============================================================================
// searchEntities — Full-text search + 1-hop graph traversal
// ============================================================================

export async function searchEntities(query: string): Promise<GraphSearchResult> {
  const start = Date.now();

  let session;
  try {
    session = neo4jDriver.session({ defaultAccessMode: "READ" });
  } catch {
    return { entities: [], allSourceRefs: [], timingMs: 0 };
  }

  try {
    // Escape Lucene special characters for full-text search
    const escaped = query.replace(/[+\-&|!(){}[\]^"~*?:\\/]/g, "\\$&");
    // Try both Arabic and English indexes
    const result = await session.run(
      `
      CALL {
        CALL db.index.fulltext.queryNodes('entity_name_arabic', $query)
        YIELD node, score
        RETURN node, score
        UNION
        CALL db.index.fulltext.queryNodes('entity_name_english', $query)
        YIELD node, score
        RETURN node, score
      }
      WITH node, max(score) AS score
      WHERE score > 0.5
      WITH node, score ORDER BY score DESC LIMIT 5
      OPTIONAL MATCH (node)-[r:MENTIONED_IN]->(g:AyahGroup)
      OPTIONAL MATCH (node)-[rel]->(other:Entity)
      RETURN node {
        .id, .type, .nameArabic, .nameEnglish,
        .descriptionArabic, .descriptionEnglish, .sources
      } AS entity,
      score,
      collect(DISTINCT CASE WHEN r IS NOT NULL THEN {
        role: r.role,
        context: r.context,
        ayahGroupId: g.id
      } END) AS mentions,
      collect(DISTINCT CASE WHEN rel IS NOT NULL AND NOT type(rel) = 'MENTIONED_IN' THEN {
        type: type(rel),
        targetId: other.id,
        targetNameArabic: other.nameArabic,
        targetNameEnglish: other.nameEnglish,
        description: rel.description,
        sources: rel.sources
      } END) AS relationships
      `,
      { query: escaped }
    );

    const entities: GraphEntity[] = [];
    const allSourceRefs: SourceRef[] = [];

    for (const record of result.records) {
      const entity = record.get("entity");
      const score = record.get("score");
      const mentions = record.get("mentions").filter((m: unknown) => m !== null);
      const rels = record.get("relationships").filter((r: unknown) => r !== null);

      // Parse sources from JSON string
      const entitySources: SourceRef[] = parseSources(entity.sources);
      allSourceRefs.push(...entitySources);

      const parsedRels = rels.map((r: { type: string; targetId: string; targetNameArabic: string; targetNameEnglish: string; description: string; sources?: string }) => {
        const relSources = parseSources(r.sources);
        allSourceRefs.push(...relSources);
        return {
          type: r.type,
          targetId: r.targetId,
          targetNameArabic: r.targetNameArabic,
          targetNameEnglish: r.targetNameEnglish,
          description: r.description || "",
          sources: relSources,
        };
      });

      entities.push({
        id: entity.id,
        type: entity.type,
        nameArabic: entity.nameArabic,
        nameEnglish: entity.nameEnglish,
        descriptionArabic: entity.descriptionArabic || "",
        descriptionEnglish: entity.descriptionEnglish || "",
        score: typeof score === "number" ? score : score.toNumber?.() ?? 0,
        sources: entitySources,
        relationships: parsedRels,
        mentionedIn: mentions.map((m: { ayahGroupId: string; role: string; context: string }) => ({
          ayahGroupId: m.ayahGroupId,
          role: m.role || "referenced",
          context: m.context || "",
        })),
      });
    }

    // Deduplicate source refs
    const uniqueRefs = deduplicateRefs(allSourceRefs);

    return {
      entities,
      allSourceRefs: uniqueRefs,
      timingMs: Date.now() - start,
    };
  } catch (err) {
    console.error("[neo4j-search] searchEntities error:", err);
    return { entities: [], allSourceRefs: [], timingMs: Date.now() - start };
  } finally {
    await session.close();
  }
}

// ============================================================================
// resolveSources — Batch-resolve source refs from PostgreSQL
// ============================================================================

export async function resolveSources(
  sourceRefs: SourceRef[]
): Promise<Map<string, ResolvedSource>> {
  const resolved = new Map<string, ResolvedSource>();
  if (sourceRefs.length === 0) return resolved;

  const quranRefs = sourceRefs.filter((r) => r.type === "quran");
  const hadithRefs = sourceRefs.filter((r) => r.type === "hadith");
  const tafsirRefs = sourceRefs.filter((r) => r.type === "tafsir");
  const bookRefs = sourceRefs.filter((r) => r.type === "book");

  await Promise.all([
    resolveQuranRefs(quranRefs, resolved),
    resolveHadithRefs(hadithRefs, resolved),
    resolveTafsirRefs(tafsirRefs, resolved),
    resolveBookRefs(bookRefs, resolved),
  ]);

  return resolved;
}

// ============================================================================
// Quran resolution — "28:7" or "28:36-37" → Ayah text from PostgreSQL
// ============================================================================

async function resolveQuranRefs(
  refs: SourceRef[],
  resolved: Map<string, ResolvedSource>
): Promise<void> {
  if (refs.length === 0) return;

  // Parse all refs into surah+ayah pairs
  const lookups: { ref: string; surahNumber: number; ayahNumbers: number[] }[] = [];
  for (const r of refs) {
    const parsed = parseQuranRef(r.ref);
    if (parsed) lookups.push({ ref: r.ref, ...parsed });
  }

  if (lookups.length === 0) return;

  // Collect all unique surah+ayah pairs for a single batch query
  const allSurahNumbers = [...new Set(lookups.map((l) => l.surahNumber))];
  const allAyahNumbers = [...new Set(lookups.flatMap((l) => l.ayahNumbers))];

  const ayahs = await prisma.ayah.findMany({
    where: {
      surah: { number: { in: allSurahNumbers } },
      ayahNumber: { in: allAyahNumbers },
    },
    select: {
      ayahNumber: true,
      textUthmani: true,
      surah: {
        select: { number: true, nameArabic: true, nameEnglish: true },
      },
    },
  });

  // Index by "surah:ayah"
  const ayahMap = new Map<string, typeof ayahs[0]>();
  for (const a of ayahs) {
    ayahMap.set(`${a.surah.number}:${a.ayahNumber}`, a);
  }

  for (const lookup of lookups) {
    const texts: string[] = [];
    let surahNameArabic = "";
    let surahNameEnglish = "";

    for (const ayahNum of lookup.ayahNumbers) {
      const key = `${lookup.surahNumber}:${ayahNum}`;
      const ayah = ayahMap.get(key);
      if (ayah) {
        texts.push(ayah.textUthmani);
        surahNameArabic = ayah.surah.nameArabic;
        surahNameEnglish = ayah.surah.nameEnglish;
      }
    }

    if (texts.length > 0) {
      const refKey = `quran:${lookup.ref}`;
      resolved.set(refKey, {
        type: "quran",
        ref: lookup.ref,
        text: texts.join(" "),
        metadata: {
          label: `${surahNameArabic} ${toArabicNumerals(lookup.ref)}`,
          labelEnglish: `${surahNameEnglish} ${lookup.ref}`,
        },
      });
    }
  }
}

// ============================================================================
// Hadith resolution — "bukhari:3394" → Hadith text from PostgreSQL
// ============================================================================

async function resolveHadithRefs(
  refs: SourceRef[],
  resolved: Map<string, ResolvedSource>
): Promise<void> {
  if (refs.length === 0) return;

  const lookups: { ref: string; collectionSlug: string; hadithNumber: string }[] = [];
  for (const r of refs) {
    const parts = r.ref.split(":");
    if (parts.length === 2) {
      lookups.push({ ref: r.ref, collectionSlug: parts[0], hadithNumber: parts[1] });
    }
  }

  if (lookups.length === 0) return;

  // Group by collection for efficient querying
  const byCollection = new Map<string, string[]>();
  for (const l of lookups) {
    const nums = byCollection.get(l.collectionSlug) || [];
    nums.push(l.hadithNumber);
    byCollection.set(l.collectionSlug, nums);
  }

  const allHadiths = await Promise.all(
    Array.from(byCollection.entries()).map(([slug, numbers]) =>
      prisma.hadith.findMany({
        where: {
          hadithNumber: { in: numbers },
          book: { collection: { slug } },
        },
        select: {
          hadithNumber: true,
          textArabic: true,
          book: {
            select: {
              collection: {
                select: { slug: true, nameArabic: true, nameEnglish: true },
              },
            },
          },
        },
      })
    )
  );

  const hadithMap = new Map<string, typeof allHadiths[0][0]>();
  for (const batch of allHadiths) {
    for (const h of batch) {
      hadithMap.set(`${h.book.collection.slug}:${h.hadithNumber}`, h);
    }
  }

  for (const lookup of lookups) {
    const hadith = hadithMap.get(lookup.ref);
    if (hadith) {
      const refKey = `hadith:${lookup.ref}`;
      // Truncate long hadith text for display
      const text = hadith.textArabic.length > 300
        ? hadith.textArabic.slice(0, 300) + "..."
        : hadith.textArabic;
      resolved.set(refKey, {
        type: "hadith",
        ref: lookup.ref,
        text,
        metadata: {
          label: `${hadith.book.collection.nameArabic} ${toArabicNumerals(lookup.hadithNumber)}`,
          labelEnglish: `${hadith.book.collection.nameEnglish} ${lookup.hadithNumber}`,
        },
      });
    }
  }
}

// ============================================================================
// Tafsir resolution — "ibn-kathir:28:7" → AyahTafsir text
// ============================================================================

async function resolveTafsirRefs(
  refs: SourceRef[],
  resolved: Map<string, ResolvedSource>
): Promise<void> {
  if (refs.length === 0) return;

  const lookups: { ref: string; source: string; surahNumber: number; ayahNumber: number }[] = [];
  for (const r of refs) {
    // Format: "ibn-kathir:28:7" → source=ibn_kathir, surah=28, ayah=7
    const parts = r.ref.split(":");
    if (parts.length === 3) {
      lookups.push({
        ref: r.ref,
        source: parts[0].replace(/-/g, "_"),
        surahNumber: parseInt(parts[1], 10),
        ayahNumber: parseInt(parts[2], 10),
      });
    }
  }

  if (lookups.length === 0) return;

  // Batch query all tafsir entries
  const tafsirs = await prisma.ayahTafsir.findMany({
    where: {
      OR: lookups.map((l) => ({
        source: l.source,
        surahNumber: l.surahNumber,
        ayahNumber: l.ayahNumber,
      })),
    },
    select: {
      source: true,
      surahNumber: true,
      ayahNumber: true,
      text: true,
    },
  });

  const tafsirMap = new Map<string, typeof tafsirs[0]>();
  for (const t of tafsirs) {
    tafsirMap.set(`${t.source}:${t.surahNumber}:${t.ayahNumber}`, t);
  }

  const sourceDisplayNames: Record<string, { ar: string; en: string }> = {
    ibn_kathir: { ar: "تفسير ابن كثير", en: "Tafsir Ibn Kathir" },
    jalalayn: { ar: "تفسير الجلالين", en: "Tafsir al-Jalalayn" },
    saadi: { ar: "تفسير السعدي", en: "Tafsir as-Sa'di" },
  };

  for (const lookup of lookups) {
    const key = `${lookup.source}:${lookup.surahNumber}:${lookup.ayahNumber}`;
    const tafsir = tafsirMap.get(key);
    if (tafsir) {
      const refKey = `tafsir:${lookup.ref}`;
      const text = tafsir.text.length > 400
        ? tafsir.text.slice(0, 400) + "..."
        : tafsir.text;
      const display = sourceDisplayNames[lookup.source] || { ar: lookup.source, en: lookup.source };
      resolved.set(refKey, {
        type: "tafsir",
        ref: lookup.ref,
        text,
        metadata: {
          label: `${display.ar} ${toArabicNumerals(`${lookup.surahNumber}:${lookup.ayahNumber}`)}`,
          labelEnglish: `${display.en} ${lookup.surahNumber}:${lookup.ayahNumber}`,
        },
      });
    }
  }
}

// ============================================================================
// Book resolution — "book:12345:42" → Page text
// ============================================================================

async function resolveBookRefs(
  refs: SourceRef[],
  resolved: Map<string, ResolvedSource>
): Promise<void> {
  if (refs.length === 0) return;

  const lookups: { ref: string; bookId: string; pageNumber: number }[] = [];
  for (const r of refs) {
    // Format: "book:12345:42"
    const parts = r.ref.split(":");
    if (parts.length === 3 && parts[0] === "book") {
      lookups.push({
        ref: r.ref,
        bookId: parts[1],
        pageNumber: parseInt(parts[2], 10),
      });
    }
  }

  if (lookups.length === 0) return;

  const pages = await prisma.page.findMany({
    where: {
      OR: lookups.map((l) => ({
        bookId: l.bookId,
        pageNumber: l.pageNumber,
      })),
    },
    select: {
      bookId: true,
      pageNumber: true,
      contentPlain: true,
      book: { select: { titleArabic: true, titleLatin: true } },
    },
  });

  const pageMap = new Map<string, typeof pages[0]>();
  for (const p of pages) {
    pageMap.set(`${p.bookId}:${p.pageNumber}`, p);
  }

  for (const lookup of lookups) {
    const key = `${lookup.bookId}:${lookup.pageNumber}`;
    const page = pageMap.get(key);
    if (page) {
      const refKey = `book:${lookup.ref}`;
      const text = page.contentPlain.length > 300
        ? page.contentPlain.slice(0, 300) + "..."
        : page.contentPlain;
      resolved.set(refKey, {
        type: "book",
        ref: lookup.ref,
        text,
        metadata: {
          label: `${page.book.titleArabic} ص${toArabicNumerals(String(lookup.pageNumber))}`,
          labelEnglish: `${page.book.titleLatin} p.${lookup.pageNumber}`,
        },
      });
    }
  }
}

// ============================================================================
// resolveGraphMentions — Resolve AyahGroup mentions to full text from PG
// ============================================================================

export async function resolveGraphMentions(
  entities: GraphEntity[]
): Promise<GraphContextEntity["mentionedIn"][]> {
  // Collect all unique ayahGroupIds
  const allGroupIds = new Set<string>();
  for (const e of entities) {
    for (const m of e.mentionedIn) {
      allGroupIds.add(m.ayahGroupId);
    }
  }

  if (allGroupIds.size === 0) return entities.map(() => []);

  // Parse ayahGroupIds → surah:ayah lookups
  const ayahLookups: { surahNumber: number; ayahNumber: number }[] = [];
  for (const gid of allGroupIds) {
    const parsed = parseQuranRef(gid);
    if (parsed) {
      for (const an of parsed.ayahNumbers) {
        ayahLookups.push({ surahNumber: parsed.surahNumber, ayahNumber: an });
      }
    }
  }

  const allSurahNumbers = [...new Set(ayahLookups.map((l) => l.surahNumber))];
  const allAyahNumbers = [...new Set(ayahLookups.map((l) => l.ayahNumber))];

  const ayahs = await prisma.ayah.findMany({
    where: {
      surah: { number: { in: allSurahNumbers } },
      ayahNumber: { in: allAyahNumbers },
    },
    select: {
      ayahNumber: true,
      textUthmani: true,
      surah: {
        select: { number: true, nameArabic: true, nameEnglish: true },
      },
    },
  });

  const ayahMap = new Map<string, typeof ayahs[0]>();
  for (const a of ayahs) {
    ayahMap.set(`${a.surah.number}:${a.ayahNumber}`, a);
  }

  // Resolve per entity
  return entities.map((entity) =>
    entity.mentionedIn
      .map((m) => {
        const parsed = parseQuranRef(m.ayahGroupId);
        if (!parsed) return null;

        const texts: string[] = [];
        let surahNameArabic = "";
        let surahNameEnglish = "";

        for (const an of parsed.ayahNumbers) {
          const ayah = ayahMap.get(`${parsed.surahNumber}:${an}`);
          if (ayah) {
            texts.push(ayah.textUthmani);
            surahNameArabic = ayah.surah.nameArabic;
            surahNameEnglish = ayah.surah.nameEnglish;
          }
        }

        if (texts.length === 0) return null;

        return {
          surahNumber: parsed.surahNumber,
          surahNameArabic,
          surahNameEnglish,
          ayahStart: parsed.ayahNumbers[0],
          ayahEnd: parsed.ayahNumbers[parsed.ayahNumbers.length - 1],
          textUthmani: texts.join(" "),
          role: m.role,
          context: m.context,
        };
      })
      .filter((m): m is NonNullable<typeof m> => m !== null)
  );
}

// ============================================================================
// Helpers
// ============================================================================

function parseSources(raw: string | undefined | null): SourceRef[] {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      return parsed.filter(
        (s) =>
          s &&
          typeof s.type === "string" &&
          typeof s.ref === "string" &&
          ["quran", "hadith", "tafsir", "book"].includes(s.type)
      );
    }
  } catch {
    // Invalid JSON — ignore
  }
  return [];
}

function parseQuranRef(ref: string): { surahNumber: number; ayahNumbers: number[] } | null {
  // "28:7" → surah 28, ayah [7]
  // "28:36-37" → surah 28, ayahs [36, 37]
  // "28:1-3" → surah 28, ayahs [1, 2, 3]
  const parts = ref.split(":");
  if (parts.length !== 2) return null;

  const surahNumber = parseInt(parts[0], 10);
  if (isNaN(surahNumber)) return null;

  const ayahPart = parts[1];
  if (ayahPart.includes("-")) {
    const [startStr, endStr] = ayahPart.split("-");
    const start = parseInt(startStr, 10);
    const end = parseInt(endStr, 10);
    if (isNaN(start) || isNaN(end)) return null;
    const ayahNumbers: number[] = [];
    for (let i = start; i <= end; i++) ayahNumbers.push(i);
    return { surahNumber, ayahNumbers };
  }

  const ayahNumber = parseInt(ayahPart, 10);
  if (isNaN(ayahNumber)) return null;
  return { surahNumber, ayahNumbers: [ayahNumber] };
}

function deduplicateRefs(refs: SourceRef[]): SourceRef[] {
  const seen = new Set<string>();
  return refs.filter((r) => {
    const key = `${r.type}:${r.ref}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

const arabicDigits = ["\u0660", "\u0661", "\u0662", "\u0663", "\u0664", "\u0665", "\u0666", "\u0667", "\u0668", "\u0669"];

function toArabicNumerals(str: string): string {
  return str.replace(/[0-9]/g, (d) => arabicDigits[parseInt(d, 10)]);
}
