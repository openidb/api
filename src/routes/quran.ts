import { OpenAPIHono, createRoute } from "@hono/zod-openapi";
import { prisma } from "../db";
import { generateQuranUrl, generateTafsirSourceUrl, generateTranslationSourceUrl, SOURCES } from "../utils/source-urls";
import { ErrorResponse } from "../schemas/common";
import {
  SurahNumberParam, TafsirPathParam, TranslationPathParam,
  AyahsQuery, TafsirListQuery, TafsirQuery, TranslationListQuery, TranslationQuery,
  SurahListResponse, SurahDetailResponse, AyahListResponse,
  TafsirListResponse, TafsirResponse,
  TranslationListResponse, TranslationResponse,
} from "../schemas/quran";

// --- Route definitions ---

const listSurahs = createRoute({
  method: "get",
  path: "/surahs",
  tags: ["Quran"],
  summary: "List all surahs",
  responses: {
    200: {
      content: { "application/json": { schema: SurahListResponse } },
      description: "List of all 114 surahs",
    },
  },
});

const getSurah = createRoute({
  method: "get",
  path: "/surahs/{number}",
  tags: ["Quran"],
  summary: "Get surah with ayahs",
  request: { params: SurahNumberParam },
  responses: {
    200: {
      content: { "application/json": { schema: SurahDetailResponse } },
      description: "Surah with all ayahs",
    },
    404: {
      content: { "application/json": { schema: ErrorResponse } },
      description: "Surah not found",
    },
  },
});

const listAyahs = createRoute({
  method: "get",
  path: "/ayahs",
  tags: ["Quran"],
  summary: "Query ayahs with filters",
  request: { query: AyahsQuery },
  responses: {
    200: {
      content: { "application/json": { schema: AyahListResponse } },
      description: "Filtered list of ayahs",
    },
  },
});

const listTafsirs = createRoute({
  method: "get",
  path: "/tafsirs",
  tags: ["Quran"],
  summary: "List available tafsir editions",
  request: { query: TafsirListQuery },
  responses: {
    200: {
      content: { "application/json": { schema: TafsirListResponse } },
      description: "Available tafsir editions",
    },
  },
});

const getTafsir = createRoute({
  method: "get",
  path: "/tafsir/{surah}/{ayah}",
  tags: ["Quran"],
  summary: "Get tafsir for an ayah",
  request: {
    params: TafsirPathParam,
    query: TafsirQuery,
  },
  responses: {
    200: {
      content: { "application/json": { schema: TafsirResponse } },
      description: "Tafsir entries for the ayah",
    },
  },
});

const listTranslations = createRoute({
  method: "get",
  path: "/translations",
  tags: ["Quran"],
  summary: "List available translation editions",
  request: { query: TranslationListQuery },
  responses: {
    200: {
      content: { "application/json": { schema: TranslationListResponse } },
      description: "Available translation editions",
    },
  },
});

const getTranslation = createRoute({
  method: "get",
  path: "/translations/{surah}/{ayah}",
  tags: ["Quran"],
  summary: "Get translations for an ayah",
  request: {
    params: TranslationPathParam,
    query: TranslationQuery,
  },
  responses: {
    200: {
      content: { "application/json": { schema: TranslationResponse } },
      description: "Translation entries for the ayah",
    },
  },
});

// --- Handlers ---

export const quranRoutes = new OpenAPIHono();

quranRoutes.openapi(listSurahs, async (c) => {
  const surahs = await prisma.surah.findMany({
    orderBy: { number: "asc" },
    select: {
      number: true,
      nameArabic: true,
      nameEnglish: true,
      revelationType: true,
      ayahCount: true,
    },
  });
  c.header("Cache-Control", "public, max-age=3600");
  return c.json({
    surahs,
    _sources: [...SOURCES.quranCloud],
  }, 200);
});

quranRoutes.openapi(getSurah, async (c) => {
  const { number } = c.req.valid("param");

  const surah = await prisma.surah.findUnique({
    where: { number },
    include: {
      ayahs: {
        orderBy: { ayahNumber: "asc" },
        select: {
          ayahNumber: true,
          textUthmani: true,
          textPlain: true,
          contentHash: true,
          juzNumber: true,
          pageNumber: true,
        },
      },
    },
  });

  if (!surah) {
    return c.json({ error: "Surah not found" }, 404);
  }

  c.header("Cache-Control", "public, max-age=86400");
  return c.json({
    surah: {
      ...surah,
      ayahs: surah.ayahs.map((a) => ({
        ...a,
        quranComUrl: generateQuranUrl(number, a.ayahNumber),
      })),
    },
    _sources: [...SOURCES.quranCloud],
  }, 200);
});

quranRoutes.openapi(listAyahs, async (c) => {
  const { surah, juz, page, limit, offset } = c.req.valid("query");

  const where: Record<string, unknown> = {};
  if (surah) {
    where.surah = { number: surah };
  }
  if (juz) where.juzNumber = juz;
  if (page) where.pageNumber = page;

  const [ayahs, total] = await Promise.all([
    prisma.ayah.findMany({
      where,
      orderBy: [{ surahId: "asc" }, { ayahNumber: "asc" }],
      take: limit,
      skip: offset,
      select: {
        ayahNumber: true,
        textUthmani: true,
        textPlain: true,
        contentHash: true,
        juzNumber: true,
        pageNumber: true,
        surah: {
          select: {
            number: true,
            nameArabic: true,
            nameEnglish: true,
          },
        },
      },
    }),
    prisma.ayah.count({ where }),
  ]);

  return c.json({
    ayahs: ayahs.map((a) => ({
      ...a,
      quranComUrl: generateQuranUrl(a.surah.number, a.ayahNumber),
    })),
    total,
    limit,
    offset,
    _sources: [...SOURCES.quranCloud],
  }, 200);
});

quranRoutes.openapi(listTafsirs, async (c) => {
  const { language } = c.req.valid("query");
  const where: Record<string, unknown> = {};
  if (language) where.language = language;

  const tafsirs = await prisma.quranTafsir.findMany({
    where,
    orderBy: [{ language: "asc" }, { name: "asc" }],
  });

  return c.json({ tafsirs, count: tafsirs.length }, 200);
});

quranRoutes.openapi(getTafsir, async (c) => {
  const { surah: surahNumber, ayah: ayahNumber } = c.req.valid("param");
  const { source, editionId, language } = c.req.valid("query");

  const where: Record<string, unknown> = { surahNumber, ayahNumber };
  if (editionId) where.editionId = editionId;
  else if (source) where.source = source;
  if (language) where.language = language;

  const tafsirs = await prisma.ayahTafsir.findMany({
    where,
    select: { source: true, editionId: true, language: true, text: true, contentHash: true },
  });

  return c.json({
    surahNumber,
    ayahNumber,
    tafsirs: tafsirs.map((t) => ({
      ...t,
      sourceUrl: generateTafsirSourceUrl(t.editionId, surahNumber),
    })),
    _sources: [...SOURCES.tafsir],
  }, 200);
});

quranRoutes.openapi(listTranslations, async (c) => {
  const { language } = c.req.valid("query");
  const where: Record<string, unknown> = {};
  if (language) where.language = language;

  const translations = await prisma.quranTranslation.findMany({
    where,
    orderBy: [{ language: "asc" }, { name: "asc" }],
  });

  return c.json({ translations, count: translations.length }, 200);
});

quranRoutes.openapi(getTranslation, async (c) => {
  const { surah: surahNumber, ayah: ayahNumber } = c.req.valid("param");
  const { language, editionId } = c.req.valid("query");

  const where: Record<string, unknown> = { surahNumber, ayahNumber };
  if (editionId) where.editionId = editionId;
  else if (language) where.language = language;

  const translations = await prisma.ayahTranslation.findMany({
    where,
    select: { language: true, editionId: true, text: true, contentHash: true },
  });

  return c.json({
    surahNumber,
    ayahNumber,
    translations: translations.map((t) => ({
      ...t,
      sourceUrl: generateTranslationSourceUrl(t.editionId),
    })),
    _sources: [...SOURCES.quranTranslation],
  }, 200);
});
