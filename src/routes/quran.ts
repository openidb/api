import { Hono } from "hono";
import { prisma } from "../db";
import { generateQuranUrl, generateTafsirSourceUrl, generateTranslationSourceUrl, SOURCES } from "../utils/source-urls";
import { parsePagination } from "../utils/pagination";

export const quranRoutes = new Hono();

// GET /surahs — list all surahs
quranRoutes.get("/surahs", async (c) => {
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
  return c.json({
    surahs,
    _sources: SOURCES.quranCloud,
  });
});

// GET /surahs/:number — get surah with ayahs
quranRoutes.get("/surahs/:number", async (c) => {
  const number = parseInt(c.req.param("number"), 10);
  if (isNaN(number) || number < 1 || number > 114) {
    return c.json({ error: "Invalid surah number" }, 400);
  }

  const surah = await prisma.surah.findUnique({
    where: { number },
    include: {
      ayahs: {
        orderBy: { ayahNumber: "asc" },
        select: {
          ayahNumber: true,
          textUthmani: true,
          textPlain: true,
          juzNumber: true,
          pageNumber: true,
        },
      },
    },
  });

  if (!surah) {
    return c.json({ error: "Surah not found" }, 404);
  }

  return c.json({
    surah: {
      ...surah,
      ayahs: surah.ayahs.map((a) => ({
        ...a,
        quranUrl: generateQuranUrl(number, a.ayahNumber),
      })),
    },
    _sources: SOURCES.quranCloud,
  });
});

// GET /ayahs — query ayahs with filters
quranRoutes.get("/ayahs", async (c) => {
  const surah = c.req.query("surah");
  const juz = c.req.query("juz");
  const page = c.req.query("page");
  const { limit, offset } = parsePagination(c.req.query("limit"), c.req.query("offset"), 50, 500);

  const where: Record<string, unknown> = {};
  if (surah) {
    where.surah = { number: parseInt(surah, 10) };
  }
  if (juz) where.juzNumber = parseInt(juz, 10);
  if (page) where.pageNumber = parseInt(page, 10);

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
      quranUrl: generateQuranUrl(a.surah.number, a.ayahNumber),
    })),
    total,
    limit,
    offset,
    _sources: SOURCES.quranCloud,
  });
});

// GET /tafsirs — list available tafsir editions
quranRoutes.get("/tafsirs", async (c) => {
  const language = c.req.query("language");
  const where: Record<string, unknown> = {};
  if (language) where.language = language;

  const tafsirs = await prisma.quranTafsir.findMany({
    where,
    orderBy: [{ language: "asc" }, { name: "asc" }],
  });

  return c.json({ tafsirs, count: tafsirs.length });
});

// GET /tafsir/:surah/:ayah — get tafsir for an ayah
quranRoutes.get("/tafsir/:surah/:ayah", async (c) => {
  const surahNumber = parseInt(c.req.param("surah"), 10);
  const ayahNumber = parseInt(c.req.param("ayah"), 10);
  const source = c.req.query("source");
  const editionId = c.req.query("editionId");
  const language = c.req.query("language");

  const where: Record<string, unknown> = { surahNumber, ayahNumber };
  if (editionId) where.editionId = editionId;
  else if (source) where.source = source;
  if (language) where.language = language;

  const tafsirs = await prisma.ayahTafsir.findMany({
    where,
    select: { source: true, editionId: true, language: true, text: true },
  });

  return c.json({
    surahNumber,
    ayahNumber,
    tafsirs: tafsirs.map((t) => ({
      ...t,
      sourceUrl: generateTafsirSourceUrl(t.editionId, surahNumber),
    })),
    _sources: SOURCES.tafsir,
  });
});

// GET /translations — list available translation editions
quranRoutes.get("/translations", async (c) => {
  const language = c.req.query("language");
  const where: Record<string, unknown> = {};
  if (language) where.language = language;

  const translations = await prisma.quranTranslation.findMany({
    where,
    orderBy: [{ language: "asc" }, { name: "asc" }],
  });

  return c.json({ translations, count: translations.length });
});

// GET /translations/:surah/:ayah — get translations for an ayah
quranRoutes.get("/translations/:surah/:ayah", async (c) => {
  const surahNumber = parseInt(c.req.param("surah"), 10);
  const ayahNumber = parseInt(c.req.param("ayah"), 10);
  const language = c.req.query("language");
  const editionId = c.req.query("editionId");

  const where: Record<string, unknown> = { surahNumber, ayahNumber };
  if (editionId) where.editionId = editionId;
  else if (language) where.language = language;

  const translations = await prisma.ayahTranslation.findMany({
    where,
    select: { language: true, editionId: true, text: true },
  });

  return c.json({
    surahNumber,
    ayahNumber,
    translations: translations.map((t) => ({
      ...t,
      sourceUrl: generateTranslationSourceUrl(t.editionId),
    })),
    _sources: SOURCES.quranTranslation,
  });
});
