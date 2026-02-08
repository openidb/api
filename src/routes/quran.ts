import { Hono } from "hono";
import { prisma } from "../db";
import { generateQuranComUrl, generateTafsirSourceUrl, generateTranslationSourceUrl } from "../utils/source-urls";

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
    _sources: [{ name: "Al Quran Cloud API", url: "https://api.alquran.cloud", type: "api" }],
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
        quranComUrl: generateQuranComUrl(number, a.ayahNumber),
      })),
    },
  });
});

// GET /ayahs — query ayahs with filters
quranRoutes.get("/ayahs", async (c) => {
  const surah = c.req.query("surah");
  const juz = c.req.query("juz");
  const page = c.req.query("page");
  const limitParam = c.req.query("limit");
  const offsetParam = c.req.query("offset");

  const limit = Math.min(Math.max(parseInt(limitParam || "50", 10), 1), 500);
  const offset = Math.max(parseInt(offsetParam || "0", 10), 0);

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
      quranComUrl: generateQuranComUrl(a.surah.number, a.ayahNumber),
    })),
    total,
    limit,
    offset,
  });
});

// GET /tafsir/:surah/:ayah — get tafsir for an ayah
quranRoutes.get("/tafsir/:surah/:ayah", async (c) => {
  const surahNumber = parseInt(c.req.param("surah"), 10);
  const ayahNumber = parseInt(c.req.param("ayah"), 10);
  const source = c.req.query("source");

  const where: Record<string, unknown> = { surahNumber, ayahNumber };
  if (source) where.source = source;

  const tafsirs = await prisma.ayahTafsir.findMany({
    where,
    select: { source: true, text: true },
  });

  return c.json({
    surahNumber,
    ayahNumber,
    tafsirs: tafsirs.map((t) => ({
      ...t,
      sourceUrl: generateTafsirSourceUrl(t.source, surahNumber, ayahNumber),
    })),
    _sources: [
      { name: "quran-tafseer.com (Jalalayn)", url: "http://api.quran-tafseer.com", type: "api" },
      { name: "spa5k/tafsir_api (Ibn Kathir)", url: "https://github.com/spa5k/tafsir_api", type: "api" },
    ],
  });
});

// GET /translations/:surah/:ayah — get translations for an ayah
quranRoutes.get("/translations/:surah/:ayah", async (c) => {
  const surahNumber = parseInt(c.req.param("surah"), 10);
  const ayahNumber = parseInt(c.req.param("ayah"), 10);
  const language = c.req.query("language");

  const where: Record<string, unknown> = { surahNumber, ayahNumber };
  if (language) where.language = language;

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
    _sources: [{ name: "fawazahmed0/quran-api", url: "https://github.com/fawazahmed0/quran-api", type: "api" }],
  });
});
