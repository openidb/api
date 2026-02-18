import { OpenAPIHono, createRoute } from "@hono/zod-openapi";
import { stat } from "fs/promises";
import { GetObjectCommand } from "@aws-sdk/client-s3";
import s3 from "../s3";
import { prisma } from "../db";
import { generateQuranUrl, generateTafsirSourceUrl, generateTranslationSourceUrl, SOURCES } from "../utils/source-urls";
import { audioFilePath } from "../utils/audio-storage";
import { ErrorResponse } from "../schemas/common";
import {
  SurahNumberParam, TafsirPathParam, TranslationPathParam,
  AyahsQuery, TafsirListQuery, TafsirQuery, TranslationListQuery, TranslationQuery,
  SurahListResponse, SurahDetailResponse, AyahListResponse,
  TafsirListResponse, TafsirResponse,
  TranslationListResponse, TranslationResponse,
  WordTranslationPathParam, WordTranslationQuery, WordTranslationResponse,
  ReciterListQuery, ReciterListResponse, AudioPathParam, AudioQuery,
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

const getWordTranslations = createRoute({
  method: "get",
  path: "/word-translations/{surah}/{ayah}",
  tags: ["Quran"],
  summary: "Get word-by-word translations for an ayah",
  request: {
    params: WordTranslationPathParam,
    query: WordTranslationQuery,
  },
  responses: {
    200: {
      content: { "application/json": { schema: WordTranslationResponse } },
      description: "Word-by-word translations for the ayah",
    },
  },
});

const listReciters = createRoute({
  method: "get",
  path: "/reciters",
  tags: ["Quran"],
  summary: "List available reciters",
  request: { query: ReciterListQuery },
  responses: {
    200: {
      content: { "application/json": { schema: ReciterListResponse } },
      description: "Available reciters",
    },
  },
});

const getAudio = createRoute({
  method: "get",
  path: "/audio/{surah}/{ayah}",
  tags: ["Quran"],
  summary: "Stream ayah audio",
  request: {
    params: AudioPathParam,
    query: AudioQuery,
  },
  responses: {
    200: {
      content: { "audio/mpeg": { schema: { type: "string", format: "binary" } } },
      description: "MP3 audio file",
    },
    404: {
      content: { "application/json": { schema: ErrorResponse } },
      description: "Audio file not found",
    },
  },
});

// --- In-memory cache for static data (10-minute TTL) ---
const CACHE_TTL_MS = 10 * 60 * 1000;
const quranCache = new Map<string, { data: unknown; expiry: number }>();

function getCached<T>(key: string): T | null {
  const entry = quranCache.get(key);
  if (entry && Date.now() < entry.expiry) return entry.data as T;
  quranCache.delete(key);
  return null;
}

function setCache(key: string, data: unknown) {
  quranCache.set(key, { data, expiry: Date.now() + CACHE_TTL_MS });
}

// --- Handlers ---

export const quranRoutes = new OpenAPIHono();

quranRoutes.openapi(listSurahs, async (c) => {
  const cacheKey = "surahs";
  let surahs = getCached<any[]>(cacheKey);
  if (!surahs) {
    surahs = await prisma.surah.findMany({
      orderBy: { number: "asc" },
      select: {
        number: true,
        nameArabic: true,
        nameEnglish: true,
        revelationType: true,
        ayahCount: true,
      },
    });
    setCache(cacheKey, surahs);
  }
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
  const cacheKey = `tafsirs:${language || "all"}`;
  let tafsirs = getCached<any[]>(cacheKey);
  if (!tafsirs) {
    const where: Record<string, unknown> = {};
    if (language) where.language = language;
    tafsirs = await prisma.quranTafsir.findMany({
      where,
      orderBy: [{ language: "asc" }, { name: "asc" }],
    });
    setCache(cacheKey, tafsirs);
  }

  return c.json({ tafsirs, count: tafsirs.length, _sources: [...SOURCES.tafsir, ...SOURCES.qul] }, 200);
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
  const cacheKey = `translations:${language || "all"}`;
  let translations = getCached<any[]>(cacheKey);
  if (!translations) {
    const where: Record<string, unknown> = {};
    if (language) where.language = language;
    translations = await prisma.quranTranslation.findMany({
      where,
      orderBy: [{ language: "asc" }, { name: "asc" }],
    });
    setCache(cacheKey, translations);
  }

  return c.json({ translations, count: translations.length, _sources: [...SOURCES.quranTranslation, ...SOURCES.qul] }, 200);
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

// --- Word-by-Word Translations ---

quranRoutes.openapi(getWordTranslations, async (c) => {
  const { surah: surahNumber, ayah: ayahNumber } = c.req.valid("param");
  const { language } = c.req.valid("query");

  const words = await prisma.wordTranslation.findMany({
    where: { surahNumber, ayahNumber, language },
    orderBy: { wordPosition: "asc" },
    select: { wordPosition: true, text: true, transliteration: true },
  });

  c.header("Cache-Control", "public, max-age=86400");
  return c.json({
    surahNumber,
    ayahNumber,
    language,
    words: words.map((w) => ({
      position: w.wordPosition,
      text: w.text,
      transliteration: w.transliteration,
    })),
    _sources: [...SOURCES.wordTranslation],
  }, 200);
});

// --- Reciters & Audio ---

quranRoutes.openapi(listReciters, async (c) => {
  const { qiraat, language } = c.req.valid("query");
  const where: Record<string, unknown> = {};
  if (qiraat) where.qiraat = qiraat;
  if (language) where.language = language;

  const reciters = await prisma.quranReciter.findMany({
    where,
    orderBy: [{ source: "asc" }, { nameEnglish: "asc" }],
    select: {
      id: true, slug: true, nameArabic: true, nameEnglish: true,
      style: true, qiraat: true, bitrate: true, totalAyahs: true,
      language: true, source: true,
    },
  });

  c.header("Cache-Control", "public, max-age=3600");
  return c.json({ reciters, count: reciters.length, _sources: [...SOURCES.quranAudio] }, 200);
});

quranRoutes.openapi(getAudio, async (c) => {
  const { surah, ayah } = c.req.valid("param");
  const { reciter: reciterSlug } = c.req.valid("query");

  // Resolve reciter — use provided slug or fall back to first complete reciter
  let slug = reciterSlug;
  if (!slug) {
    const defaultReciter = await prisma.quranReciter.findFirst({
      where: { totalAyahs: 6236 },
      orderBy: { id: "asc" },
      select: { slug: true },
    });
    if (!defaultReciter) {
      return c.json({ error: "No complete reciters available" }, 404);
    }
    slug = defaultReciter.slug;
  }

  const filename = `${String(surah).padStart(3, "0")}${String(ayah).padStart(3, "0")}.mp3`;
  // RustFS stores files without source prefix: "alafasy-128kbps/001001.mp3"
  // DB slugs include source prefix: "everyayah/alafasy-128kbps"
  const reciterKey = slug.includes("/") ? slug.split("/").slice(1).join("/") : slug;
  const s3Key = `${reciterKey}/${filename}`;

  // Try rustfs first, fall back to local disk
  try {
    const obj = await s3.send(new GetObjectCommand({
      Bucket: "quran-audio",
      Key: s3Key,
    }));

    if (!obj.Body) throw new Error("Empty body");

    const body = await obj.Body.transformToByteArray();
    return new Response(body, {
      status: 200,
      headers: {
        "Content-Type": "audio/mpeg",
        "Content-Length": String(body.length),
        "Accept-Ranges": "bytes",
        "Cache-Control": "public, max-age=86400, immutable",
      },
    }) as unknown as ReturnType<typeof c.json>;
  } catch {
    // Fall back to local disk
  }

  const filePath = audioFilePath(slug, surah, ayah);

  let fileStat;
  try {
    fileStat = await stat(filePath);
  } catch {
    return c.json({ error: "Audio file not found" }, 404);
  }

  const file = Bun.file(filePath);
  return new Response(file, {
    status: 200,
    headers: {
      "Content-Type": "audio/mpeg",
      "Content-Length": String(fileStat.size),
      "Accept-Ranges": "bytes",
      "Cache-Control": "public, max-age=86400, immutable",
    },
  }) as unknown as ReturnType<typeof c.json>;
});
