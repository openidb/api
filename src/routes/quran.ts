import { OpenAPIHono, createRoute } from "@hono/zod-openapi";
import { readFile, stat } from "fs/promises";
import { join } from "path";
import { GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import s3, { BUCKET_NAME } from "../s3";
import { prisma } from "../db";
import { generateQuranUrl, generateTafsirSourceUrl, generateTranslationSourceUrl, SOURCES } from "../utils/source-urls";
import { audioFilePath, getAudioBasePath } from "../utils/audio-storage";
import { ErrorResponse } from "../schemas/common";
import {
  SurahNumberParam, TafsirPathParam, TranslationPathParam,
  AyahsQuery, TafsirListQuery, TafsirQuery, TranslationListQuery, TranslationQuery,
  SurahListResponse, SurahDetailResponse, AyahListResponse,
  TafsirListResponse, TafsirResponse,
  TranslationListResponse, TranslationResponse,
  WordTranslationPathParam, WordTranslationQuery, WordTranslationResponse,
  ReciterListQuery, ReciterListResponse, AudioPathParam, AudioQuery,
  SegmentsQuery, SegmentsResponse,
  MushafPageParam, MushafPageResponse,
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

const getSegments = createRoute({
  method: "get",
  path: "/segments",
  tags: ["Quran"],
  summary: "Get word-level timing segments for a surah",
  request: { query: SegmentsQuery },
  responses: {
    200: {
      content: { "application/json": { schema: SegmentsResponse } },
      description: "Word-level timing segments",
    },
    404: {
      content: { "application/json": { schema: ErrorResponse } },
      description: "Segments not available for this reciter",
    },
  },
});

const getMushafPage = createRoute({
  method: "get",
  path: "/mushaf/{page}",
  tags: ["Quran"],
  summary: "Get mushaf page layout data",
  request: { params: MushafPageParam },
  responses: {
    200: {
      content: { "application/json": { schema: MushafPageResponse } },
      description: "Mushaf page with line/word layout",
    },
    404: {
      content: { "application/json": { schema: ErrorResponse } },
      description: "Page not found",
    },
  },
});

// --- In-memory cache for static data (10-minute TTL) ---
import { TTLCache } from "../lib/ttl-cache";
const quranCache = new TTLCache<unknown>({ maxSize: 100, ttlMs: 10 * 60 * 1000, evictionCount: 20, label: "Quran" });

// --- Segment data cache (in-memory, never expires — segments don't change) ---
const SEGMENT_RECITERS = ["tarteel/alafasy", "tarteel/sudais", "tarteel/rifai", "tarteel/dossary", "tarteel/muaiqly"];
const segmentCache = new Map<string, Record<string, { segments: number[][]; duration: number | null }>>();

// --- Handlers ---

export const quranRoutes = new OpenAPIHono();

quranRoutes.openapi(listSurahs, async (c) => {
  const cacheKey = "surahs";
  let surahs = quranCache.get(cacheKey) as any[] | null;
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
    quranCache.set(cacheKey, surahs);
  }
  c.header("Cache-Control", "public, max-age=86400, stale-while-revalidate=86400");
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

  c.header("Cache-Control", "public, max-age=86400, stale-while-revalidate=86400");
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
  let tafsirs = quranCache.get(cacheKey) as any[] | null;
  if (!tafsirs) {
    const where: Record<string, unknown> = {};
    if (language) where.language = language;
    tafsirs = await prisma.quranTafsir.findMany({
      where,
      orderBy: [{ language: "asc" }, { name: "asc" }],
    });
    quranCache.set(cacheKey, tafsirs);
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
  let translations = quranCache.get(cacheKey) as any[] | null;
  if (!translations) {
    const where: Record<string, unknown> = {};
    if (language) where.language = language;
    translations = await prisma.quranTranslation.findMany({
      where,
      orderBy: [{ language: "asc" }, { name: "asc" }],
    });
    quranCache.set(cacheKey, translations);
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

  c.header("Cache-Control", "public, max-age=86400, stale-while-revalidate=86400");
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

  c.header("Cache-Control", "public, max-age=86400, stale-while-revalidate=86400");
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
  // Tarteel aliases map to existing per-ayah audio (avoids duplicating ~12K files)
  const AUDIO_ALIASES: Record<string, string> = {
    "tarteel/dossary": "yasser-ad-dussary-128kbps",
    "tarteel/muaiqly": "maher-almuaiqly-128kbps",
  };
  const reciterKey = AUDIO_ALIASES[slug] || (slug.includes("/") ? slug.split("/").slice(1).join("/") : slug);
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

// --- Word-level timing segments ---

quranRoutes.openapi(getSegments, async (c) => {
  const { reciter, surah } = c.req.valid("query");

  if (!SEGMENT_RECITERS.includes(reciter)) {
    return c.json({ error: "Segments not available for this reciter" }, 404);
  }

  // Load and cache the _segments.json file
  if (!segmentCache.has(reciter)) {
    try {
      const filePath = join(getAudioBasePath(), reciter, "_segments.json");
      const raw = await readFile(filePath, "utf-8");
      segmentCache.set(reciter, JSON.parse(raw));
    } catch (e) {
      console.error(`[segments] Failed to load segments for ${reciter}:`, e);
      return c.json({ error: "Segments file not found" }, 404);
    }
  }

  const allSegments = segmentCache.get(reciter)!;
  const prefix = `${surah}:`;
  const ayahs: Record<string, { segments: number[][]; duration: number | null }> = {};

  for (const [key, value] of Object.entries(allSegments)) {
    if (key.startsWith(prefix)) {
      const ayahNum = key.slice(prefix.length);
      ayahs[ayahNum] = value;
    }
  }

  c.header("Cache-Control", "public, max-age=86400");
  return c.json({ reciter, surah, ayahs }, 200);
});

// --- Mushaf Page ---

// --- Mushaf PDF presigned URL (must be registered before /mushaf/{page}) ---

quranRoutes.get("/mushaf/pdf", async (c) => {
  const expiresIn = 3600;
  const command = new GetObjectCommand({ Bucket: BUCKET_NAME, Key: "quran/mushaf.pdf" });
  const url = await getSignedUrl(s3, command, { expiresIn });
  c.header("Cache-Control", "public, max-age=3000");
  return c.json({ url, expiresIn });
});

// Stream mushaf PDF directly (supports range requests for pdf.js)
quranRoutes.get("/mushaf/pdf/stream", async (c) => {
  const rangeHeader = c.req.header("range");

  // Generate presigned URL and fetch from RustFS
  const presignedUrl = await getSignedUrl(
    s3,
    new GetObjectCommand({ Bucket: BUCKET_NAME, Key: "quran/mushaf.pdf" }),
    { expiresIn: 3600 },
  );

  const fetchHeaders: HeadersInit = {};
  if (rangeHeader) fetchHeaders["Range"] = rangeHeader;

  const pdfRes = await fetch(presignedUrl, { headers: fetchHeaders });

  const responseHeaders = new Headers();
  responseHeaders.set("Content-Type", "application/pdf");
  responseHeaders.set("Cache-Control", "public, max-age=86400, immutable");
  responseHeaders.set("Accept-Ranges", "bytes");

  const contentLength = pdfRes.headers.get("content-length");
  if (contentLength) responseHeaders.set("Content-Length", contentLength);

  const contentRange = pdfRes.headers.get("content-range");
  if (contentRange) responseHeaders.set("Content-Range", contentRange);

  return new Response(pdfRes.body as any, {
    status: pdfRes.status,
    headers: responseHeaders,
  });
});

quranRoutes.openapi(getMushafPage, async (c) => {
  const { page } = c.req.valid("param");

  const cacheKey = `mushaf:${page}`;
  let cached = quranCache.get(cacheKey);
  if (cached) {
    c.header("Cache-Control", "public, max-age=86400, immutable");
    return c.json(cached as any, 200);
  }

  const words = await prisma.mushafWord.findMany({
    where: { pageNumber: page },
    orderBy: [{ lineNumber: "asc" }, { positionInLine: "asc" }],
  });

  if (words.length === 0) {
    return c.json({ error: "Page not found" }, 404);
  }

  // Group words by line
  const lineMap = new Map<number, typeof words>();
  for (const w of words) {
    if (!lineMap.has(w.lineNumber)) lineMap.set(w.lineNumber, []);
    lineMap.get(w.lineNumber)!.push(w);
  }

  // Build lines using lineType stored in the database
  const lines = Array.from(lineMap.entries())
    .sort(([a], [b]) => a - b)
    .map(([lineNumber, lineWords]) => {
      // Use lineType from the first word on this line (all words on a line share the same type)
      const lineType = lineWords[0].lineType;

      return {
        lineNumber,
        lineType,
        words: lineWords.map((w) => ({
          position: w.positionInLine,
          charType: w.charTypeName,
          surahNumber: w.surahNumber,
          ayahNumber: w.ayahNumber,
          wordPosition: w.wordPosition,
          text: w.textUthmani,
          glyph: w.glyphCode,
        })),
      };
    });

  // Collect unique surahs on this page
  const surahNumbers = [...new Set(words.map((w) => w.surahNumber))].sort((a, b) => a - b);
  const surahs = await prisma.surah.findMany({
    where: { number: { in: surahNumbers } },
    select: { number: true, nameArabic: true, nameEnglish: true },
    orderBy: { number: "asc" },
  });

  // Get juz/hizb from the first ayah on the page
  const firstWord = words.find((w) => w.charTypeName === "word" || w.charTypeName === "end");
  let juzNumber: number | null = null;
  let hizbNumber: number | null = null;
  if (firstWord) {
    const ayah = await prisma.ayah.findFirst({
      where: {
        surah: { number: firstWord.surahNumber },
        ayahNumber: firstWord.ayahNumber,
      },
      select: { juzNumber: true },
    });
    if (ayah) {
      juzNumber = ayah.juzNumber;
      hizbNumber = Math.ceil(juzNumber * 2 * page / 604);
    }
  }

  const response = {
    pageNumber: page,
    totalPages: 604,
    juzNumber,
    hizbNumber,
    surahs: surahs.map((s) => ({
      number: s.number,
      nameArabic: s.nameArabic,
      nameEnglish: s.nameEnglish,
    })),
    lines,
  };

  quranCache.set(cacheKey, response);
  c.header("Cache-Control", "public, max-age=86400, immutable");
  return c.json(response, 200);
});

