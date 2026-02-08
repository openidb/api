/**
 * Quran Resource Registry
 *
 * Fetches and normalizes available editions from upstream sources:
 * - fawazahmed0/quran-api — Quran translations (500+ editions)
 * - spa5k/tafsir_api — Quran tafsirs (27 editions)
 *
 * Provides metadata sync to populate QuranTranslation and QuranTafsir tables.
 */

import { prisma } from "../../src/db";

// CDN base URLs
const TRANSLATIONS_EDITIONS_URL =
  "https://cdn.jsdelivr.net/gh/fawazahmed0/quran-api@1/editions.json";
const TAFSIRS_EDITIONS_URL =
  "https://cdn.jsdelivr.net/gh/spa5k/tafsir_api@main/tafsir/editions.json";
const TRANSLATION_CDN_BASE =
  "https://cdn.jsdelivr.net/gh/fawazahmed0/quran-api@1/editions";
const TAFSIR_CDN_BASE =
  "https://cdn.jsdelivr.net/gh/spa5k/tafsir_api@main/tafsir";

// Map full language names from fawazahmed0 editions.json to ISO 639-1 codes
const LANGUAGE_TO_ISO: Record<string, string> = {
  afar: "aa", albanian: "sq", amharic: "am", amazigh: "zgh", arabic: "ar",
  azerbaijani: "az", bengali: "bn", berber: "ber", bosnian: "bs",
  bulgarian: "bg", chinese: "zh", czech: "cs", divehi: "dv", dutch: "nl",
  english: "en", french: "fr", german: "de", hausa: "ha", hindi: "hi",
  indonesian: "id", italian: "it", japanese: "ja", korean: "ko",
  kurdish: "ku", malay: "ms", malayalam: "ml", norwegian: "no",
  pashto: "ps", persian: "fa", polish: "pl", portuguese: "pt",
  romanian: "ro", russian: "ru", sindhi: "sd", somali: "so", spanish: "es",
  swahili: "sw", swedish: "sv", tajik: "tg", tamil: "ta", tatar: "tt",
  thai: "th", turkish: "tr", uighur: "ug", ukrainian: "uk", urdu: "ur",
  uzbek: "uz", vietnamese: "vi", yoruba: "yo",
};

function normalizeLanguageCode(lang: string): string {
  const lower = lang.toLowerCase();
  return LANGUAGE_TO_ISO[lower] || lower.slice(0, 2).toLowerCase();
}

// RTL languages
const RTL_LANGUAGES = new Set(["ar", "ur", "fa", "he", "ps", "sd", "dv", "ku", "ug"]);

// ============================================================================
// Translation Editions
// ============================================================================

export interface TranslationEdition {
  id: string;       // "eng-mustafakhattaba"
  language: string;  // ISO code: "en"
  name: string;      // Author/display name
  author: string | null;
  direction: "ltr" | "rtl";
  cdnUrl: string;
}

interface FawazEdition {
  name: string;
  author: string;
  language: string;
  direction: "ltr" | "rtl";
  source: string;
  comments: string;
  link?: string;
  linkmin?: string;
}

export async function fetchTranslationEditions(): Promise<TranslationEdition[]> {
  const response = await fetch(TRANSLATIONS_EDITIONS_URL);
  if (!response.ok) {
    throw new Error(`Failed to fetch translation editions: ${response.status}`);
  }

  const data: Record<string, FawazEdition> = await response.json();
  const editions: TranslationEdition[] = [];

  for (const [, edition] of Object.entries(data)) {
    const id = edition.name; // e.g. "eng-mustafakhattaba"
    const isoLang = normalizeLanguageCode(edition.language);
    editions.push({
      id,
      language: isoLang,
      name: edition.author || edition.name,
      author: edition.author || null,
      direction: RTL_LANGUAGES.has(isoLang) ? "rtl" : "ltr",
      cdnUrl: `${TRANSLATION_CDN_BASE}/${id}.json`,
    });
  }

  return editions;
}

export async function syncTranslationMetadata(
  editions: TranslationEdition[]
): Promise<number> {
  let synced = 0;
  // Batch upsert in chunks
  const BATCH = 50;
  for (let i = 0; i < editions.length; i += BATCH) {
    const batch = editions.slice(i, i + BATCH);
    for (const ed of batch) {
      await prisma.quranTranslation.upsert({
        where: { id: ed.id },
        update: {
          language: ed.language,
          name: ed.name,
          author: ed.author,
          direction: ed.direction,
        },
        create: {
          id: ed.id,
          language: ed.language,
          name: ed.name,
          author: ed.author,
          source: "fawazahmed0",
          direction: ed.direction,
        },
      });
      synced++;
    }
  }
  return synced;
}

// ============================================================================
// Tafsir Editions
// ============================================================================

export interface TafsirEdition {
  id: string;       // "ar-tafsir-ibn-kathir"
  slug: string;     // same as id for spa5k
  language: string;  // ISO code: "ar", "en", etc.
  name: string;
  author: string | null;
  direction: "ltr" | "rtl";
  sourceAttribution: string; // "quran.com" or "altafsir.com"
}

interface Spa5kEdition {
  id: number;
  author_name: string;
  name: string;
  language_name: string;
  slug: string;
  source: string;
}

// Map spa5k language names to ISO codes
const TAFSIR_LANG_MAP: Record<string, string> = {
  arabic: "ar", english: "en", bengali: "bn", urdu: "ur",
  russian: "ru", kurdish: "ku",
};

export async function fetchTafsirEditions(): Promise<TafsirEdition[]> {
  const response = await fetch(TAFSIRS_EDITIONS_URL);
  if (!response.ok) {
    throw new Error(`Failed to fetch tafsir editions: ${response.status}`);
  }

  const data: Spa5kEdition[] = await response.json();
  return data.map((t) => {
    const isoLang = TAFSIR_LANG_MAP[t.language_name.toLowerCase()] || t.language_name.slice(0, 2).toLowerCase();
    return {
      id: t.slug,
      slug: t.slug,
      language: isoLang,
      name: t.name,
      author: t.author_name || null,
      direction: RTL_LANGUAGES.has(isoLang) ? "rtl" : "ltr",
      sourceAttribution: t.source,
    };
  });
}

export async function syncTafsirMetadata(
  editions: TafsirEdition[]
): Promise<number> {
  let synced = 0;
  for (const ed of editions) {
    await prisma.quranTafsir.upsert({
      where: { id: ed.id },
      update: {
        language: ed.language,
        name: ed.name,
        author: ed.author,
        direction: ed.direction,
      },
      create: {
        id: ed.id,
        language: ed.language,
        name: ed.name,
        author: ed.author,
        source: "spa5k-tafsir",
        direction: ed.direction,
      },
    });
    synced++;
  }
  return synced;
}

// Re-export CDN base for use in import scripts
export { TRANSLATION_CDN_BASE, TAFSIR_CDN_BASE };
