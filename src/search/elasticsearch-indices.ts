/**
 * Elasticsearch Index Configurations
 *
 * Defines Arabic text analyzer and index mappings for:
 * - arabic_pages: Book pages
 * - arabic_hadiths: Hadith texts with collection metadata
 * - arabic_ayahs: Quran ayahs with surah metadata
 */

import type { IndicesCreateRequest } from "@elastic/elasticsearch/lib/api/types";

/**
 * Arabic text analyzer settings
 *
 * Normalization steps:
 * 1. Remove diacritics (tashkeel): U+064B-U+065F, U+0670 (superscript alef)
 * 2. Normalize alef variants: \u0622\u0623\u0625\u0671 \u2192 \u0627
 * 3. Normalize teh marbuta: \u0629 \u2192 \u0647
 * 4. Remove Arabic stopwords
 */
const arabicAnalyzerSettings = {
  analysis: {
    char_filter: {
      arabic_normalize: {
        type: "pattern_replace" as const,
        // Remove diacritics (tashkeel)
        pattern: "[\u064B-\u065F\u0670]",
        replacement: "",
      },
      alef_normalize: {
        type: "mapping" as const,
        // Normalize alef variants to plain alef, remove standalone hamza, normalize alef maksura
        mappings: [
          "\u0622=>\u0627", // آ → ا
          "\u0623=>\u0627", // أ → ا
          "\u0625=>\u0627", // إ → ا
          "\u0671=>\u0627", // ٱ → ا (alef wasla)
          "\u0621=>",       // ء → (remove standalone hamza)
          "\u0649=>\u064A", // ى → ي (alef maksura to yeh)
        ],
      },
      teh_marbuta_normalize: {
        type: "mapping" as const,
        // Normalize teh marbuta to heh
        mappings: ["\u0629=>\u0647"], // ة → ه
      },
    },
    filter: {
      arabic_stopwords: {
        type: "stop" as const,
        stopwords: [
          // Common Arabic stopwords
          "على",
          "في",
          "من",
          "إلى",
          "الى",
          "هذا",
          "هذه",
          "التي",
          "الذي",
          "الذين",
          "ان",
          "أن",
          "إن",
          "كان",
          "كانت",
          "عن",
          "مع",
          "هو",
          "هي",
          "ما",
          "لا",
          "قد",
          "قال",
          "بن",
          "ابن",
          "بين",
          "كل",
          "ذلك",
          "تلك",
          "أو",
          "او",
          "ثم",
          "بعد",
          "قبل",
          "عند",
          "له",
          "لها",
          "لهم",
          "به",
          "بها",
          "فيه",
          "فيها",
          "منه",
          "منها",
          "إذا",
          "اذا",
          "لم",
          "لن",
          "حتى",
          "وقد",
          "ولا",
          "وهو",
          "وهي",
          "ومن",
          "فإن",
          "فان",
          "والله",
        ],
      },
    },
    analyzer: {
      arabic_normalized: {
        type: "custom" as const,
        char_filter: [
          "arabic_normalize",
          "alef_normalize",
          "teh_marbuta_normalize",
        ],
        tokenizer: "standard",
        filter: ["lowercase", "arabic_stopwords"],
      },
      // Analyzer without stopwords for exact phrase matching
      arabic_normalized_no_stop: {
        type: "custom" as const,
        char_filter: [
          "arabic_normalize",
          "alef_normalize",
          "teh_marbuta_normalize",
        ],
        tokenizer: "standard",
        filter: ["lowercase"],
      },
    },
  },
};

/**
 * Pages index configuration
 */
export const pagesIndexConfig: IndicesCreateRequest = {
  index: "arabic_pages",
  settings: {
    ...arabicAnalyzerSettings,
    number_of_shards: 1,
    number_of_replicas: 0,
  },
  mappings: {
    properties: {
      book_id: { type: "keyword" },
      page_number: { type: "integer" },
      volume_number: { type: "integer" },
      content_plain: { type: "text", index: false }, // Stored for display, not searchable
      text_searchable: {
        type: "text",
        analyzer: "arabic_normalized",
        search_analyzer: "arabic_normalized",
        fields: {
          exact: {
            type: "text",
            analyzer: "arabic_normalized_no_stop",
            search_analyzer: "arabic_normalized_no_stop",
          },
        },
      },
      url_page_index: { type: "keyword" },
    },
  },
};

/**
 * Hadiths index configuration
 * Includes denormalized collection/book metadata for fast retrieval
 */
export const hadithsIndexConfig: IndicesCreateRequest = {
  index: "arabic_hadiths",
  settings: {
    ...arabicAnalyzerSettings,
    number_of_shards: 1,
    number_of_replicas: 0,
  },
  mappings: {
    properties: {
      id: { type: "integer" },
      book_id: { type: "integer" },
      hadith_number: { type: "keyword" },
      text_arabic: { type: "text", index: false },
      text_plain: { type: "text", index: false },
      text_searchable: {
        type: "text",
        analyzer: "arabic_normalized",
        search_analyzer: "arabic_normalized",
        fields: {
          exact: {
            type: "text",
            analyzer: "arabic_normalized_no_stop",
            search_analyzer: "arabic_normalized_no_stop",
          },
        },
      },
      chapter_arabic: { type: "text", index: false },
      chapter_english: { type: "keyword" },
      book_number: { type: "integer" },
      book_name_arabic: { type: "keyword" },
      book_name_english: { type: "keyword" },
      collection_slug: { type: "keyword" },
      collection_name_arabic: { type: "keyword" },
      collection_name_english: { type: "keyword" },
      is_chain_variation: { type: "boolean" },
    },
  },
};

/**
 * Ayahs index configuration
 * Includes denormalized surah metadata for fast retrieval
 */
export const ayahsIndexConfig: IndicesCreateRequest = {
  index: "arabic_ayahs",
  settings: {
    ...arabicAnalyzerSettings,
    number_of_shards: 1,
    number_of_replicas: 0,
  },
  mappings: {
    properties: {
      id: { type: "integer" },
      ayah_number: { type: "integer" },
      text_uthmani: { type: "text", index: false },
      text_plain: { type: "text", index: false },
      text_searchable: {
        type: "text",
        analyzer: "arabic_normalized",
        search_analyzer: "arabic_normalized",
        fields: {
          exact: {
            type: "text",
            analyzer: "arabic_normalized_no_stop",
            search_analyzer: "arabic_normalized_no_stop",
          },
        },
      },
      juz_number: { type: "integer" },
      page_number: { type: "integer" },
      surah_id: { type: "integer" },
      surah_number: { type: "integer" },
      surah_name_arabic: { type: "keyword" },
      surah_name_english: { type: "keyword" },
    },
  },
};
