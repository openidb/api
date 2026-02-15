import { describe, test, expect } from "bun:test";
import {
  calculateRRFScore,
  mergeWithRRF,
  mergeWithRRFGeneric,
  getMatchType,
  mergeAndDeduplicateBooks,
  mergeAndDeduplicateAyahs,
  mergeAndDeduplicateHadiths,
} from "../routes/search/fusion";
import { RRF_K, SEMANTIC_WEIGHT, KEYWORD_WEIGHT, FLOAT_TOLERANCE } from "../routes/search/config";
import type { RankedResult, AyahRankedResult, HadithRankedResult } from "../routes/search/types";

// --- calculateRRFScore ---

describe("calculateRRFScore", () => {
  test("returns 0 for empty ranks", () => {
    expect(calculateRRFScore([])).toBe(0);
  });

  test("returns 0 for all-undefined ranks", () => {
    expect(calculateRRFScore([undefined, undefined])).toBe(0);
  });

  test("calculates correctly for a single rank", () => {
    // 1 / (60 + 1) = 1/61
    expect(calculateRRFScore([1])).toBeCloseTo(1 / (RRF_K + 1), 10);
  });

  test("calculates correctly for rank 0", () => {
    // 1 / (60 + 0) = 1/60
    expect(calculateRRFScore([0])).toBeCloseTo(1 / RRF_K, 10);
  });

  test("sums contributions from multiple ranks", () => {
    const expected = 1 / (RRF_K + 1) + 1 / (RRF_K + 5);
    expect(calculateRRFScore([1, 5])).toBeCloseTo(expected, 10);
  });

  test("skips undefined ranks in mixed array", () => {
    const expected = 1 / (RRF_K + 3);
    expect(calculateRRFScore([undefined, 3, undefined])).toBeCloseTo(expected, 10);
  });

  test("higher ranks produce higher scores", () => {
    const rank0 = calculateRRFScore([0]);
    const rank10 = calculateRRFScore([10]);
    const rank100 = calculateRRFScore([100]);
    expect(rank0).toBeGreaterThan(rank10);
    expect(rank10).toBeGreaterThan(rank100);
  });
});

// --- mergeWithRRFGeneric ---

describe("mergeWithRRFGeneric", () => {
  test("returns empty array when both inputs are empty", () => {
    const result = mergeWithRRFGeneric([], [], (r: any) => r.id, "test");
    expect(result).toEqual([]);
  });

  test("returns semantic-only results with correct scores", () => {
    const semantic = [
      { id: "a", semanticRank: 0, semanticScore: 0.9 },
    ];
    const result = mergeWithRRFGeneric(semantic, [], (r: any) => r.id, "test");
    expect(result).toHaveLength(1);
    // Semantic-only: fusedScore = semanticScore
    expect(result[0].fusedScore).toBeCloseTo(0.9, 5);
  });

  test("returns keyword-only results with normalized BM25 score", () => {
    const keyword = [
      { id: "a", keywordRank: 0, bm25Score: 10 },
    ];
    const result = mergeWithRRFGeneric([], keyword, (r: any) => r.id, "test");
    expect(result).toHaveLength(1);
    // Keyword-only: fusedScore = normalizeBM25Score(10) = 10/(10+5) = 0.667
    expect(result[0].fusedScore).toBeCloseTo(10 / (10 + 5), 3);
  });

  test("merges overlapping results with both scores", () => {
    const semantic = [
      { id: "a", semanticRank: 0, semanticScore: 0.8 },
    ];
    const keyword = [
      { id: "a", keywordRank: 0, bm25Score: 10 },
    ];
    const result = mergeWithRRFGeneric(semantic, keyword, (r: any) => r.id, "test");
    expect(result).toHaveLength(1);
    // Both: SEMANTIC_WEIGHT * 0.8 + KEYWORD_WEIGHT * normalizeBM25(10)
    const expectedFused = SEMANTIC_WEIGHT * 0.8 + KEYWORD_WEIGHT * (10 / (10 + 5));
    expect(result[0].fusedScore).toBeCloseTo(expectedFused, 3);
  });

  test("preserves non-overlapping results from both lists", () => {
    const semantic = [
      { id: "a", semanticRank: 0, semanticScore: 0.9 },
    ];
    const keyword = [
      { id: "b", keywordRank: 0, bm25Score: 15 },
    ];
    const result = mergeWithRRFGeneric(semantic, keyword, (r: any) => r.id, "test");
    expect(result).toHaveLength(2);
  });

  test("sorts by fusedScore descending, then rrfScore as tiebreaker", () => {
    const semantic = [
      { id: "a", semanticRank: 0, semanticScore: 0.5 },
      { id: "b", semanticRank: 1, semanticScore: 0.9 },
    ];
    const result = mergeWithRRFGeneric(semantic, [], (r: any) => r.id, "test");
    // b has higher semanticScore so should be first
    expect(result[0].id).toBe("b");
    expect(result[1].id).toBe("a");
  });

  test("calls onMerge when keyword matches existing semantic result", () => {
    let merged = false;
    const semantic = [{ id: "a", semanticRank: 0, semanticScore: 0.8, extra: "old" }];
    const keyword = [{ id: "a", keywordRank: 0, bm25Score: 5, extra: "new" }];
    mergeWithRRFGeneric(semantic, keyword, (r: any) => r.id, "test", {
      onMerge: (existing, incoming) => {
        merged = true;
        (existing as any).extra = (incoming as any).extra;
      },
    });
    expect(merged).toBe(true);
  });
});

// --- mergeWithRRF (book-specific) ---

describe("mergeWithRRF", () => {
  const makeBookResult = (overrides: Partial<RankedResult>): RankedResult => ({
    bookId: "1",
    pageNumber: 1,
    volumeNumber: 1,
    textSnippet: "test",
    highlightedSnippet: "test",
    ...overrides,
  });

  test("merges books by bookId-pageNumber key", () => {
    const semantic = [
      makeBookResult({ bookId: "1", pageNumber: 10, semanticRank: 0, semanticScore: 0.8 }),
    ];
    const keyword = [
      makeBookResult({ bookId: "1", pageNumber: 10, keywordRank: 0, bm25Score: 8, highlightedSnippet: "<b>match</b>" }),
    ];
    const result = mergeWithRRF(semantic, keyword, "test");
    expect(result).toHaveLength(1);
    // Should use keyword's highlighted snippet
    expect(result[0].highlightedSnippet).toBe("<b>match</b>");
  });

  test("keeps separate entries for different pages of same book", () => {
    const semantic = [
      makeBookResult({ bookId: "1", pageNumber: 10, semanticRank: 0, semanticScore: 0.9 }),
      makeBookResult({ bookId: "1", pageNumber: 20, semanticRank: 1, semanticScore: 0.7 }),
    ];
    const result = mergeWithRRF(semantic, [], "test");
    expect(result).toHaveLength(2);
  });
});

// --- getMatchType ---

describe("getMatchType", () => {
  test("returns 'both' when both ranks exist", () => {
    const result = getMatchType({ semanticRank: 0, keywordRank: 1 } as RankedResult);
    expect(result).toBe("both");
  });

  test("returns 'semantic' when only semantic rank exists", () => {
    const result = getMatchType({ semanticRank: 0 } as RankedResult);
    expect(result).toBe("semantic");
  });

  test("returns 'keyword' when only keyword rank exists", () => {
    const result = getMatchType({ keywordRank: 0 } as RankedResult);
    expect(result).toBe("keyword");
  });

  test("returns 'keyword' when neither rank exists", () => {
    const result = getMatchType({} as RankedResult);
    expect(result).toBe("keyword");
  });
});

// --- mergeAndDeduplicateBooks ---

describe("mergeAndDeduplicateBooks", () => {
  test("returns empty array for no queries", () => {
    expect(mergeAndDeduplicateBooks([])).toEqual([]);
  });

  test("deduplicates same book-page across queries", () => {
    const results = mergeAndDeduplicateBooks([
      {
        results: [
          { bookId: "1", pageNumber: 5, volumeNumber: 1, textSnippet: "t", highlightedSnippet: "t", semanticScore: 0.8 } as RankedResult,
        ],
        weight: 1.0,
      },
      {
        results: [
          { bookId: "1", pageNumber: 5, volumeNumber: 1, textSnippet: "t", highlightedSnippet: "<b>t</b>", semanticScore: 0.7 } as RankedResult,
        ],
        weight: 0.5,
      },
    ]);
    expect(results).toHaveLength(1);
    // Should keep higher semanticScore
    expect(results[0].semanticScore).toBe(0.8);
    // Should take highlighted snippet from second (since it differs from textSnippet)
    expect(results[0].highlightedSnippet).toBe("<b>t</b>");
  });

  test("higher-weighted queries contribute more to ranking", () => {
    const results = mergeAndDeduplicateBooks([
      {
        results: [
          { bookId: "1", pageNumber: 1, volumeNumber: 1, textSnippet: "a", highlightedSnippet: "a" } as RankedResult,
        ],
        weight: 1.0,
      },
      {
        results: [
          { bookId: "2", pageNumber: 1, volumeNumber: 1, textSnippet: "b", highlightedSnippet: "b" } as RankedResult,
        ],
        weight: 0.1,
      },
    ]);
    // First result should be from higher-weight query
    expect(results[0].bookId).toBe("1");
  });
});

// --- mergeAndDeduplicateAyahs ---

describe("mergeAndDeduplicateAyahs", () => {
  test("deduplicates by surah-ayah", () => {
    const ayah: AyahRankedResult = {
      surahNumber: 2, ayahNumber: 255, score: 0.9, surahNameArabic: "البقرة",
      surahNameEnglish: "Al-Baqarah", text: "آية الكرسي", juzNumber: 3,
      pageNumber: 42, quranComUrl: "/quran/2/255",
    };
    const results = mergeAndDeduplicateAyahs([
      { results: [{ ...ayah, semanticScore: 0.9 }], weight: 1.0 },
      { results: [{ ...ayah, semanticScore: 0.8 }], weight: 0.5 },
    ]);
    expect(results).toHaveLength(1);
    expect(results[0].semanticScore).toBe(0.9);
  });
});

// --- mergeAndDeduplicateHadiths ---

describe("mergeAndDeduplicateHadiths", () => {
  test("deduplicates by collection-hadith number", () => {
    const hadith: HadithRankedResult = {
      score: 0.85, bookId: 1, collectionSlug: "bukhari", collectionNameArabic: "صحيح البخاري",
      collectionNameEnglish: "Sahih al-Bukhari", bookNumber: 1, bookNameArabic: "بدء الوحي",
      bookNameEnglish: "Revelation", hadithNumber: "1", text: "إنما الأعمال بالنيات",
      chapterArabic: null, chapterEnglish: null, sourceUrl: "https://sunnah.com/bukhari:1",
    };
    const results = mergeAndDeduplicateHadiths([
      { results: [hadith], weight: 1.0 },
      { results: [{ ...hadith, semanticScore: 0.7 }], weight: 0.5 },
    ]);
    expect(results).toHaveLength(1);
  });
});

// --- Config constants sanity checks ---

describe("Search config constants", () => {
  test("RRF_K is the standard value", () => {
    expect(RRF_K).toBe(60);
  });

  test("fusion weights allow max combined score > 1 (rewards both-method matches)", () => {
    expect(SEMANTIC_WEIGHT + KEYWORD_WEIGHT).toBeGreaterThan(1);
  });

  test("FLOAT_TOLERANCE is small but non-zero", () => {
    expect(FLOAT_TOLERANCE).toBeGreaterThan(0);
    expect(FLOAT_TOLERANCE).toBeLessThan(0.01);
  });
});
