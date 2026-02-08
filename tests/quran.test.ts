import { describe, test, expect } from "bun:test";
import { get, expectOk, expectPagination, expectSources } from "./helpers/api";

describe("GET /api/quran/surahs", () => {
  test("returns all 114 surahs with correct fields", async () => {
    const data = expectOk(await get("/api/quran/surahs"));
    expect(data.surahs).toHaveLength(114);
    expectSources(data);

    const first = data.surahs[0];
    expect(first.number).toBe(1);
    expect(typeof first.nameArabic).toBe("string");
    expect(typeof first.nameEnglish).toBe("string");
    expect(typeof first.revelationType).toBe("string");
    expect(typeof first.ayahCount).toBe("number");

    const last = data.surahs[113];
    expect(last.number).toBe(114);
  });
});

describe("GET /api/quran/surahs/:number", () => {
  test("surah 1 (Al-Fatiha) has 7 ayahs", async () => {
    const data = expectOk(await get("/api/quran/surahs/1"));
    expect(data.surah.number).toBe(1);
    expect(data.surah.ayahs).toHaveLength(7);
    expectSources(data);

    const ayah = data.surah.ayahs[0];
    expect(ayah.ayahNumber).toBe(1);
    expect(typeof ayah.textUthmani).toBe("string");
    expect(typeof ayah.textPlain).toBe("string");
    expect(typeof ayah.juzNumber).toBe("number");
    expect(typeof ayah.pageNumber).toBe("number");
    expect(typeof ayah.quranUrl).toBe("string");
  });

  test("surah 114 exists and has ayahs", async () => {
    const data = expectOk(await get("/api/quran/surahs/114"));
    expect(data.surah.number).toBe(114);
    expect(data.surah.ayahs.length).toBeGreaterThan(0);
  });

  test("surah 0 returns 400", async () => {
    const res = await get("/api/quran/surahs/0");
    expect(res.status).toBe(400);
  });

  test("surah 115 returns 400", async () => {
    const res = await get("/api/quran/surahs/115");
    expect(res.status).toBe(400);
  });

  test("surah 'abc' returns 400", async () => {
    const res = await get("/api/quran/surahs/abc");
    expect(res.status).toBe(400);
  });
});

describe("GET /api/quran/ayahs", () => {
  test("default returns paginated ayahs with total 6236", async () => {
    const data = expectOk(await get("/api/quran/ayahs"));
    expect(data.total).toBe(6236);
    expectPagination(data);
    expectSources(data);

    const ayah = data.ayahs[0];
    expect(typeof ayah.ayahNumber).toBe("number");
    expect(typeof ayah.textUthmani).toBe("string");
    expect(ayah.surah).toBeDefined();
    expect(typeof ayah.surah.number).toBe("number");
    expect(typeof ayah.quranUrl).toBe("string");
  });

  test("?surah=1 returns 7 ayahs, all from surah 1", async () => {
    const data = expectOk(await get("/api/quran/ayahs", { surah: 1 }));
    expect(data.total).toBe(7);
    for (const ayah of data.ayahs) {
      expect(ayah.surah.number).toBe(1);
    }
  });

  test("?juz=1 returns ayahs from juz 1", async () => {
    const data = expectOk(await get("/api/quran/ayahs", { juz: 1 }));
    expect(data.total).toBeGreaterThan(0);
    for (const ayah of data.ayahs) {
      expect(ayah.juzNumber).toBe(1);
    }
  });

  test("?limit=5&offset=10 respects pagination", async () => {
    const data = expectOk(
      await get("/api/quran/ayahs", { limit: 5, offset: 10 })
    );
    expect(data.ayahs.length).toBe(5);
    expect(data.offset).toBe(10);
  });
});

describe("GET /api/quran/tafsirs", () => {
  test("returns tafsir editions with count", async () => {
    const data = expectOk(await get("/api/quran/tafsirs"));
    expect(data.count).toBe(data.tafsirs.length);
    expect(data.tafsirs.length).toBeGreaterThan(0);

    const t = data.tafsirs[0];
    expect(t.id).toBeDefined();
    expect(typeof t.language).toBe("string");
    expect(typeof t.name).toBe("string");
  });

  test("?language=ar filters to Arabic", async () => {
    const data = expectOk(await get("/api/quran/tafsirs", { language: "ar" }));
    expect(data.tafsirs.length).toBeGreaterThan(0);
    for (const t of data.tafsirs) {
      expect(t.language).toBe("ar");
    }
  });
});

describe("GET /api/quran/tafsir/:surah/:ayah", () => {
  test("1/1 returns tafsirs", async () => {
    const data = expectOk(await get("/api/quran/tafsir/1/1"));
    expect(data.surahNumber).toBe(1);
    expect(data.ayahNumber).toBe(1);
    expect(data.tafsirs.length).toBeGreaterThan(0);
    expectSources(data);

    const t = data.tafsirs[0];
    expect(typeof t.source).toBe("string");
    expect(typeof t.editionId).toBe("string");
    expect(typeof t.language).toBe("string");
    expect(typeof t.text).toBe("string");
    expect(typeof t.sourceUrl).toBe("string");
  });

  test("1/1?language=ar filters to Arabic", async () => {
    const data = expectOk(
      await get("/api/quran/tafsir/1/1", { language: "ar" })
    );
    for (const t of data.tafsirs) {
      expect(t.language).toBe("ar");
    }
  });

  test("1/999 returns empty tafsirs array", async () => {
    const data = expectOk(await get("/api/quran/tafsir/1/999"));
    expect(data.tafsirs).toHaveLength(0);
  });
});

describe("GET /api/quran/translations", () => {
  test("returns translation editions with count", async () => {
    const data = expectOk(await get("/api/quran/translations"));
    expect(data.count).toBe(data.translations.length);
    expect(data.translations.length).toBeGreaterThan(0);

    const t = data.translations[0];
    expect(t.id).toBeDefined();
    expect(typeof t.language).toBe("string");
    expect(typeof t.name).toBe("string");
  });

  test("?language=en filters to English", async () => {
    const data = expectOk(
      await get("/api/quran/translations", { language: "en" })
    );
    expect(data.translations.length).toBeGreaterThan(0);
    for (const t of data.translations) {
      expect(t.language).toBe("en");
    }
  });
});

describe("GET /api/quran/translations/:surah/:ayah", () => {
  test("1/1 returns translations", async () => {
    const data = expectOk(await get("/api/quran/translations/1/1"));
    expect(data.surahNumber).toBe(1);
    expect(data.ayahNumber).toBe(1);
    expect(data.translations.length).toBeGreaterThan(0);
    expectSources(data);

    const t = data.translations[0];
    expect(typeof t.language).toBe("string");
    expect(typeof t.editionId).toBe("string");
    expect(typeof t.text).toBe("string");
    expect(typeof t.sourceUrl).toBe("string");
  });

  test("1/1?editionId=eng-mustafakhattaba returns exactly 1 result", async () => {
    const data = expectOk(
      await get("/api/quran/translations/1/1", {
        editionId: "eng-mustafakhattaba",
      })
    );
    expect(data.translations).toHaveLength(1);
    expect(data.translations[0].editionId).toBe("eng-mustafakhattaba");
  });

  test("1/999 returns empty translations array", async () => {
    const data = expectOk(await get("/api/quran/translations/1/999"));
    expect(data.translations).toHaveLength(0);
  });
});
