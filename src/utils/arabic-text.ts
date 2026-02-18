/**
 * Arabic Text Normalization & Stemming Utilities
 *
 * Used for dictionary lookup: normalizing headwords/roots and matching user queries.
 */

// Tashkeel (diacritics) range: fathatan through alef superscript
const TASHKEEL_RE = /[\u064B-\u065F\u0670]/g;

// Alef variants → bare alef
const ALEF_VARIANTS_RE = /[\u0623\u0625\u0622\u0671]/g; // أ إ آ ٱ

// All hamza forms → bare alef (used specifically for root extraction, not general normalization)
const HAMZA_NORMALIZE_RE = /[\u0621\u0623\u0624\u0625\u0622\u0626\u0671]/g; // ء أ ؤ إ آ ئ ٱ

/**
 * Normalize Arabic text for comparison:
 * - Strip diacritics (tashkeel)
 * - Normalize alef variants (أ إ آ ٱ → ا)
 * - Alef maksura → yeh (ى → ي)
 * - Teh marbuta → heh (ة → ه)
 */
export function normalizeArabic(text: string): string {
  return text
    .replace(TASHKEEL_RE, "")
    .replace(ALEF_VARIANTS_RE, "\u0627") // → ا
    .replace(/\u0649/g, "\u064A")        // ى → ي
    .replace(/\u0629/g, "\u0647")        // ة → ه
    .trim();
}

// Definite-article prefixes only, ordered longest first for greedy match
const DEFINITE_ARTICLE_PREFIXES = ["بال", "فال", "وال", "كال", "لل", "ال"];

/**
 * Normalize all hamza variants to bare alef.
 * Used specifically for root extraction where ء ؤ ئ أ إ all represent the same consonant.
 */
function normalizeHamza(text: string): string {
  return text.replace(HAMZA_NORMALIZE_RE, "\u0627"); // → ا
}

/**
 * Strip only definite-article prefixes from a normalized Arabic word.
 * Conservative: does NOT strip other prefixes (و, ف, ب, ك, ل) or any suffixes,
 * avoiding false matches caused by aggressive stemming.
 */
export function stripDefiniteArticle(word: string): string {
  const norm = normalizeArabic(word);
  for (const prefix of DEFINITE_ARTICLE_PREFIXES) {
    if (norm.startsWith(prefix) && norm.length - prefix.length > 2) {
      return norm.slice(prefix.length);
    }
  }
  return norm;
}

// Prefixes ordered longest first for greedy match
const PREFIXES = ["وال", "فال", "بال", "كال", "لل", "ال", "و", "ف", "ب", "ك", "ل"];
// Suffixes ordered longest first
const SUFFIXES = ["ات", "ون", "ين", "ها", "هم", "كم", "نا", "ه", "ك", "ي"];

// Imperfect verb prefixes (يـ تـ أـ نـ) and suffixes (ون ان ين ن وا)
const IMPERFECT_PREFIXES = ["ي", "ت", "ا", "ن"];
const IMPERFECT_SUFFIXES = ["ون", "ان", "ين", "وا", "ن"];

/**
 * Strip imperfect verb conjugation affixes.
 * يستقيمون → ستقيم, تكتبان → كتبا, يكتبون → كتب
 * Returns array of candidates (original + stripped forms).
 */
function stripImperfectAffixes(word: string): string[] {
  const norm = normalizeArabic(word);
  const candidates: string[] = [];

  for (const prefix of IMPERFECT_PREFIXES) {
    if (!norm.startsWith(prefix)) continue;
    const afterPrefix = norm.slice(prefix.length);
    if (afterPrefix.length < 3) continue;

    // Prefix-only first (longest/least aggressive) — catches Form X stems etc.
    candidates.push(afterPrefix);

    // Then try stripping suffixes too (most aggressive last)
    // Require remaining >= 3 chars (trilateral root minimum)
    for (const suffix of IMPERFECT_SUFFIXES) {
      if (afterPrefix.endsWith(suffix) && afterPrefix.length - suffix.length >= 3) {
        candidates.push(afterPrefix.slice(0, -suffix.length));
      }
    }
  }

  return [...new Set(candidates)];
}

/**
 * Light Arabic stemmer for dictionary root matching.
 * Normalizes then strips one prefix and one suffix if the remaining word has length > 2.
 */
function stemArabic(word: string): string {
  let stem = normalizeArabic(word);

  // Strip one prefix
  for (const prefix of PREFIXES) {
    if (stem.startsWith(prefix) && stem.length - prefix.length > 2) {
      stem = stem.slice(prefix.length);
      break;
    }
  }

  // Strip one suffix
  for (const suffix of SUFFIXES) {
    if (stem.endsWith(suffix) && stem.length - suffix.length > 2) {
      stem = stem.slice(0, -suffix.length);
      break;
    }
  }

  return stem;
}

/**
 * Arabic pattern-based root extraction.
 *
 * Tries to extract the 3-letter (or 4-letter quadriliteral) root from a
 * normalized Arabic word by matching against common derivational patterns (أوزان).
 *
 * Returns the extracted root or null if no pattern matches.
 */
export function extractArabicRoot(word: string): string | null {
  const w = normalizeArabic(word);
  const len = w.length;

  // Only attempt extraction for words of 3–8 letters
  if (len < 3 || len > 8) return null;

  // 3-letter words are likely already roots
  if (len === 3) return w;

  // ---- 4-letter patterns ----
  if (len === 4) {
    // Doubled root (shadda): مدد→مدد, شدد→شدد — C₁ C₂ C₂ → keep as-is (quadriliteral)
    // Only flag, don't reduce — the 4-letter form IS the normalized doubled root

    // فِعَال (fi'āl): كتاب→كتب, جهاد→جهد, جمال→جمل
    if (w[2] === "ا") return w[0] + w[1] + w[3];

    // فَعِيل (fa'īl): كبير→كبر, جميل→جمل, عظيم→عظم
    if (w[2] === "ي") return w[0] + w[1] + w[3];

    // فَاعِل (fā'il): كاتب→كتب, عالم→علم, حاكم→حكم
    if (w[1] === "ا") return w[0] + w[2] + w[3];

    // فُعُول (fu'ūl): دروس→درس, نزول→نزل
    if (w[2] === "و") return w[0] + w[1] + w[3];

    // أَفْعَل (af'al): أكبر→كبر, أحسن→حسن, أعلم→علم
    if (w[0] === "ا") return w[1] + w[2] + w[3];

    // مَفْعَل (maf'al): مكتب→كتب, ملعب→لعب
    if (w[0] === "م") return w[1] + w[2] + w[3];

    // تَفْعَل / تَفَعَّل 4-letter form: تعلم→علم
    if (w[0] === "ت") return w[1] + w[2] + w[3];
  }

  // ---- 5-letter patterns ----
  if (len === 5) {
    // إِفْعَال (if'āl): إسلام→سلم, إيمان→يمن
    if (w[0] === "ا" && w[3] === "ا") return w[1] + w[2] + w[4];

    // تَفَعُّل (tafa''ul): تعلّم→علم — after normalization: ت C₁ C₂ C₂ C₃
    if (w[0] === "ت" && w[2] === w[3]) return w[1] + w[2] + w[4];

    // تَفْعِيل (taf'īl): تعليم→علم, تقديم→قدم
    if (w[0] === "ت" && w[3] === "ي") return w[1] + w[2] + w[4];

    // مَفْعُول (maf'ūl): مكتوب→كتب, معلوم→علم
    if (w[0] === "م" && w[3] === "و") return w[1] + w[2] + w[4];

    // مِفْعَال (mif'āl): مفتاح→فتح
    if (w[0] === "م" && w[3] === "ا") return w[1] + w[2] + w[4];

    // تَفَاعُل (tafā'ul): تعاون→عون, تواصل→وصل
    if (w[0] === "ت" && w[2] === "ا") return w[1] + w[3] + w[4];

    // فَعَّال (fa''āl): كتّاب→كتب, حمّال→حمل — C₁ C₂ C₂ ا C₃
    if (w[1] === w[2] && w[3] === "ا") return w[0] + w[1] + w[4];

    // فُعْلَان (fu'lān): غفران→غفر, سلطان→سلط
    if (w[3] === "ا" && w[4] === "ن") return w[0] + w[1] + w[2];

    // فَعَلَان (fa'alān): طيران→طير
    // Same structure — handled by the fu'lān rule above

    // فِعَالَة (fi'āla): كتابه→كتب, تجاره→تجر
    if (w[2] === "ا" && w[4] === "ه") return w[0] + w[1] + w[3];

    // مَفْعَلَة (maf'ala): مكتبه→كتب, مدرسه→درس
    if (w[0] === "م" && w[4] === "ه") return w[1] + w[2] + w[3];

    // مَفْعُولَة — 6 letters after normalization, handled below

    // مُفَعِّل (mufa''il): مُعلِّم → م C₁ C₂ C₂ C₃
    if (w[0] === "م" && w[2] === w[3]) return w[1] + w[2] + w[4];

    // فَعِيلَة (fa'īla): حقيقه→حقق, كريمه→كرم
    if (w[2] === "ي" && w[4] === "ه") return w[0] + w[1] + w[3];

    // اِفْعَلَّ (if'alla, Form IX): احمر→حمر — ا C₁ C₂ C₃ C₃
    if (w[0] === "ا" && w[3] === w[4]) return w[1] + w[2] + w[3];

    // سْتَفْعَل (Form X stem without imperfect prefix): ستعين→عين, ستخدم→خدم
    if (w[0] === "س" && w[1] === "ت") return w[2] + w[3] + w[4];

    // Quadriliteral فَعْلَلَ: ترجم, زلزل, دحرج — 4 consonants, no pattern prefix
    // These are already 4-letter roots if no affix matched above
  }

  // ---- 5-letter imperfect verb patterns (يـ/تـ/نـ/أـ prefix) ----
  if (len === 5) {
    const isImperfectPrefix = w[0] === "ي" || w[0] === "ت" || w[0] === "ن" || w[0] === "ا";
    if (isImperfectPrefix) {
      // يَفْتَعِل (yafta'il, Form VIII impf): يكتسب→كسب — P C₁ ت C₂ C₃
      if (w[2] === "ت") return w[1] + w[3] + w[4];

      // يَنْفَعِل (yanfa'il, Form VII impf): ينكسر→كسر — P ن C₁ C₂ C₃
      if (w[1] === "ن") return w[2] + w[3] + w[4];

      // يُفَعِّل (yufa''il, Form II impf): يعلّم→علم — P C₁ C₂ C₂ C₃
      if (w[2] === w[3]) return w[1] + w[2] + w[4];

      // يُفَاعِل (yufaa'il, Form III impf): يقاتل→قتل — P C₁ ا C₂ C₃
      if (w[2] === "ا") return w[1] + w[3] + w[4];

      // يَفْعَل (yaf'al, Form I impf): يكتب→كتب — P C₁ C₂ C₃ (already caught below)
      // يُفْعِل (yuf'il, Form IV impf): يسلم→سلم — P C₁ C₂ C₃
    }
  }

  // ---- 6-letter patterns ----
  if (len === 6) {
    // مُسْتَفْعِل (mustaf'il): مستخدم→خدم, مستعمل→عمل
    if (w[0] === "م" && w[1] === "س" && w[2] === "ت") return w[3] + w[4] + w[5];

    // يَسْتَفْعِل (yastaf'il, Form X impf): يستقيم→قيم, يستخدم→خدم — P س ت C₁ C₂ C₃
    const isImperfectPrefix = w[0] === "ي" || w[0] === "ت" || w[0] === "ن" || w[0] === "ا";
    if (isImperfectPrefix && w[1] === "س" && w[2] === "ت") return w[3] + w[4] + w[5];

    // يَتَفَعَّل (yatafa''al, Form V impf): يتعلّم→علم — P ت C₁ C₂ C₂ C₃
    if (isImperfectPrefix && w[1] === "ت" && w[3] === w[4]) return w[2] + w[3] + w[5];

    // يَتَفَاعَل (yatafaa'al, Form VI impf): يتقاتل→قتل — P ت C₁ ا C₂ C₃
    if (isImperfectPrefix && w[1] === "ت" && w[3] === "ا") return w[2] + w[4] + w[5];

    // اِنْفِعَال (infi'āl): انكسار→كسر, انفتاح→فتح
    if (w[0] === "ا" && w[1] === "ن" && w[4] === "ا") return w[2] + w[3] + w[5];

    // اِفْتِعَال (ifti'āl): اجتهاد→جهد, اختيار→خير
    if (w[0] === "ا" && w[2] === "ت" && w[4] === "ا") return w[1] + w[3] + w[5];

    // مَفْعُولَة (maf'ūla): مكتوبه→كتب — م C₁ C₂ و C₃ ه
    if (w[0] === "م" && w[3] === "و" && w[5] === "ه") return w[1] + w[2] + w[4];

    // تَفْعِيلَة: تكملَه→كمل — ت C₁ C₂ ي C₃ ه
    if (w[0] === "ت" && w[3] === "ي" && w[5] === "ه") return w[1] + w[2] + w[4];

    // اِسْتَفْعَلَ (istaf'ala, Form X verb): استخدم→خدم — ا س ت C₁ C₂ C₃
    if (w[0] === "ا" && w[1] === "س" && w[2] === "ت") return w[3] + w[4] + w[5];

    // تَفَعْلُل (tafa'lul) masdar of quadriliteral: تدحرجه→دحرج
    if (w[0] === "ت" && w[5] === "ه") return w[1] + w[2] + w[3] + w[4];
  }

  // ---- 7-letter patterns ----
  if (len === 7) {
    // اِسْتِفْعَال (istif'āl): استخدام→خدم, استعمال→عمل
    if (w[0] === "ا" && w[1] === "س" && w[2] === "ت" && w[5] === "ا") return w[3] + w[4] + w[6];

    // يَسْتَفْعِلُون (yastaf'ilūn, Form X impf + suffix) — 7-letter after suffix strip
    // Handled by stripImperfectAffixes reducing to 6-letter form → then 6-letter patterns

    // اِفْعِيلَال (if'ilāl): احميرار→حمر — ا C₁ C₂ ي C₃ ا C₃
    if (w[0] === "ا" && w[3] === "ي" && w[5] === "ا" && w[4] === w[6]) return w[1] + w[2] + w[4];
  }

  // ---- 8-letter patterns ----
  if (len === 8) {
    // اِسْتِفْعَالَة: استخدامه — ا س ت C₁ C₂ ا C₃ ه
    if (w[0] === "ا" && w[1] === "س" && w[2] === "ت" && w[5] === "ا" && w[7] === "ه") return w[3] + w[4] + w[6];
  }

  return null;
}

/**
 * Attempt to resolve weak-letter ambiguities in an extracted root.
 * Arabic "weak" roots have و or ي that transform into ا in derived forms.
 *
 * Returns candidate roots (including the original) to check against known roots.
 */
export function normalizeWeakRoot(root: string): string[] {
  if (!root || root.length < 3) return [root];

  const candidates = [root];

  // Hollow roots: middle ا might be و or ي (e.g., قال→قول or قيل→قيل)
  if (root[1] === "ا") {
    candidates.push(root[0] + "و" + root.slice(2));
    candidates.push(root[0] + "ي" + root.slice(2));
  }

  // Hollow roots: middle ي might be و (e.g., قيم→قوم in يستقيم)
  if (root[1] === "ي") {
    candidates.push(root[0] + "و" + root.slice(2));
  }

  // Hollow roots: middle و might be ي (less common, but covers cases like حول/حيل)
  if (root[1] === "و") {
    candidates.push(root[0] + "ي" + root.slice(2));
  }

  // Defective roots: final ا or ي might be و (e.g., دعا→دعو, رمي→رمي)
  if (root.endsWith("ا")) {
    candidates.push(root.slice(0, -1) + "و");
    candidates.push(root.slice(0, -1) + "ي");
  }
  if (root.endsWith("ي")) {
    candidates.push(root.slice(0, -1) + "و");
  }

  // Assimilated roots: initial ا might be و (e.g., اعد→وعد)
  if (root[0] === "ا") {
    candidates.push("و" + root.slice(1));
    candidates.push("ي" + root.slice(1));
  }

  // Doubled roots: 2-letter result might need doubling (e.g., مد→مدد)
  if (root.length === 2) {
    candidates.push(root + root[1]);
  }
  // 3-letter with last two identical may be a doubled root (e.g., شدد, مرر)
  if (root.length === 3 && root[1] === root[2]) {
    // Already correct form — just keep it
  }

  return [...new Set(candidates)];
}

type RootConfidence = "high" | "medium" | "low";

interface RootResolution {
  root: string;
  confidence: RootConfidence;
  tier: "direct" | "stripped" | "stemmed" | "verb_stem" | "pattern" | "stem_pattern";
}

/**
 * Check all weak-root candidates against known roots, returning all valid ones.
 */
async function validateWeakCandidates(
  extracted: string,
  rootExists: (r: string) => Promise<boolean>,
): Promise<string[]> {
  const weakCandidates = normalizeWeakRoot(extracted);
  const results: string[] = [];
  for (const wc of weakCandidates) {
    if (await rootExists(wc)) {
      results.push(wc);
    }
  }
  return results;
}

/**
 * Multi-tier root resolution: resolves any Arabic word to its root(s).
 *
 * Tier 1: Direct lookup — normalized word in root table          [confidence: high]
 * Tier 2: Strip article — strip ال etc, then lookup              [confidence: high]
 * Tier 3: Stem + lookup — light stemming, then lookup            [confidence: medium]
 * Tier 3b: Verb stem — strip imperfect affixes, then lookup+pattern [confidence: medium]
 * Tier 4: Pattern extract — extractArabicRoot(), validate        [confidence: medium]
 * Tier 5: Stem + pattern — stem first, then extract, validate   [confidence: low]
 *
 * `rootLookup` checks if a word exists in the root table and returns matching roots.
 * `rootExists` checks if a given root string is a known root (dictionary headword or root table).
 */
export async function resolveRoot(
  word: string,
  rootLookup: (w: string) => Promise<string[]>,
  rootExists: (r: string) => Promise<boolean>,
): Promise<RootResolution[]> {
  const normalized = normalizeArabic(word);
  // Hamza-normalized variant: ؤ ئ ء → ا (only used as fallback candidate)
  const hamzaNorm = normalizeHamza(normalized);

  // Tier 1: Direct lookup
  const directRoots = await rootLookup(normalized);
  if (directRoots.length > 0) {
    return directRoots.map((r) => ({ root: r, confidence: "high" as const, tier: "direct" as const }));
  }

  // Tier 2: Strip article
  const stripped = stripDefiniteArticle(word);
  if (stripped !== normalized) {
    const strippedRoots = await rootLookup(stripped);
    if (strippedRoots.length > 0) {
      return strippedRoots.map((r) => ({ root: r, confidence: "high" as const, tier: "stripped" as const }));
    }
  }

  // Determine if word has hamza variants (ؤ ئ ء) — these need special handling
  const hasHamza = hamzaNorm !== normalized;

  // Tier 3: Stem + verb-stem + lookup
  // For hamza words (ؤ ئ ء), run verb_stem on hamza-normalized form FIRST
  // to avoid false DB matches (e.g., تؤمن→تمن instead of correct تؤمنون→امن)
  const stemmed = stemArabic(word);

  if (hasHamza) {
    // Try verb_stem on hamza-normalized first — most reliable for hamza conjugations
    for (const vs of stripImperfectAffixes(hamzaNorm)) {
      // For 3-letter candidates, check if they ARE a root directly (avoids noisy word→root mappings)
      if (vs.length === 3) {
        const weakRoots = await validateWeakCandidates(vs, rootExists);
        if (weakRoots.length > 0) {
          return weakRoots.map((r) => ({ root: r, confidence: "medium" as const, tier: "verb_stem" as const }));
        }
      }
      const vsRoots = await rootLookup(vs);
      if (vsRoots.length > 0) {
        return vsRoots.map((r) => ({ root: r, confidence: "medium" as const, tier: "verb_stem" as const }));
      }
    }
    // Then try hamza-normalized stem
    const hamzaStemmed = stemArabic(hamzaNorm);
    if (hamzaStemmed !== normalized) {
      const hsRoots = await rootLookup(hamzaStemmed);
      if (hsRoots.length > 0) {
        return hsRoots.map((r) => ({ root: r, confidence: "medium" as const, tier: "stemmed" as const }));
      }
    }
  }

  // Basic stem lookup
  if (stemmed !== normalized && stemmed !== stripped) {
    const stemmedRoots = await rootLookup(stemmed);
    if (stemmedRoots.length > 0) {
      return stemmedRoots.map((r) => ({ root: r, confidence: "medium" as const, tier: "stemmed" as const }));
    }
  }

  // Tier 3b: Strip imperfect verb affixes, then try lookup + pattern extraction
  // Handles يستقيم→ستقيم→قيم→قوم, يكتبون→كتب, تقاتلون→قاتل, etc.
  const verbStemsSeen = new Set<string>();
  for (const vs of stripImperfectAffixes(normalized)) {
    if (verbStemsSeen.has(vs)) continue;
    verbStemsSeen.add(vs);
    // For 3-letter candidates, check if they ARE a root directly (avoids noisy word→root mappings)
    if (vs.length === 3) {
      const weakRoots = await validateWeakCandidates(vs, rootExists);
      if (weakRoots.length > 0) {
        return weakRoots.map((r) => ({ root: r, confidence: "medium" as const, tier: "verb_stem" as const }));
      }
    }
    const vsRoots = await rootLookup(vs);
    if (vsRoots.length > 0) {
      return vsRoots.map((r) => ({ root: r, confidence: "medium" as const, tier: "verb_stem" as const }));
    }
    const extracted = extractArabicRoot(vs);
    if (extracted) {
      const validated = await validateWeakCandidates(extracted, rootExists);
      if (validated.length > 0) {
        return validated.map((r) => ({ root: r, confidence: "medium" as const, tier: "verb_stem" as const }));
      }
    }
  }

  // Tier 4: Pattern extract → validate against known roots
  for (const candidate of [normalized, stripped, ...(hamzaNorm !== normalized ? [hamzaNorm] : [])]) {
    const extracted = extractArabicRoot(candidate);
    if (extracted) {
      const validated = await validateWeakCandidates(extracted, rootExists);
      if (validated.length > 0) {
        return validated.map((r) => ({ root: r, confidence: "medium" as const, tier: "pattern" as const }));
      }
    }
  }

  // Tier 5: Stem then pattern extract → validate
  if (stemmed !== normalized) {
    const extracted = extractArabicRoot(stemmed);
    if (extracted) {
      const validated = await validateWeakCandidates(extracted, rootExists);
      if (validated.length > 0) {
        return validated.map((r) => ({ root: r, confidence: "low" as const, tier: "stem_pattern" as const }));
      }
    }
  }

  // No match found — return best-effort pattern extraction without validation
  const bestGuess = extractArabicRoot(normalized) || extractArabicRoot(stripped) || extractArabicRoot(stemmed);
  if (bestGuess) {
    return [{ root: bestGuess, confidence: "low", tier: "pattern" }];
  }

  return [];
}

/**
 * Light normalization: normalizes alef variants, taa marbuta, alef maqsura
 * but does NOT strip tashkeel. Used for vocalized matching where diacritics matter.
 */
export function normalizeArabicLight(text: string): string {
  return text
    .replace(ALEF_VARIANTS_RE, "\u0627")  // أ إ آ ٱ → ا
    .replace(/\u0649/g, "\u064A")          // ى → ي
    .replace(/\u0629/g, "\u0647")          // ة → ه
    .trim();
}

/**
 * Check if a string contains Arabic tashkeel (diacritics).
 */
export function hasTashkeel(text: string): boolean {
  return TASHKEEL_RE.test(text);
}

/**
 * Build a regex that matches an Arabic term with optional tashkeel between letters.
 * e.g., "تحرير" → /ت[\u064B-\u065F\u0670]*ح[\u064B-\u065F\u0670]*ر[\u064B-\u065F\u0670]*ي[\u064B-\u065F\u0670]*ر/
 */
function buildTashkeelTolerantRegex(term: string): RegExp {
  const tashkeelOpt = "[\\u064B-\\u065F\\u0670]*";
  const escaped = [...term].map((ch) => ch.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  return new RegExp(escaped.join(tashkeelOpt));
}

/**
 * Extract a relevant excerpt from a dictionary definition based on a search word.
 * Searches for the word, its stem, and related forms in the definition text
 * (tolerating tashkeel), then returns ~300 chars centered on the best match.
 * Returns null if no relevant section found (caller should fall back to beginning).
 */
export function extractRelevantExcerpt(
  definitionPlain: string,
  searchWord: string,
  excerptLength = 300,
): string | null {
  const stem = stemArabic(searchWord);
  const stripped = stripDefiniteArticle(searchWord);

  // Search terms ordered by specificity (most specific first)
  const terms = [...new Set([stem, stripped, normalizeArabic(searchWord)])].filter(
    (t) => t.length >= 3,
  );

  for (const term of terms) {
    const re = buildTashkeelTolerantRegex(term);
    const match = re.exec(definitionPlain);
    if (match) {
      const idx = match.index;
      // Don't bother with excerpt if match is near the beginning
      if (idx < 40) return null;

      // Extract centered excerpt from the original text
      const halfLen = Math.floor(excerptLength / 2);
      let start = Math.max(0, idx - halfLen);
      let end = Math.min(definitionPlain.length, idx + halfLen);

      // Snap to word boundaries
      if (start > 0) {
        const spaceIdx = definitionPlain.indexOf(" ", start);
        if (spaceIdx >= 0 && spaceIdx < start + 30) start = spaceIdx + 1;
      }
      if (end < definitionPlain.length) {
        const spaceIdx = definitionPlain.lastIndexOf(" ", end);
        if (spaceIdx > end - 30) end = spaceIdx;
      }

      const excerpt = definitionPlain.slice(start, end);
      return (start > 0 ? "..." : "") + excerpt + (end < definitionPlain.length ? "..." : "");
    }
  }

  return null;
}
