/**
 * Generate Derived Arabic Forms from Known Roots
 *
 * For each known root in the database, applies Arabic morphological pattern
 * templates (verb forms I-X + noun patterns) to generate candidate derived words.
 * These generated forms improve dictionary lookup coverage.
 *
 * Usage:
 *   bun run pipelines/import/dictionary/generate-derived-forms.ts [--dry-run] [--limit=N]
 */

import "../../env";
import { prisma } from "../../../src/db";
import { normalizeArabic } from "../../../src/utils/arabic-text";

const BATCH_SIZE = 1000;

function parseArgs(): { dryRun: boolean; limit: number } {
  const args = process.argv.slice(2);
  let dryRun = false;
  let limit = 0;

  for (const arg of args) {
    if (arg === "--dry-run") dryRun = true;
    else if (arg.startsWith("--limit=")) limit = parseInt(arg.split("=")[1], 10);
  }

  return { dryRun, limit };
}

/**
 * Arabic morphological pattern templates.
 * Each takes a triliteral root (C1, C2, C3) and generates a derived form.
 */
interface PatternTemplate {
  name: string;          // Pattern name (Arabic wazn)
  nameEn: string;        // English description
  partOfSpeech: "verb" | "noun";
  generate: (c1: string, c2: string, c3: string) => string;
}

const VERB_PATTERNS: PatternTemplate[] = [
  // Form I: فَعَلَ (fa'ala)
  { name: "فعل", nameEn: "Form I", partOfSpeech: "verb",
    generate: (c1, c2, c3) => c1 + c2 + c3 },
  // Form II: فَعَّلَ (fa''ala) — after normalization: C₁C₂C₂C₃
  { name: "فعّل", nameEn: "Form II", partOfSpeech: "verb",
    generate: (c1, c2, c3) => c1 + c2 + c2 + c3 },
  // Form III: فَاعَلَ (faa'ala)
  { name: "فاعل", nameEn: "Form III verb", partOfSpeech: "verb",
    generate: (c1, c2, c3) => c1 + "ا" + c2 + c3 },
  // Form IV: أَفْعَلَ (af'ala)
  { name: "افعل", nameEn: "Form IV", partOfSpeech: "verb",
    generate: (c1, c2, c3) => "ا" + c1 + c2 + c3 },
  // Form V: تَفَعَّلَ (tafa''ala) — after normalization: تC₁C₂C₂C₃
  { name: "تفعّل", nameEn: "Form V", partOfSpeech: "verb",
    generate: (c1, c2, c3) => "ت" + c1 + c2 + c2 + c3 },
  // Form VI: تَفَاعَلَ (tafaa'ala)
  { name: "تفاعل", nameEn: "Form VI", partOfSpeech: "verb",
    generate: (c1, c2, c3) => "ت" + c1 + "ا" + c2 + c3 },
  // Form VII: اِنْفَعَلَ (infa'ala)
  { name: "انفعل", nameEn: "Form VII", partOfSpeech: "verb",
    generate: (c1, c2, c3) => "ا" + "ن" + c1 + c2 + c3 },
  // Form VIII: اِفْتَعَلَ (ifta'ala)
  { name: "افتعل", nameEn: "Form VIII", partOfSpeech: "verb",
    generate: (c1, c2, c3) => "ا" + c1 + "ت" + c2 + c3 },
  // Form IX: اِفْعَلَّ (if'alla) — rare, colors/defects
  { name: "افعلّ", nameEn: "Form IX", partOfSpeech: "verb",
    generate: (c1, c2, c3) => "ا" + c1 + c2 + c3 + c3 },
  // Form X: اِسْتَفْعَلَ (istaf'ala)
  { name: "استفعل", nameEn: "Form X", partOfSpeech: "verb",
    generate: (c1, c2, c3) => "ا" + "س" + "ت" + c1 + c2 + c3 },
];

const NOUN_PATTERNS: PatternTemplate[] = [
  // فَاعِل (faa'il) — active participle
  { name: "فاعل", nameEn: "active participle", partOfSpeech: "noun",
    generate: (c1, c2, c3) => c1 + "ا" + c2 + c3 },
  // مَفْعُول (maf'uul) — passive participle
  { name: "مفعول", nameEn: "passive participle", partOfSpeech: "noun",
    generate: (c1, c2, c3) => "م" + c1 + c2 + "و" + c3 },
  // فِعَال (fi'aal) — verbal noun
  { name: "فعال", nameEn: "verbal noun fi'aal", partOfSpeech: "noun",
    generate: (c1, c2, c3) => c1 + c2 + "ا" + c3 },
  // فِعَالَة (fi'aala) — profession/craft (after normalization ة→ه)
  { name: "فعاله", nameEn: "profession fi'aala", partOfSpeech: "noun",
    generate: (c1, c2, c3) => c1 + c2 + "ا" + c3 + "ه" },
  // مَفْعَل (maf'al) — place noun
  { name: "مفعل", nameEn: "place noun maf'al", partOfSpeech: "noun",
    generate: (c1, c2, c3) => "م" + c1 + c2 + c3 },
  // مَفْعَلَة (maf'ala) — place noun feminine
  { name: "مفعله", nameEn: "place noun maf'ala", partOfSpeech: "noun",
    generate: (c1, c2, c3) => "م" + c1 + c2 + c3 + "ه" },
  // تَفْعِيل (taf'iil) — Form II masdar
  { name: "تفعيل", nameEn: "Form II masdar", partOfSpeech: "noun",
    generate: (c1, c2, c3) => "ت" + c1 + c2 + "ي" + c3 },
  // فَعَّال (fa''aal) — intensive
  { name: "فعّال", nameEn: "intensive fa''aal", partOfSpeech: "noun",
    generate: (c1, c2, c3) => c1 + c2 + c2 + "ا" + c3 },
  // فَعِيل (fa'iil) — adjective
  { name: "فعيل", nameEn: "adjective fa'iil", partOfSpeech: "noun",
    generate: (c1, c2, c3) => c1 + c2 + "ي" + c3 },
  // فُعُول (fu'uul) — plural/masdar
  { name: "فعول", nameEn: "plural fu'uul", partOfSpeech: "noun",
    generate: (c1, c2, c3) => c1 + c2 + "و" + c3 },
  // مِفْعَال (mif'aal) — instrument
  { name: "مفعال", nameEn: "instrument mif'aal", partOfSpeech: "noun",
    generate: (c1, c2, c3) => "م" + c1 + c2 + "ا" + c3 },
  // مُفَعِّل (mufa''il) — Form II active participle
  { name: "مفعّل", nameEn: "Form II active participle", partOfSpeech: "noun",
    generate: (c1, c2, c3) => "م" + c1 + c2 + c2 + c3 },
  // مُسْتَفْعِل (mustaf'il) — Form X active participle
  { name: "مستفعل", nameEn: "Form X active participle", partOfSpeech: "noun",
    generate: (c1, c2, c3) => "م" + "س" + "ت" + c1 + c2 + c3 },
  // إِفْعَال (if'aal) — Form IV masdar
  { name: "افعال", nameEn: "Form IV masdar", partOfSpeech: "noun",
    generate: (c1, c2, c3) => "ا" + c1 + c2 + "ا" + c3 },
  // اِنْفِعَال (infi'aal) — Form VII masdar
  { name: "انفعال", nameEn: "Form VII masdar", partOfSpeech: "noun",
    generate: (c1, c2, c3) => "ا" + "ن" + c1 + c2 + "ا" + c3 },
  // اِفْتِعَال (ifti'aal) — Form VIII masdar
  { name: "افتعال", nameEn: "Form VIII masdar", partOfSpeech: "noun",
    generate: (c1, c2, c3) => "ا" + c1 + "ت" + c2 + "ا" + c3 },
  // اِسْتِفْعَال (istif'aal) — Form X masdar
  { name: "استفعال", nameEn: "Form X masdar", partOfSpeech: "noun",
    generate: (c1, c2, c3) => "ا" + "س" + "ت" + c1 + c2 + "ا" + c3 },
  // فُعْلَان (fu'laan) — verbal noun
  { name: "فعلان", nameEn: "verbal noun fu'laan", partOfSpeech: "noun",
    generate: (c1, c2, c3) => c1 + c2 + c3 + "ا" + "ن" },
  // فَعِيلَة (fa'iila) — feminine adjective
  { name: "فعيله", nameEn: "feminine adjective", partOfSpeech: "noun",
    generate: (c1, c2, c3) => c1 + c2 + "ي" + c3 + "ه" },
  // مَفْعُولَة (maf'uula) — feminine passive participle
  { name: "مفعوله", nameEn: "feminine passive participle", partOfSpeech: "noun",
    generate: (c1, c2, c3) => "م" + c1 + c2 + "و" + c3 + "ه" },
  // أَفْعَل (af'al) — elative/comparative
  { name: "افعل", nameEn: "elative af'al", partOfSpeech: "noun",
    generate: (c1, c2, c3) => "ا" + c1 + c2 + c3 },
];

// Imperfect verb patterns (يـ prefix, most common conjugation)
const IMPERFECT_PATTERNS: PatternTemplate[] = [
  // Form I imperfect: يَفْعَل / يَفْعِل / يَفْعُل
  { name: "يفعل", nameEn: "Form I imperfect", partOfSpeech: "verb",
    generate: (c1, c2, c3) => "ي" + c1 + c2 + c3 },
  // Form II imperfect: يُفَعِّل
  { name: "يفعّل", nameEn: "Form II imperfect", partOfSpeech: "verb",
    generate: (c1, c2, c3) => "ي" + c1 + c2 + c2 + c3 },
  // Form III imperfect: يُفَاعِل
  { name: "يفاعل", nameEn: "Form III imperfect", partOfSpeech: "verb",
    generate: (c1, c2, c3) => "ي" + c1 + "ا" + c2 + c3 },
  // Form IV imperfect: يُفْعِل
  { name: "يفعل٤", nameEn: "Form IV imperfect", partOfSpeech: "verb",
    generate: (c1, c2, c3) => "ي" + c1 + c2 + c3 }, // same shape as Form I after normalization
  // Form V imperfect: يَتَفَعَّل
  { name: "يتفعّل", nameEn: "Form V imperfect", partOfSpeech: "verb",
    generate: (c1, c2, c3) => "ي" + "ت" + c1 + c2 + c2 + c3 },
  // Form VI imperfect: يَتَفَاعَل
  { name: "يتفاعل", nameEn: "Form VI imperfect", partOfSpeech: "verb",
    generate: (c1, c2, c3) => "ي" + "ت" + c1 + "ا" + c2 + c3 },
  // Form VII imperfect: يَنْفَعِل
  { name: "ينفعل", nameEn: "Form VII imperfect", partOfSpeech: "verb",
    generate: (c1, c2, c3) => "ي" + "ن" + c1 + c2 + c3 },
  // Form VIII imperfect: يَفْتَعِل
  { name: "يفتعل", nameEn: "Form VIII imperfect", partOfSpeech: "verb",
    generate: (c1, c2, c3) => "ي" + c1 + "ت" + c2 + c3 },
  // Form X imperfect: يَسْتَفْعِل
  { name: "يستفعل", nameEn: "Form X imperfect", partOfSpeech: "verb",
    generate: (c1, c2, c3) => "ي" + "س" + "ت" + c1 + c2 + c3 },
];

const ALL_PATTERNS = [...VERB_PATTERNS, ...NOUN_PATTERNS, ...IMPERFECT_PATTERNS];

async function main() {
  const { dryRun, limit } = parseArgs();

  // Collect all distinct 3-letter roots
  console.log("Fetching distinct roots from database...");
  const rootRows = await prisma.arabicRoot.findMany({
    select: { root: true },
    distinct: ["root"],
  });

  // Also get dictionary headwords as roots
  const headwordRows = await prisma.dictionaryEntry.findMany({
    select: { rootNormalized: true },
    distinct: ["rootNormalized"],
  });

  const allRoots = new Set<string>();
  for (const r of rootRows) {
    const norm = normalizeArabic(r.root);
    if (norm.length === 3) allRoots.add(norm);
  }
  for (const r of headwordRows) {
    const norm = normalizeArabic(r.rootNormalized);
    if (norm.length === 3) allRoots.add(norm);
  }

  let roots = [...allRoots];
  if (limit > 0) roots = roots.slice(0, limit);

  console.log(`  Found ${allRoots.size} unique triliteral roots (processing ${roots.length})`);
  console.log(`  Patterns: ${ALL_PATTERNS.length} (${VERB_PATTERNS.length} verb + ${NOUN_PATTERNS.length} noun)`);
  console.log(`  Expected candidates: ~${roots.length * ALL_PATTERNS.length}`);

  // Get existing (word, root) pairs for dedup
  console.log("\nFetching existing arabic_roots entries for deduplication...");
  const existingPairs = new Set<string>();
  const existingRows = await prisma.arabicRoot.findMany({
    select: { word: true, root: true },
  });
  for (const r of existingRows) {
    existingPairs.add(`${r.word}|${r.root}`);
  }
  console.log(`  Existing entries: ${existingPairs.size}`);

  // Generate derived forms
  console.log("\nGenerating derived forms...");
  const records: Array<{ word: string; root: string; pattern: string; wordType: string; partOfSpeech: string }> = [];

  for (const root of roots) {
    const [c1, c2, c3] = root;

    for (const pat of ALL_PATTERNS) {
      const derived = normalizeArabic(pat.generate(c1, c2, c3));
      if (!derived || derived.length < 3) continue;

      const key = `${derived}|${root}`;
      if (existingPairs.has(key)) continue;

      records.push({
        word: derived,
        root,
        pattern: pat.name,
        wordType: pat.nameEn,
        partOfSpeech: pat.partOfSpeech,
      });
      existingPairs.add(key); // prevent self-duplicates
    }
  }

  console.log(`  New derived forms to insert: ${records.length}`);

  if (dryRun) {
    console.log("\n[DRY RUN] Sample entries (first 30):");
    for (const r of records.slice(0, 30)) {
      console.log(`  ${r.word} → ${r.root} [${r.pattern}] (${r.wordType})`);
    }

    // Show per-pattern stats
    const patternCounts = new Map<string, number>();
    for (const r of records) {
      patternCounts.set(r.pattern, (patternCounts.get(r.pattern) || 0) + 1);
    }
    console.log("\nPer-pattern counts:");
    for (const [p, c] of [...patternCounts.entries()].sort((a, b) => b[1] - a[1])) {
      console.log(`  ${p}: ${c}`);
    }
    console.log("\nDry run complete — no data written.");
    return;
  }

  // Batch insert
  console.log("\nInserting generated forms...");
  let inserted = 0;
  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const batch = records.slice(i, i + BATCH_SIZE);
    const result = await prisma.arabicRoot.createMany({
      data: batch.map((r) => ({
        word: r.word,
        root: r.root,
        pattern: r.pattern,
        wordType: r.wordType,
        partOfSpeech: r.partOfSpeech,
        source: "generated",
      })),
      skipDuplicates: true,
    });
    inserted += result.count;

    if ((i / BATCH_SIZE) % 20 === 0) {
      console.log(`  Batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(records.length / BATCH_SIZE)} — ${inserted} inserted so far`);
    }
  }

  console.log(`\nDone! Inserted ${inserted} generated derived forms.`);
  const total = await prisma.arabicRoot.count();
  console.log(`Total arabic_roots records: ${total}`);
  await prisma.$disconnect();
}

main().catch((err) => {
  console.error("Generation failed:", err);
  process.exit(1);
});
