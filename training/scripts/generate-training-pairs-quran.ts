/**
 * Generate Training Pairs from Quran (Arabic Ayah + English Translation)
 *
 * Creates training pairs for fine-tuning BGE-M3:
 * - Query: English translation text
 * - Positive: Original Arabic ayah text
 *
 * Output format (JSONL):
 * {"query": "english translation...", "pos": ["arabic ayah..."], "neg": []}
 *
 * Usage: bun run training/scripts/generate-training-pairs-quran.ts
 */

import "dotenv/config";
import { prisma } from "../../lib/db";
import * as fs from "fs";
import * as path from "path";

const OUTPUT_FILE = path.join(__dirname, "../data/quran_pairs.jsonl");

interface TrainingPair {
  query: string;
  pos: string[];
  neg: string[];
}

async function main() {
  console.log("Generating Quran training pairs...");
  console.log(`Output file: ${OUTPUT_FILE}`);
  console.log();

  // Fetch all ayahs
  const ayahs = await prisma.ayah.findMany({
    select: {
      ayahNumber: true,
      textUthmani: true,
      textPlain: true,
      surah: {
        select: {
          number: true,
          nameArabic: true,
        },
      },
    },
    orderBy: [{ surahId: "asc" }, { ayahNumber: "asc" }],
  });

  console.log(`Found ${ayahs.length} ayahs`);

  // Fetch English translations (use first available edition)
  const translations = await prisma.ayahTranslation.findMany({
    where: { language: "en" },
    select: {
      surahNumber: true,
      ayahNumber: true,
      text: true,
    },
    distinct: ["surahNumber", "ayahNumber"],
  });

  console.log(`Found ${translations.length} English translations`);

  // Create translation lookup map
  const translationMap = new Map<string, string>();
  for (const t of translations) {
    const key = `${t.surahNumber}:${t.ayahNumber}`;
    // Only keep first translation for each ayah
    if (!translationMap.has(key)) {
      translationMap.set(key, t.text);
    }
  }

  // Generate training pairs
  const pairs: TrainingPair[] = [];
  let matched = 0;
  let skipped = 0;

  for (const ayah of ayahs) {
    const key = `${ayah.surah.number}:${ayah.ayahNumber}`;
    const translation = translationMap.get(key);

    if (!translation) {
      skipped++;
      continue;
    }

    // Skip very short texts (less useful for training)
    if (ayah.textPlain.length < 20 || translation.length < 20) {
      skipped++;
      continue;
    }

    pairs.push({
      query: translation.trim(),
      pos: [ayah.textPlain.trim()],
      neg: [], // Negatives can be added later via hard negative mining
    });
    matched++;
  }

  console.log(`Generated ${matched} training pairs (skipped: ${skipped})`);

  // Write to JSONL file
  const outputDir = path.dirname(OUTPUT_FILE);
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  const lines = pairs.map((p) => JSON.stringify(p));
  fs.writeFileSync(OUTPUT_FILE, lines.join("\n") + "\n");

  console.log(`Written to ${OUTPUT_FILE}`);
  console.log();

  // Sample output
  console.log("Sample pairs:");
  console.log("-".repeat(60));
  for (let i = 0; i < Math.min(3, pairs.length); i++) {
    console.log(`Query (EN): ${pairs[i].query.substring(0, 100)}...`);
    console.log(`Pos (AR): ${pairs[i].pos[0].substring(0, 100)}...`);
    console.log();
  }
}

main()
  .catch((e) => {
    console.error("Failed:", e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
