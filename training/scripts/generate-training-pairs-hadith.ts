/**
 * Generate Training Pairs from Hadith (Arabic + English Translation)
 *
 * Creates training pairs for fine-tuning BGE-M3:
 * - Query: English translation text
 * - Positive: Original Arabic hadith text
 *
 * Output format (JSONL):
 * {"query": "english translation...", "pos": ["arabic hadith..."], "neg": []}
 *
 * Usage: bun run training/scripts/generate-training-pairs-hadith.ts
 */

import "dotenv/config";
import { prisma } from "../../lib/db";
import * as fs from "fs";
import * as path from "path";

const OUTPUT_FILE = path.join(__dirname, "../data/hadith_pairs.jsonl");

interface TrainingPair {
  query: string;
  pos: string[];
  neg: string[];
}

async function main() {
  console.log("Generating Hadith training pairs...");
  console.log(`Output file: ${OUTPUT_FILE}`);
  console.log();

  // Fetch all hadiths with book info
  const hadiths = await prisma.hadith.findMany({
    select: {
      id: true,
      bookId: true,
      hadithNumber: true,
      textArabic: true,
      textPlain: true,
      book: {
        select: {
          bookNumber: true,
          nameEnglish: true,
          collection: {
            select: {
              slug: true,
              nameEnglish: true,
            },
          },
        },
      },
    },
    where: {
      // Only include hadiths that have text
      textPlain: {
        not: "",
      },
    },
    orderBy: [{ bookId: "asc" }, { hadithNumber: "asc" }],
  });

  console.log(`Found ${hadiths.length} hadiths`);

  // Fetch all translations
  const translations = await prisma.hadithTranslation.findMany({
    where: {
      language: "en",
    },
    select: {
      bookId: true,
      hadithNumber: true,
      text: true,
    },
  });

  console.log(`Found ${translations.length} English translations`);

  // Create translation lookup map using bookId + hadithNumber as key
  const translationMap = new Map<string, string>();
  for (const t of translations) {
    const key = `${t.bookId}:${t.hadithNumber}`;
    translationMap.set(key, t.text);
  }

  // Generate training pairs
  const pairs: TrainingPair[] = [];
  let matched = 0;
  let skipped = 0;

  for (const hadith of hadiths) {
    const key = `${hadith.bookId}:${hadith.hadithNumber}`;
    const translation = translationMap.get(key);

    if (!translation) {
      skipped++;
      continue;
    }

    // Skip very short texts (less useful for training)
    if (hadith.textPlain.length < 30 || translation.length < 30) {
      skipped++;
      continue;
    }

    pairs.push({
      query: translation.trim(),
      pos: [hadith.textPlain.trim()],
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
