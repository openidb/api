/**
 * Setup Qdrant collections for benchmark techniques.
 *
 * Creates one Quran + one Hadith collection per technique (3072d cosine).
 *
 * Usage:
 *   bun run scripts/benchmark-techniques/setup-collections.ts [--techniques=baseline,stopword,...] [--force]
 */

import "../env";
import { qdrant } from "../../src/qdrant";
import { GEMINI_DIMENSIONS } from "../../src/constants";
import { getTechniques, getCollectionNames } from "./registry";

// Parse args
const args = process.argv.slice(2);
const forceFlag = args.includes("--force");
const techniquesArg = args.find((a) => a.startsWith("--techniques="));
const techniqueIds = techniquesArg?.split("=")[1]?.split(",");

async function main() {
  const techniques = getTechniques(techniqueIds);

  console.log("=".repeat(60));
  console.log("Benchmark Collections Setup");
  console.log("=".repeat(60));
  console.log(`Techniques: ${techniques.map((t) => t.id).join(", ")}`);
  console.log(`Dimensions: ${GEMINI_DIMENSIONS}`);
  console.log(`Force recreate: ${forceFlag}`);
  console.log();

  const existing = await qdrant.getCollections();
  const existingNames = new Set(existing.collections.map((c) => c.name));

  let created = 0;
  let skipped = 0;

  for (const technique of techniques) {
    const { quran, hadith } = getCollectionNames(technique.id);

    for (const collectionName of [quran, hadith]) {
      const exists = existingNames.has(collectionName);

      if (exists && !forceFlag) {
        console.log(`  SKIP ${collectionName} (already exists)`);
        skipped++;
        continue;
      }

      if (exists && forceFlag) {
        console.log(`  DELETE ${collectionName}`);
        await qdrant.deleteCollection(collectionName);
      }

      console.log(`  CREATE ${collectionName}`);
      await qdrant.createCollection(collectionName, {
        vectors: {
          size: GEMINI_DIMENSIONS,
          distance: "Cosine",
        },
        optimizers_config: {
          indexing_threshold: 10000,
        },
      });

      // Create payload indexes for filtering and ID extraction
      const isQuran = collectionName.includes("quran");
      if (isQuran) {
        await qdrant.createPayloadIndex(collectionName, {
          field_name: "surahNumber",
          field_schema: "integer",
        });
        await qdrant.createPayloadIndex(collectionName, {
          field_name: "ayahNumber",
          field_schema: "integer",
        });
      } else {
        await qdrant.createPayloadIndex(collectionName, {
          field_name: "collectionSlug",
          field_schema: "keyword",
        });
        await qdrant.createPayloadIndex(collectionName, {
          field_name: "hadithNumber",
          field_schema: "keyword",
        });
      }

      created++;
    }
  }

  console.log();
  console.log(`Created: ${created}, Skipped: ${skipped}`);
  console.log("Done!");
}

main().catch((e) => {
  console.error("Setup failed:", e);
  process.exit(1);
});
