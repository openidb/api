/**
 * Import Quran Translations Script
 *
 * Fetches translations from fawazahmed0/quran-api via jsDelivr CDN
 * and imports them into the database.
 *
 * Data Source: https://github.com/fawazahmed0/quran-api
 * CDN: https://cdn.jsdelivr.net/gh/fawazahmed0/quran-api@1/editions/{edition-id}.json
 *
 * Usage: bun run scripts/import-quran-translations.ts [--force] [--lang=en,ur,fr]
 */

import "../env";
import { prisma } from "../../src/db";

// Translation editions to import (12 languages matching app UI languages, excluding Arabic)
const TRANSLATIONS: { lang: string; edition: string; name: string }[] = [
  { lang: "en", edition: "eng-mustafakhattaba", name: "Dr. Mustafa Khattab (The Clear Quran)" },
  { lang: "fr", edition: "fra-muhammadhameedu", name: "Muhammad Hamidullah" },
  { lang: "id", edition: "ind-indonesianislam", name: "Indonesian Islamic Ministry" },
  { lang: "ur", edition: "urd-fatehmuhammadja", name: "Fateh Muhammad Jalandhry" },
  { lang: "es", edition: "spa-muhammadisagarc", name: "Isa Garcia" },
  { lang: "zh", edition: "zho-majian", name: "Ma Jian" },
  { lang: "pt", edition: "por-samirelhayek", name: "Samir El-Hayek" },
  { lang: "ru", edition: "rus-elmirkuliev", name: "Elmir Kuliev" },
  { lang: "ja", edition: "jpn-ryoichimita", name: "Ryoichi Mita" },
  { lang: "ko", edition: "kor-hamidchoi", name: "Hamid Choi" },
  { lang: "it", edition: "ita-hamzarobertopic", name: "Hamza Roberto Piccardo" },
  { lang: "bn", edition: "ben-muhiuddinkhan", name: "Muhiuddin Khan" },
];

// API response type from fawazahmed0/quran-api
interface QuranApiResponse {
  quran: Array<{
    chapter: number;
    verse: number;
    text: string;
  }>;
}

/**
 * Strip HTML tags from translation text (some editions have <sup> tags for footnotes)
 */
function stripHtmlTags(text: string): string {
  return text
    .replace(/<[^>]+>/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Fetch translation from jsDelivr CDN
 */
async function fetchTranslation(edition: string): Promise<QuranApiResponse | null> {
  const url = `https://cdn.jsdelivr.net/gh/fawazahmed0/quran-api@1/editions/${edition}.json`;

  try {
    const response = await fetch(url);
    if (!response.ok) {
      console.error(`Failed to fetch ${edition}: ${response.status} ${response.statusText}`);
      return null;
    }
    return await response.json() as QuranApiResponse;
  } catch (error) {
    console.error(`Error fetching ${edition}:`, error);
    return null;
  }
}

/**
 * Import a single translation into the database
 */
async function importTranslation(
  lang: string,
  edition: string,
  name: string,
  force: boolean
): Promise<{ imported: number; skipped: number }> {
  console.log(`\n📖 Importing ${name} (${lang})...`);

  // Check if translation already exists (if not forcing)
  if (!force) {
    const existingCount = await prisma.ayahTranslation.count({
      where: { language: lang },
    });
    if (existingCount > 0) {
      console.log(`   ⏭️  Already imported (${existingCount} ayahs). Use --force to re-import.`);
      return { imported: 0, skipped: existingCount };
    }
  }

  // Fetch translation from CDN
  const data = await fetchTranslation(edition);
  if (!data || !data.quran) {
    console.error(`   ❌ Failed to fetch translation data`);
    return { imported: 0, skipped: 0 };
  }

  // Delete existing translations for this language (if forcing)
  if (force) {
    const deleted = await prisma.ayahTranslation.deleteMany({
      where: { language: lang },
    });
    if (deleted.count > 0) {
      console.log(`   🗑️  Deleted ${deleted.count} existing translations`);
    }
  }

  // Prepare upsert data
  const translations = data.quran.map((ayah) => ({
    surahNumber: ayah.chapter,
    ayahNumber: ayah.verse,
    language: lang,
    editionId: edition,
    text: stripHtmlTags(ayah.text),
  }));

  // Batch insert using createMany for performance
  const BATCH_SIZE = 1000;
  let imported = 0;

  for (let i = 0; i < translations.length; i += BATCH_SIZE) {
    const batch = translations.slice(i, i + BATCH_SIZE);
    await prisma.ayahTranslation.createMany({
      data: batch,
      skipDuplicates: true,
    });
    imported += batch.length;
    process.stdout.write(`   📝 Imported ${imported}/${translations.length} ayahs\r`);
  }

  console.log(`   ✅ Imported ${imported} ayahs`);
  return { imported, skipped: 0 };
}

/**
 * Main import function
 */
async function main() {
  const args = process.argv.slice(2);
  const force = args.includes("--force");

  // Parse --lang argument for selective import
  const langArg = args.find((arg) => arg.startsWith("--lang="));
  const selectedLangs = langArg
    ? langArg.split("=")[1].split(",").map((l) => l.trim())
    : null;

  console.log("🕌 Quran Translations Import Script");
  console.log("=====================================");
  console.log(`Force mode: ${force ? "ON" : "OFF"}`);
  if (selectedLangs) {
    console.log(`Selected languages: ${selectedLangs.join(", ")}`);
  }

  // Filter translations if specific languages requested
  const translationsToImport = selectedLangs
    ? TRANSLATIONS.filter((t) => selectedLangs.includes(t.lang))
    : TRANSLATIONS;

  if (translationsToImport.length === 0) {
    console.error("No valid translations found for the specified languages.");
    process.exit(1);
  }

  let totalImported = 0;
  let totalSkipped = 0;

  for (const { lang, edition, name } of translationsToImport) {
    const { imported, skipped } = await importTranslation(lang, edition, name, force);
    totalImported += imported;
    totalSkipped += skipped;
  }

  console.log("\n=====================================");
  console.log(`📊 Summary:`);
  console.log(`   Imported: ${totalImported} ayahs`);
  console.log(`   Skipped: ${totalSkipped} ayahs`);
  console.log("✅ Done!");
}

main()
  .catch((error) => {
    console.error("Fatal error:", error);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
