/**
 * Update Author Fields
 *
 * CLI utility to update structured fields on an author record.
 * Designed to be called by Claude subagents during batch enrichment.
 *
 * Usage:
 *   bun run pipelines/import/update-author-fields.ts --id=100 --fields='{"kunya":"أبو عبد الله","nameLatin":"Al-Shawkani"}'
 */

import "../env";
import { prisma } from "../../src/db";

const ALLOWED_FIELDS = new Set([
  "nameLatin",
  "kunya",
  "nasab",
  "nisba",
  "laqab",
  "birthDateHijri",
  "deathDateHijri",
  "birthDateGregorian",
  "deathDateGregorian",
]);

function parseArgs(): { id: string; fields: Record<string, string> } {
  const args = process.argv.slice(2);
  let id = "";
  let fieldsRaw = "";

  for (const arg of args) {
    if (arg.startsWith("--id=")) {
      id = arg.slice(5);
    } else if (arg.startsWith("--fields=")) {
      fieldsRaw = arg.slice(9);
    }
  }

  if (!id || !fieldsRaw) {
    console.error(
      "Usage: bun run pipelines/import/update-author-fields.ts --id=<author_id> --fields='{...}'",
    );
    process.exit(1);
  }

  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(fieldsRaw);
  } catch {
    console.error("Error: --fields must be valid JSON");
    process.exit(1);
  }

  // Filter to allowed fields only, skip nulls
  const fields: Record<string, string> = {};
  for (const [key, value] of Object.entries(parsed)) {
    if (!ALLOWED_FIELDS.has(key)) {
      console.warn(`  Skipping unknown field: ${key}`);
      continue;
    }
    if (value === null || value === undefined || value === "") continue;
    fields[key] = String(value);
  }

  return { id, fields };
}

async function main() {
  const { id, fields } = parseArgs();

  if (Object.keys(fields).length === 0) {
    console.log(`Author ${id}: no fields to update, skipping.`);
    process.exit(0);
  }

  const author = await prisma.author.findUnique({ where: { id } });
  if (!author) {
    console.error(`Author ${id}: not found in database.`);
    process.exit(1);
  }

  await prisma.author.update({
    where: { id },
    data: fields,
  });

  const fieldNames = Object.keys(fields).join(", ");
  console.log(`Author ${id} (${author.nameArabic}): updated ${fieldNames}`);
}

main()
  .catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
  })
  .finally(() => prisma.$disconnect());
