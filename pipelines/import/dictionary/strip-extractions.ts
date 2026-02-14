/**
 * Strip "extracted" field from all batch JSON files.
 * Preserves raw page content for re-extraction.
 *
 * Usage:
 *   bun run pipelines/import/dictionary/strip-extractions.ts [--slug=muhit]
 */

import { readdirSync, readFileSync, writeFileSync } from "fs";
import { resolve } from "path";

const BATCH_DIR = resolve(import.meta.dir, "extraction-batches");

function parseArgs() {
  const args = process.argv.slice(2);
  let slug = "";
  for (const arg of args) {
    if (arg.startsWith("--slug=")) slug = arg.slice(7);
  }
  return { slug };
}

function main() {
  const { slug } = parseArgs();

  const slugs = slug
    ? [slug]
    : readdirSync(BATCH_DIR).filter((f) => {
        try { return readdirSync(resolve(BATCH_DIR, f)).some((ff) => ff.endsWith(".json")); }
        catch { return false; }
      });

  let totalFiles = 0;
  let strippedFiles = 0;

  for (const s of slugs) {
    const dir = resolve(BATCH_DIR, s);
    const files = readdirSync(dir).filter((f) => f.endsWith(".json")).sort();

    for (const file of files) {
      const path = resolve(dir, file);
      const data = JSON.parse(readFileSync(path, "utf-8"));
      totalFiles++;

      if (data.extracted) {
        delete data.extracted;
        writeFileSync(path, JSON.stringify(data, null, 2));
        strippedFiles++;
      }
    }
    console.log(`${s}: ${files.length} files, ${strippedFiles} stripped`);
    strippedFiles = 0;
  }

  console.log(`\nDone. Processed ${totalFiles} batch files.`);
}

main();
