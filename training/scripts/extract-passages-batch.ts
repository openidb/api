/**
 * Extract passages in batches for Claude-based generation
 *
 * Usage:
 *   bun run training/scripts/extract-passages-batch.ts --source=quran --batch=0 --size=50
 */

import * as fs from "fs";
import * as path from "path";

const DATA_DIR = path.join(__dirname, "../data");

interface TrainingPair {
  query: string;
  pos: string[];
  neg: string[];
}

function parseArgs() {
  const args = process.argv.slice(2);
  const options: Record<string, string> = {};
  for (const arg of args) {
    if (arg.startsWith("--")) {
      const [key, value] = arg.slice(2).split("=");
      options[key] = value;
    }
  }
  return {
    source: options["source"] || "quran",
    batch: parseInt(options["batch"]) || 0,
    size: parseInt(options["size"]) || 50,
  };
}

function loadPairs(source: string): TrainingPair[] {
  const file = path.join(DATA_DIR, `${source}_pairs.jsonl`);
  const content = fs.readFileSync(file, "utf-8");
  return content.trim().split("\n").filter(Boolean).map(line => JSON.parse(line));
}

const options = parseArgs();
const pairs = loadPairs(options.source);
const start = options.batch * options.size;
const end = Math.min(start + options.size, pairs.length);
const batch = pairs.slice(start, end);

console.log(`Source: ${options.source}`);
console.log(`Total pairs: ${pairs.length}`);
console.log(`Batch ${options.batch}: ${start}-${end} (${batch.length} passages)`);
console.log(`Remaining batches: ${Math.ceil((pairs.length - end) / options.size)}`);
console.log("");

// Output passages in a format easy for Claude to process
for (let i = 0; i < batch.length; i++) {
  const globalIdx = start + i;
  const p = batch[i];
  console.log(`[${globalIdx}]`);
  console.log(`AR: ${p.pos[0]}`);
  console.log(`EN: ${p.query}`);
  console.log("");
}
