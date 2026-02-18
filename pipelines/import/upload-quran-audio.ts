/**
 * Upload Quran audio files to RustFS (S3-compatible storage).
 *
 * Usage:
 *   bun run pipelines/import/upload-quran-audio.ts --reciter=alafasy-128kbps --audio-dir=/path/to/dir
 *   bun run pipelines/import/upload-quran-audio.ts --all
 *
 * Options:
 *   --reciter=<slug>   Reciter folder name (used as S3 prefix)
 *   --audio-dir=<path> Audio directory for single reciter
 *   --all              Upload all reciters found under $QURAN_AUDIO_PATH
 *   --skip-existing    Skip reciters already in RustFS (checks first file)
 *   --delete-after     Delete local files after successful upload
 */

import { S3Client, PutObjectCommand, CreateBucketCommand, HeadBucketCommand, HeadObjectCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";
import { readdir, readFile, rm } from "fs/promises";
import { join } from "path";

const args = process.argv.slice(2);
function getArg(name: string): string | undefined {
  const arg = args.find((a) => a.startsWith(`--${name}=`));
  return arg?.split("=")[1];
}
function hasFlag(name: string): boolean {
  return args.includes(`--${name}`);
}

const BASE_PATH = process.env.QURAN_AUDIO_PATH || "/Volumes/KIOXIA/quran-audio";
const BUCKET = "quran-audio";
const CONCURRENCY = 20;
const ALL_MODE = hasFlag("all");
const SKIP_EXISTING = hasFlag("skip-existing");
const DELETE_AFTER = hasFlag("delete-after");

const s3 = new S3Client({
  endpoint: process.env.RUSTFS_ENDPOINT || "http://localhost:9000",
  region: "us-east-1",
  credentials: {
    accessKeyId: process.env.RUSTFS_ACCESS_KEY || "openidb_access",
    secretAccessKey: process.env.RUSTFS_SECRET_KEY || "openidb_secret_change_me",
  },
  forcePathStyle: true,
});

async function ensureBucket() {
  try {
    await s3.send(new HeadBucketCommand({ Bucket: BUCKET }));
  } catch {
    console.log(`Creating bucket "${BUCKET}"...`);
    await s3.send(new CreateBucketCommand({ Bucket: BUCKET }));
  }
}

async function countS3Objects(prefix: string): Promise<number> {
  try {
    const res = await s3.send(new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: `${prefix}/`,
      MaxKeys: 1,
    }));
    return res.KeyCount || 0;
  } catch {
    return 0;
  }
}

async function uploadReciter(reciterName: string, audioDir: string): Promise<{ uploaded: number; failed: number; skipped: boolean }> {
  // Check if already uploaded
  if (SKIP_EXISTING) {
    const existing = await countS3Objects(reciterName);
    if (existing > 0) {
      return { uploaded: 0, failed: 0, skipped: true };
    }
  }

  let files: string[];
  try {
    files = (await readdir(audioDir)).filter(f => f.endsWith(".mp3")).sort();
  } catch {
    console.error(`  Directory not found: ${audioDir}`);
    return { uploaded: 0, failed: 0, skipped: false };
  }

  if (files.length === 0) {
    return { uploaded: 0, failed: 0, skipped: false };
  }

  let uploaded = 0;
  let failed = 0;

  for (let i = 0; i < files.length; i += CONCURRENCY) {
    const batch = files.slice(i, i + CONCURRENCY);
    const results = await Promise.allSettled(batch.map(async (filename) => {
      const filePath = join(audioDir, filename);
      const key = `${reciterName}/${filename}`;
      const body = await readFile(filePath);
      await s3.send(new PutObjectCommand({
        Bucket: BUCKET,
        Key: key,
        Body: body,
        ContentType: "audio/mpeg",
      }));
    }));
    for (const r of results) {
      if (r.status === "fulfilled") uploaded++;
      else { failed++; console.error("  Failed:", r.reason); }
    }
    if (uploaded % 500 === 0 || i + CONCURRENCY >= files.length) {
      console.log(`    ${uploaded}/${files.length} uploaded, ${failed} failed`);
    }
  }

  if (DELETE_AFTER && failed === 0) {
    console.log(`  Deleting local files: ${audioDir}`);
    await rm(audioDir, { recursive: true, force: true });
  }

  return { uploaded, failed, skipped: false };
}

async function discoverReciters(): Promise<Array<{ name: string; dir: string }>> {
  const sources = ["everyayah", "alquran-cloud", "quran-foundation", "tarteel", "tarteel-surah"];
  const reciters: Array<{ name: string; dir: string }> = [];

  for (const source of sources) {
    const sourceDir = join(BASE_PATH, source);
    try {
      const entries = await readdir(sourceDir);
      for (const entry of entries) {
        const reciterDir = join(sourceDir, entry);
        try {
          const files = await readdir(reciterDir);
          const mp3Count = files.filter(f => f.endsWith(".mp3")).length;
          if (mp3Count > 0) {
            reciters.push({ name: entry, dir: reciterDir });
          }
        } catch { /* not a directory */ }
      }
    } catch { /* source dir doesn't exist */ }
  }

  return reciters;
}

async function main() {
  await ensureBucket();

  if (ALL_MODE) {
    console.log(`Discovering reciters under ${BASE_PATH}...`);
    const reciters = await discoverReciters();
    console.log(`Found ${reciters.length} reciters with audio files\n`);

    let totalUploaded = 0;
    let totalFailed = 0;
    let totalSkipped = 0;

    for (let i = 0; i < reciters.length; i++) {
      const { name, dir } = reciters[i];
      process.stdout.write(`[${i + 1}/${reciters.length}] ${name}...`);

      const result = await uploadReciter(name, dir);
      if (result.skipped) {
        console.log(" already in RustFS, skipping");
        totalSkipped++;
      } else if (result.uploaded === 0 && result.failed === 0) {
        console.log(" empty, skipping");
      } else {
        console.log(` ${result.uploaded} uploaded, ${result.failed} failed`);
        totalUploaded += result.uploaded;
        totalFailed += result.failed;
      }
    }

    console.log(`\n=== Summary ===`);
    console.log(`Reciters processed: ${reciters.length} (${totalSkipped} skipped)`);
    console.log(`Files uploaded: ${totalUploaded.toLocaleString()}`);
    console.log(`Files failed: ${totalFailed.toLocaleString()}`);
  } else {
    const reciterName = getArg("reciter") || "alafasy-128kbps";
    const audioDir = getArg("audio-dir") || join(BASE_PATH, "everyayah", reciterName);

    console.log(`Reciter: ${reciterName}`);
    console.log(`Audio dir: ${audioDir}`);
    console.log(`S3 prefix: ${reciterName}/`);

    const result = await uploadReciter(reciterName, audioDir);
    console.log(`Done: ${result.uploaded} uploaded, ${result.failed} failed`);
  }
}

main().catch(console.error);
