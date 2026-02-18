/**
 * Upload Quran audio files to RustFS (S3-compatible storage).
 *
 * Usage:
 *   bun run pipelines/import/upload-quran-audio.ts --reciter=alafasy-128kbps
 *   bun run pipelines/import/upload-quran-audio.ts --reciter=maher-almuaiqly-128kbps
 *
 * Options:
 *   --reciter=<slug>   Reciter folder name (used as S3 prefix and dir name)
 *   --audio-dir=<path> Override audio directory (default: $QURAN_AUDIO_PATH/everyayah/<reciter>)
 */

import { S3Client, PutObjectCommand, CreateBucketCommand, HeadBucketCommand } from "@aws-sdk/client-s3";
import { readdir, readFile } from "fs/promises";
import { join } from "path";

const args = process.argv.slice(2);
function getArg(name: string): string | undefined {
  const arg = args.find((a) => a.startsWith(`--${name}=`));
  return arg?.split("=")[1];
}

const RECITER = getArg("reciter") || "alafasy-128kbps";
const BASE_PATH = process.env.QURAN_AUDIO_PATH || process.env.AUDIO_DIR || "/Volumes/KIOXIA/quran-audio";
const AUDIO_DIR = getArg("audio-dir") || join(BASE_PATH, "everyayah", RECITER);
const BUCKET = "quran-audio";
const PREFIX = RECITER;
const CONCURRENCY = 20;

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
    console.log(`Bucket "${BUCKET}" exists`);
  } catch {
    console.log(`Creating bucket "${BUCKET}"...`);
    await s3.send(new CreateBucketCommand({ Bucket: BUCKET }));
    console.log(`Bucket "${BUCKET}" created`);
  }
}

async function uploadFile(filename: string): Promise<void> {
  const filePath = join(AUDIO_DIR, filename);
  const key = `${PREFIX}/${filename}`;
  const body = await readFile(filePath);
  await s3.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: key,
    Body: body,
    ContentType: "audio/mpeg",
  }));
}

async function main() {
  console.log(`Reciter: ${RECITER}`);
  console.log(`Audio dir: ${AUDIO_DIR}`);
  console.log(`S3 prefix: ${PREFIX}/`);

  await ensureBucket();

  const files = (await readdir(AUDIO_DIR)).filter(f => f.endsWith(".mp3")).sort();
  console.log(`Found ${files.length} mp3 files to upload`);

  let uploaded = 0;
  let failed = 0;

  for (let i = 0; i < files.length; i += CONCURRENCY) {
    const batch = files.slice(i, i + CONCURRENCY);
    const results = await Promise.allSettled(batch.map(f => uploadFile(f)));
    for (const r of results) {
      if (r.status === "fulfilled") uploaded++;
      else { failed++; console.error("Failed:", r.reason); }
    }
    if (uploaded % 200 === 0 || i + CONCURRENCY >= files.length) {
      console.log(`Progress: ${uploaded}/${files.length} uploaded, ${failed} failed`);
    }
  }

  console.log(`Done: ${uploaded} uploaded, ${failed} failed`);
}

main().catch(console.error);
