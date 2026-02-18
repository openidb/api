import { join } from "path";

const AUDIO_BASE = process.env.QURAN_AUDIO_PATH || "/Volumes/KIOXIA/quran-audio";

/**
 * Compute the absolute file path for a reciter's ayah audio.
 * e.g. /Volumes/KIOXIA/quran-audio/everyayah/alafasy-128kbps/001001.mp3
 */
export function audioFilePath(slug: string, surah: number, ayah: number): string {
  const filename = `${String(surah).padStart(3, "0")}${String(ayah).padStart(3, "0")}.mp3`;
  return join(AUDIO_BASE, slug, filename);
}

/**
 * Get the base directory for all audio files.
 */
export function getAudioBasePath(): string {
  return AUDIO_BASE;
}
