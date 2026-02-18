/**
 * In-memory tracker for in-flight page translations.
 *
 * Keyed by "pageId:language", stores the Promise returned by the LLM call.
 * - Deduplication: concurrent requests for the same page+lang share one Promise.
 * - Background completion: the Promise lives independently of HTTP connections,
 *   so if the client disconnects the translation still finishes and saves to DB.
 */

export interface TranslationResult {
  paragraphs: { index: number; translation: string }[];
  contentHash: string;
  model: string;
}

const inflight = new Map<string, Promise<TranslationResult>>();

export function getInflight(key: string): Promise<TranslationResult> | undefined {
  return inflight.get(key);
}

export function setInflight(key: string, promise: Promise<TranslationResult>): void {
  inflight.set(key, promise);
  // Self-clean when the promise settles (success or failure)
  promise.finally(() => {
    // Only delete if the map still holds this exact promise (guard against replacement)
    if (inflight.get(key) === promise) {
      inflight.delete(key);
    }
  });
}
