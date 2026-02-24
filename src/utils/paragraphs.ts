/**
 * Paragraph extraction utilities for Arabic book content.
 *
 * Shared by the translate page route (src/routes/books.ts) and
 * the batch translation pipeline (pipelines/translate/translate-book.ts).
 */

export const ARABIC_REGEX = /[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]/;

export function isMeaningfulContent(text: string): boolean {
  if (text.length < 2) return false;
  if (!ARABIC_REGEX.test(text)) return false;
  if (/^[\s\d\-–—_.*•·,،؛:;!?'"()[\]{}«»<>\/\\|@#$%^&+=~`]+$/.test(text)) return false;
  return true;
}

export function stripHtmlEntities(text: string): string {
  return text
    .replace(/<[^>]+>/g, "")
    .replace(/&nbsp;/g, " ").replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<").replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"').replace(/&#39;/g, "'")
    .replace(/\s+/g, " ").trim();
}

export function extractParagraphs(html: string): { index: number; text: string }[] {
  const paragraphs: { index: number; text: string }[] = [];

  // Try <p> tag extraction first
  const pRegex = /<p[^>]*>([\s\S]*?)<\/p>/gi;
  let match;
  let index = 0;
  while ((match = pRegex.exec(html)) !== null) {
    const text = stripHtmlEntities(match[1]);
    if (isMeaningfulContent(text)) paragraphs.push({ index, text });
    index++;
  }
  if (paragraphs.length > 0) return paragraphs;

  // Fallback: newline splitting (Turath raw text format)
  // Join multi-line title spans into single lines (matches frontend formatContentHtml)
  html = html.replace(
    /<span\s+data-type=['"]title['"][^>]*>[\s\S]*?<\/span>/g,
    (m) => m.replace(/\n/g, " ")
  );
  const lines = html.split(/\n/);
  index = 0;
  for (const line of lines) {
    const text = stripHtmlEntities(line);
    if (isMeaningfulContent(text)) {
      paragraphs.push({ index, text });
    }
    index++;
  }
  return paragraphs;
}
