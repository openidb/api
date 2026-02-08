/**
 * Normalize Arabic text for consistent matching
 * - Removes diacritics (tashkeel)
 * - Normalizes alef variants, hamza, alef maksura, teh marbuta
 * - Normalizes whitespace
 */
export function normalizeArabicText(text: string): string {
  return (
    text
      // Remove Arabic diacritics (tashkeel)
      .replace(/[\u064B-\u065F\u0670]/g, "")
      // Normalize alef variants to plain alef
      .replace(/[\u0622\u0623\u0625\u0671]/g, "\u0627")
      // Remove standalone hamza
      .replace(/\u0621/g, "")
      // Normalize alef maksura to yeh
      .replace(/\u0649/g, "\u064A")
      // Normalize teh marbuta to heh
      .replace(/\u0629/g, "\u0647")
      // Normalize whitespace
      .replace(/\s+/g, " ")
      .trim()
  );
}
