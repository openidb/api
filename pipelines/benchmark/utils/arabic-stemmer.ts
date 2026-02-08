/**
 * Light10 Arabic Stemmer
 *
 * A lightweight Arabic stemmer that removes common prefixes and suffixes
 * without attempting full root extraction. This preserves more semantic
 * meaning than aggressive root-based stemmers (like Khoja).
 *
 * Based on the Light10 algorithm by Larkey, Ballesteros, Connell (2007).
 */

// Common Arabic prefixes to remove (ordered longest first)
const PREFIXES = [
  "وال",  // wa-al (and the)
  "بال",  // bi-al (with the)
  "كال",  // ka-al (like the)
  "فال",  // fa-al (so the)
  "لل",   // li-l (to the)
  "ال",   // al (the)
  "وب",   // wa-bi
  "وك",   // wa-ka
  "ول",   // wa-li
  "وف",   // wa-fa
  "و",    // wa (and)
  "ب",    // bi (with/in)
  "ك",    // ka (like)
  "ل",    // li (to/for)
  "ف",    // fa (so/then)
];

// Common Arabic suffixes to remove (ordered longest first)
const SUFFIXES = [
  "هما",  // dual pronoun
  "كما",  // your (dual)
  "هم",   // their (masc)
  "هن",   // their (fem)
  "كم",   // your (pl masc)
  "كن",   // your (pl fem)
  "نا",   // our/us
  "ها",   // her/it
  "ية",   // nisba (adjective) feminine
  "ات",   // feminine plural
  "ون",   // masculine plural nominative
  "ين",   // masculine plural accusative/genitive
  "ان",   // dual nominative
  "تم",   // you (pl masc) verb
  "تن",   // you (pl fem) verb
  "وا",   // they (verb ending)
  "ة",    // ta marbuta
  "ه",    // pronoun suffix
  "ي",    // first person / nisba
  "و",    // plural marker
  "ا",    // alef suffix
];

// Minimum stem length to avoid over-stemming
const MIN_STEM_LENGTH = 2;

/**
 * Apply light stemming to a single Arabic word.
 * Removes common prefixes and suffixes without root extraction.
 */
export function stemWord(word: string): string {
  if (word.length <= MIN_STEM_LENGTH) return word;

  let stem = word;

  // Remove one prefix (longest match first)
  for (const prefix of PREFIXES) {
    if (stem.startsWith(prefix) && stem.length - prefix.length >= MIN_STEM_LENGTH) {
      stem = stem.slice(prefix.length);
      break;
    }
  }

  // Remove one suffix (longest match first)
  for (const suffix of SUFFIXES) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= MIN_STEM_LENGTH) {
      stem = stem.slice(0, -suffix.length);
      break;
    }
  }

  return stem;
}

/**
 * Apply light stemming to entire text.
 * Stems each word individually, preserving word order.
 */
export function stemText(text: string): string {
  return text
    .split(/\s+/)
    .map(stemWord)
    .join(" ");
}
