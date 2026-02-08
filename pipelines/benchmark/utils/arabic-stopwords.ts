/**
 * Curated Arabic stopwords list for Islamic text retrieval.
 * Includes: prepositions (حروف الجر), conjunctions (حروف العطف),
 * pronouns (ضمائر), particles, and common filler words.
 *
 * Does NOT include religiously significant words like الله, رب, etc.
 */

export const ARABIC_STOPWORDS = new Set([
  // Prepositions (حروف الجر)
  "في",
  "من",
  "إلى",
  "على",
  "عن",
  "مع",
  "حتى",
  "منذ",
  "خلال",
  "بين",
  "عند",
  "لدى",
  "نحو",
  "تحت",
  "فوق",
  "أمام",
  "خلف",
  "بعد",
  "قبل",
  "حول",
  "دون",
  "ضد",
  "عبر",

  // Conjunctions (حروف العطف)
  "و",
  "أو",
  "ثم",
  "ف",
  "لكن",
  "بل",
  "حيث",
  "إذا",
  "إذ",
  "لما",
  "كي",
  "أن",
  "إن",
  "لأن",
  "حين",
  "بينما",

  // Pronouns (ضمائر)
  "هو",
  "هي",
  "هم",
  "هن",
  "أنت",
  "أنتم",
  "أنتن",
  "أنا",
  "نحن",
  "هذا",
  "هذه",
  "هؤلاء",
  "ذلك",
  "تلك",
  "أولئك",

  // Demonstratives / relatives
  "الذي",
  "التي",
  "الذين",
  "اللذان",
  "اللتان",
  "اللواتي",
  "اللائي",
  "ما",
  "من",

  // Particles (أدوات)
  "لا",
  "لم",
  "لن",
  "قد",
  "سوف",
  "كان",
  "كانت",
  "كانوا",
  "يكون",
  "تكون",
  "ليس",
  "ليست",
  "ليسوا",

  // Question particles
  "هل",
  "أ",
  "ما",
  "ماذا",
  "متى",
  "أين",
  "كيف",
  "لماذا",
  "كم",

  // Common verbs / copulas
  "كان",
  "يكون",
  "صار",
  "أصبح",
  "أمسى",
  "بات",
  "ظل",

  // Common filler / function words
  "كل",
  "بعض",
  "غير",
  "سوى",
  "فقط",
  "أيضا",
  "جدا",
  "كذلك",
  "مثل",
  "هنا",
  "هناك",
  "الآن",
  "أي",
  "لقد",
  "فإن",
  "وإن",
  "ولا",
  "فلا",
  "ولم",
  "فلم",
  "إلا",
  "عليه",
  "عليها",
  "عليهم",
  "منه",
  "منها",
  "منهم",
  "فيه",
  "فيها",
  "فيهم",
  "به",
  "بها",
  "بهم",
  "له",
  "لها",
  "لهم",
  "عنه",
  "عنها",
  "عنهم",
]);

/**
 * Remove stopwords from Arabic text.
 * Preserves word order, just removes stopword tokens.
 */
export function removeStopwords(text: string): string {
  const words = text.split(/\s+/);
  const filtered = words.filter((w) => !ARABIC_STOPWORDS.has(w));
  // If too many words removed (>80%), return original to avoid empty/meaningless text
  if (filtered.length < words.length * 0.2) {
    return text;
  }
  return filtered.join(" ");
}
