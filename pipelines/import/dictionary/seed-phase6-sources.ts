/**
 * Seed Phase 6 Dictionary Sources
 *
 * Upserts 25 new DictionarySource records for the next wave of dictionaries.
 *
 * Usage:
 *   bun run pipelines/import/dictionary/seed-phase6-sources.ts
 */

import "../../env";
import { prisma } from "../../../src/db";

const PHASE6_SOURCES = [
  { slug: "gharib-al-hadith", bookId: "32196", nameArabic: "غريب الحديث", nameEnglish: "Gharib al-Hadith", author: "ابن الجوزي" },
  { slug: "al-majmu-al-mughith", bookId: "17124", nameArabic: "المجموع المغيث", nameEnglish: "Al-Majmu al-Mughith", author: "أبو موسى المديني" },
  { slug: "al-ubab-al-zakhir", bookId: "761", nameArabic: "العباب الزاخر", nameEnglish: "Al-Ubab al-Zakhir", author: "الصغاني" },
  { slug: "mujam-al-buldan", bookId: "23735", nameArabic: "معجم البلدان", nameEnglish: "Mu'jam al-Buldan", author: "ياقوت الحموي" },
  { slug: "majma-bihar-al-anwar", bookId: "13631", nameArabic: "مجمع بحار الأنوار", nameEnglish: "Majma Bihar al-Anwar", author: "محمد طاهر الفَتَّني" },
  { slug: "kashshaf-istilahat", bookId: "2573", nameArabic: "كشاف اصطلاحات الفنون", nameEnglish: "Kashshaf Istilahat al-Funun", author: "التهانوي" },
  { slug: "kashf-al-zunun", bookId: "259", nameArabic: "كشف الظنون", nameEnglish: "Kashf al-Zunun", author: "حاجي خليفة" },
  { slug: "dustur-al-ulama", bookId: "7038", nameArabic: "دستور العلماء", nameEnglish: "Dustur al-Ulama", author: "القاضي عبد النبي" },
  { slug: "al-tawqif", bookId: "10640", nameArabic: "التوقيف على مهمات التعاريف", nameEnglish: "Al-Tawqif ala Muhimmat al-Ta'arif", author: "المناوي" },
  { slug: "abjad-al-ulum", bookId: "9579", nameArabic: "أبجد العلوم", nameEnglish: "Abjad al-Ulum", author: "صديق حسن خان" },
  { slug: "al-tarifat-fiqhiyya", bookId: "13939", nameArabic: "التعريفات الفقهية", nameEnglish: "Al-Ta'rifat al-Fiqhiyya", author: "محمد عميم الإحسان" },
  { slug: "gharib-quran-ibn-abbas", bookId: "23622", nameArabic: "غريب القرآن في شعر العرب", nameEnglish: "Gharib al-Quran fi Shi'r al-Arab", author: "ابن عباس" },
  { slug: "tuhfat-al-arib", bookId: "13304", nameArabic: "تحفة الأريب", nameEnglish: "Tuhfat al-Arib", author: "أبو حيان الأندلسي" },
  { slug: "maqalid-al-ulum", bookId: "7039", nameArabic: "معجم مقاليد العلوم", nameEnglish: "Mu'jam Maqalid al-Ulum", author: "السيوطي" },
  { slug: "al-muhadhdhab", bookId: "26097", nameArabic: "المهذب فيما وقع في القرآن من المعرب", nameEnglish: "Al-Muhadhdhab", author: "السيوطي" },
  { slug: "al-mudhakkar-wal-muannath", bookId: "99", nameArabic: "المذكر والمؤنث", nameEnglish: "Al-Mudhakkar wal-Muannath", author: "ابن الأنباري" },
  { slug: "al-asharat", bookId: "7053", nameArabic: "العشرات في غريب اللغة", nameEnglish: "Al-Asharat fi Gharib al-Lugha", author: "أبو عمر الزاهد" },
  { slug: "al-shawarid", bookId: "23427", nameArabic: "الشوارد", nameEnglish: "Al-Shawarid", author: "الصغاني" },
  { slug: "al-alfaz-al-mukhtalifa", bookId: "7049", nameArabic: "الألفاظ المختلفة", nameEnglish: "Al-Alfaz al-Mukhtalifa", author: "ابن السكيت" },
  { slug: "al-itimad", bookId: "5368", nameArabic: "الاعتماد في نظائر الظاء والضاد", nameEnglish: "Al-I'timad fi Nazair al-Za wal-Dad", author: "ابن مالك" },
  { slug: "al-furuq", bookId: "7055", nameArabic: "الفرق", nameEnglish: "Al-Furuq", author: "أبو هلال العسكري" },
  { slug: "risalat-al-khatt", bookId: "5357", nameArabic: "رسالة الخط والقلم", nameEnglish: "Risalat al-Khatt wal-Qalam", author: "ابن قتيبة" },
  { slug: "al-mustalahat-al-arbaa", bookId: "2146", nameArabic: "المصطلحات الأربعة في القرآن", nameEnglish: "Al-Mustalahat al-Arba'a fil-Quran", author: "أبو الأعلى المودودي" },
  { slug: "mufradat-quran-farahi", bookId: "96629", nameArabic: "مفردات القرآن", nameEnglish: "Mufradat al-Quran (Farahi)", author: "عبد الحميد الفراهي" },
  { slug: "min-balaghat-quran", bookId: "38085", nameArabic: "من بلاغة القرآن", nameEnglish: "Min Balaghat al-Quran", author: "أحمد أحمد بدوي" },
];

async function main() {
  console.log(`Seeding ${PHASE6_SOURCES.length} Phase 6 dictionary sources...\n`);

  for (const src of PHASE6_SOURCES) {
    const result = await prisma.dictionarySource.upsert({
      where: { slug: src.slug },
      create: {
        slug: src.slug,
        nameArabic: src.nameArabic,
        nameEnglish: src.nameEnglish,
        author: src.author,
        bookId: src.bookId,
      },
      update: {
        nameArabic: src.nameArabic,
        nameEnglish: src.nameEnglish,
        author: src.author,
        bookId: src.bookId,
      },
    });
    console.log(`  ${result.id}: ${src.slug} (bookId=${src.bookId})`);
  }

  console.log(`\nDone. ${PHASE6_SOURCES.length} sources seeded.`);
}

main()
  .catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
  })
  .finally(() => prisma.$disconnect());
