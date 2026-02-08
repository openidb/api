-- Part 1: Create new metadata tables

CREATE TABLE "quran_translations" (
    "id" TEXT NOT NULL,
    "language" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "author" TEXT,
    "source" TEXT NOT NULL,
    "direction" TEXT NOT NULL DEFAULT 'ltr',
    CONSTRAINT "quran_translations_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "quran_translations_language_idx" ON "quran_translations"("language");

CREATE TABLE "quran_tafsirs" (
    "id" TEXT NOT NULL,
    "language" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "author" TEXT,
    "source" TEXT NOT NULL,
    "direction" TEXT NOT NULL DEFAULT 'ltr',
    CONSTRAINT "quran_tafsirs_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "quran_tafsirs_language_idx" ON "quran_tafsirs"("language");

-- Part 2: Update AyahTranslation — change unique constraint from (surah, ayah, language) to (surah, ayah, editionId)

DROP INDEX IF EXISTS "ayah_translations_surah_number_ayah_number_language_key";
CREATE UNIQUE INDEX "ayah_translations_surah_number_ayah_number_edition_id_key" ON "ayah_translations"("surah_number", "ayah_number", "edition_id");
CREATE INDEX "ayah_translations_edition_id_idx" ON "ayah_translations"("edition_id");

-- Part 3: Update AyahTafsir — add edition_id and language columns, backfill, change constraint

ALTER TABLE "ayah_tafsirs" ADD COLUMN "edition_id" TEXT;
ALTER TABLE "ayah_tafsirs" ADD COLUMN "language" TEXT DEFAULT 'ar';

-- Backfill edition_id from existing source values
UPDATE "ayah_tafsirs" SET "edition_id" = 'ar-tafsir-ibn-kathir' WHERE "source" = 'ibn_kathir';
UPDATE "ayah_tafsirs" SET "edition_id" = 'ar-jalalayn' WHERE "source" = 'jalalayn';
-- Fallback: any other source gets prefixed with 'ar-'
UPDATE "ayah_tafsirs" SET "edition_id" = 'ar-' || "source" WHERE "edition_id" IS NULL;

-- Make edition_id NOT NULL now that it's backfilled
ALTER TABLE "ayah_tafsirs" ALTER COLUMN "edition_id" SET NOT NULL;
ALTER TABLE "ayah_tafsirs" ALTER COLUMN "language" SET NOT NULL;

-- Swap unique constraint
DROP INDEX IF EXISTS "ayah_tafsirs_surah_number_ayah_number_source_key";
CREATE UNIQUE INDEX "ayah_tafsirs_surah_number_ayah_number_edition_id_key" ON "ayah_tafsirs"("surah_number", "ayah_number", "edition_id");
CREATE INDEX "ayah_tafsirs_edition_id_idx" ON "ayah_tafsirs"("edition_id");
CREATE INDEX "ayah_tafsirs_language_idx" ON "ayah_tafsirs"("language");

-- Part 4: Seed metadata for existing editions

-- Existing 12 translations
INSERT INTO "quran_translations" ("id", "language", "name", "author", "source", "direction") VALUES
  ('eng-mustafakhattaba', 'en', 'Dr. Mustafa Khattab (The Clear Quran)', 'Dr. Mustafa Khattab', 'fawazahmed0', 'ltr'),
  ('fra-muhammadhameedu', 'fr', 'Muhammad Hamidullah', 'Muhammad Hamidullah', 'fawazahmed0', 'ltr'),
  ('ind-indonesianislam', 'id', 'Indonesian Islamic Ministry', NULL, 'fawazahmed0', 'ltr'),
  ('urd-fatehmuhammadja', 'ur', 'Fateh Muhammad Jalandhry', 'Fateh Muhammad Jalandhry', 'fawazahmed0', 'rtl'),
  ('spa-muhammadisagarc', 'es', 'Isa Garcia', 'Muhammad Isa Garcia', 'fawazahmed0', 'ltr'),
  ('zho-majian', 'zh', 'Ma Jian', 'Ma Jian', 'fawazahmed0', 'ltr'),
  ('por-samirelhayek', 'pt', 'Samir El-Hayek', 'Samir El-Hayek', 'fawazahmed0', 'ltr'),
  ('rus-elmirkuliev', 'ru', 'Elmir Kuliev', 'Elmir Kuliev', 'fawazahmed0', 'ltr'),
  ('jpn-ryoichimita', 'ja', 'Ryoichi Mita', 'Ryoichi Mita', 'fawazahmed0', 'ltr'),
  ('kor-hamidchoi', 'ko', 'Hamid Choi', 'Hamid Choi', 'fawazahmed0', 'ltr'),
  ('ita-hamzarobertopic', 'it', 'Hamza Roberto Piccardo', 'Hamza Roberto Piccardo', 'fawazahmed0', 'ltr'),
  ('ben-muhiuddinkhan', 'bn', 'Muhiuddin Khan', 'Muhiuddin Khan', 'fawazahmed0', 'ltr')
ON CONFLICT ("id") DO NOTHING;

-- Existing 2 tafsirs
INSERT INTO "quran_tafsirs" ("id", "language", "name", "author", "source", "direction") VALUES
  ('ar-tafsir-ibn-kathir', 'ar', 'Tafsir Ibn Kathir', 'Ibn Kathir', 'spa5k-tafsir', 'rtl'),
  ('ar-jalalayn', 'ar', 'Tafsir Al-Jalalayn', 'Jalal ad-Din al-Mahalli & Jalal ad-Din as-Suyuti', 'quran-tafseer', 'rtl')
ON CONFLICT ("id") DO NOTHING;
