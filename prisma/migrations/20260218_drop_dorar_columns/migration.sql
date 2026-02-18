-- Drop unused Dorar.net columns from hadiths table
-- These columns were always null after replacing Dorar with HadithUnlocked.com

-- Drop the unique index on dorar_id first
DROP INDEX IF EXISTS "hadiths_dorar_id_key";

ALTER TABLE "hadiths"
  DROP COLUMN IF EXISTS "dorar_id",
  DROP COLUMN IF EXISTS "narrator_name",
  DROP COLUMN IF EXISTS "grader_dorar_id",
  DROP COLUMN IF EXISTS "source_book_dorar_id",
  DROP COLUMN IF EXISTS "number_or_page",
  DROP COLUMN IF EXISTS "takhrij",
  DROP COLUMN IF EXISTS "categories",
  DROP COLUMN IF EXISTS "has_similar",
  DROP COLUMN IF EXISTS "has_alternate",
  DROP COLUMN IF EXISTS "has_usul",
  DROP COLUMN IF EXISTS "sharh_text",
  DROP COLUMN IF EXISTS "usul_data";
