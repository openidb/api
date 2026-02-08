-- AlterTable - Add unique constraints
-- Drop the old index on name_latin for authors (it's becoming unique)
DROP INDEX IF EXISTS "authors_name_latin_idx";

-- Add unique constraint on authors.name_latin
CREATE UNIQUE INDEX "authors_name_latin_key" ON "authors"("name_latin");

-- Add unique constraint on categories.name_arabic
CREATE UNIQUE INDEX "categories_name_arabic_key" ON "categories"("name_arabic");
