-- DropIndex
DROP INDEX IF EXISTS "Author_name_latin_key";

-- AlterTable: make Author.name_latin nullable
ALTER TABLE "Author" ALTER COLUMN "name_latin" DROP NOT NULL;

-- AlterTable: make Book.title_latin nullable
ALTER TABLE "Book" ALTER COLUMN "title_latin" DROP NOT NULL;

-- Clear existing transliteration data
UPDATE "Author" SET "name_latin" = NULL;
UPDATE "Book" SET "title_latin" = NULL;
