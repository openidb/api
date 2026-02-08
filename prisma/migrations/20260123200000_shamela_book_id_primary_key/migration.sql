-- Migration: Use shamelaBookId as the primary key for books
-- This migration changes the book identification from auto-increment id to shamelaBookId

-- Step 1: Add shamelaBookId column to related tables (for the transition)
ALTER TABLE "pages" ADD COLUMN "shamela_book_id" TEXT;
ALTER TABLE "table_of_contents" ADD COLUMN "shamela_book_id" TEXT;
ALTER TABLE "book_keywords" ADD COLUMN "shamela_book_id" TEXT;

-- Step 2: Populate the new columns with data from books table
UPDATE "pages" p
SET "shamela_book_id" = b.shamela_book_id
FROM "books" b
WHERE p.book_id = b.id;

UPDATE "table_of_contents" t
SET "shamela_book_id" = b.shamela_book_id
FROM "books" b
WHERE t.book_id = b.id;

UPDATE "book_keywords" k
SET "shamela_book_id" = b.shamela_book_id
FROM "books" b
WHERE k.book_id = b.id;

-- Step 3: Make the new columns NOT NULL
ALTER TABLE "pages" ALTER COLUMN "shamela_book_id" SET NOT NULL;
ALTER TABLE "table_of_contents" ALTER COLUMN "shamela_book_id" SET NOT NULL;
ALTER TABLE "book_keywords" ALTER COLUMN "shamela_book_id" SET NOT NULL;

-- Step 4: Drop old foreign key constraints
ALTER TABLE "pages" DROP CONSTRAINT "pages_book_id_fkey";
ALTER TABLE "table_of_contents" DROP CONSTRAINT "table_of_contents_book_id_fkey";
ALTER TABLE "book_keywords" DROP CONSTRAINT "book_keywords_book_id_fkey";

-- Step 5: Drop old unique index on pages (it's an index, not a constraint)
DROP INDEX IF EXISTS "pages_book_id_page_number_key";

-- Step 6: Drop old indexes
DROP INDEX IF EXISTS "pages_book_id_page_number_idx";
DROP INDEX IF EXISTS "table_of_contents_book_id_idx";

-- Step 7: Drop old book_id columns
ALTER TABLE "pages" DROP COLUMN "book_id";
ALTER TABLE "table_of_contents" DROP COLUMN "book_id";

-- Step 8: For book_keywords, we need to drop and recreate the primary key
ALTER TABLE "book_keywords" DROP CONSTRAINT "book_keywords_pkey";
ALTER TABLE "book_keywords" DROP COLUMN "book_id";
ALTER TABLE "book_keywords" ADD PRIMARY KEY ("shamela_book_id", "keyword");

-- Step 9: Drop old primary key from books and create new one
ALTER TABLE "books" DROP CONSTRAINT "books_pkey";
ALTER TABLE "books" DROP COLUMN "id";

-- Step 10: Make shamela_book_id the primary key
ALTER TABLE "books" ADD PRIMARY KEY ("shamela_book_id");

-- Step 11: Drop the old unique constraint on shamela_book_id (now it's the PK)
DROP INDEX IF EXISTS "books_shamela_book_id_key";

-- Step 12: Add new foreign key constraints
ALTER TABLE "pages" ADD CONSTRAINT "pages_shamela_book_id_fkey"
  FOREIGN KEY ("shamela_book_id") REFERENCES "books"("shamela_book_id") ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE "table_of_contents" ADD CONSTRAINT "table_of_contents_shamela_book_id_fkey"
  FOREIGN KEY ("shamela_book_id") REFERENCES "books"("shamela_book_id") ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE "book_keywords" ADD CONSTRAINT "book_keywords_shamela_book_id_fkey"
  FOREIGN KEY ("shamela_book_id") REFERENCES "books"("shamela_book_id") ON DELETE CASCADE ON UPDATE CASCADE;

-- Step 13: Recreate indexes
CREATE UNIQUE INDEX "pages_shamela_book_id_page_number_key" ON "pages"("shamela_book_id", "page_number");
CREATE INDEX "pages_shamela_book_id_page_number_idx" ON "pages"("shamela_book_id", "page_number");
CREATE INDEX "table_of_contents_shamela_book_id_idx" ON "table_of_contents"("shamela_book_id");

-- Step 14: Rename columns to bookId for consistency in Prisma
ALTER TABLE "pages" RENAME COLUMN "shamela_book_id" TO "book_id";
ALTER TABLE "table_of_contents" RENAME COLUMN "shamela_book_id" TO "book_id";
ALTER TABLE "book_keywords" RENAME COLUMN "shamela_book_id" TO "book_id";

-- Step 15: Rename primary key column in books from shamela_book_id to id
ALTER TABLE "books" RENAME COLUMN "shamela_book_id" TO "id";

-- Step 16: Update foreign key constraint names and references
ALTER TABLE "pages" DROP CONSTRAINT "pages_shamela_book_id_fkey";
ALTER TABLE "pages" ADD CONSTRAINT "pages_book_id_fkey"
  FOREIGN KEY ("book_id") REFERENCES "books"("id") ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE "table_of_contents" DROP CONSTRAINT "table_of_contents_shamela_book_id_fkey";
ALTER TABLE "table_of_contents" ADD CONSTRAINT "table_of_contents_book_id_fkey"
  FOREIGN KEY ("book_id") REFERENCES "books"("id") ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE "book_keywords" DROP CONSTRAINT "book_keywords_shamela_book_id_fkey";
ALTER TABLE "book_keywords" ADD CONSTRAINT "book_keywords_book_id_fkey"
  FOREIGN KEY ("book_id") REFERENCES "books"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- Step 17: Update index names
DROP INDEX IF EXISTS "pages_shamela_book_id_page_number_key";
DROP INDEX IF EXISTS "pages_shamela_book_id_page_number_idx";
DROP INDEX IF EXISTS "table_of_contents_shamela_book_id_idx";

CREATE UNIQUE INDEX "pages_book_id_page_number_key" ON "pages"("book_id", "page_number");
CREATE INDEX "pages_book_id_page_number_idx" ON "pages"("book_id", "page_number");
CREATE INDEX "table_of_contents_book_id_idx" ON "table_of_contents"("book_id");

-- Drop old index on books that referenced the old shamela_book_id
DROP INDEX IF EXISTS "books_shamela_book_id_idx";
