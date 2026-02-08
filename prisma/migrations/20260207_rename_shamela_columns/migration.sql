-- Rename Shamela-specific columns to generic names

-- Category: shamela_category_id → code
ALTER TABLE "categories" RENAME COLUMN "shamela_category_id" TO "code";

-- AuthorWork: shamela_book_id → book_id
ALTER TABLE "author_works" RENAME COLUMN "shamela_book_id" TO "book_id";
