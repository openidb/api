-- AlterTable: make bookId, startPage, endPage optional on dictionary_entries
ALTER TABLE "dictionary_entries" ALTER COLUMN "book_id" DROP NOT NULL;
ALTER TABLE "dictionary_entries" ALTER COLUMN "start_page" DROP NOT NULL;
ALTER TABLE "dictionary_entries" ALTER COLUMN "end_page" DROP NOT NULL;
