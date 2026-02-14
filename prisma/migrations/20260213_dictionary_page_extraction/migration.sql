-- Make entry_id nullable (sub-entries can now exist without a parent DictionaryEntry)
ALTER TABLE "dictionary_sub_entries" ALTER COLUMN "entry_id" DROP NOT NULL;

-- Drop the old unique constraint on (entry_id, position)
ALTER TABLE "dictionary_sub_entries" DROP CONSTRAINT IF EXISTS "dictionary_sub_entries_entry_id_position_key";

-- Change onDelete from CASCADE to SET NULL for the entry FK
ALTER TABLE "dictionary_sub_entries" DROP CONSTRAINT IF EXISTS "dictionary_sub_entries_entry_id_fkey";
ALTER TABLE "dictionary_sub_entries" ADD CONSTRAINT "dictionary_sub_entries_entry_id_fkey"
  FOREIGN KEY ("entry_id") REFERENCES "dictionary_entries"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- Add new columns for page-based extraction
ALTER TABLE "dictionary_sub_entries" ADD COLUMN "book_id" TEXT;
ALTER TABLE "dictionary_sub_entries" ADD COLUMN "page_number" INTEGER;

-- Index for looking up sub-entries by book + page
CREATE INDEX "dictionary_sub_entries_book_id_page_number_idx" ON "dictionary_sub_entries"("book_id", "page_number");
