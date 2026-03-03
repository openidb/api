-- Pre-computed fields to eliminate 4 queries from getBook handler
-- These replace: findFirst(maxPrintedPage), 3x groupBy(volumeNumber)

ALTER TABLE books ADD COLUMN max_printed_page INTEGER;
ALTER TABLE books ADD COLUMN volume_start_pages JSONB;
ALTER TABLE books ADD COLUMN volume_max_printed_pages JSONB;
ALTER TABLE books ADD COLUMN volume_min_printed_pages JSONB;

-- Index on pages(book_id, volume_number) for the backfill script and any fallback queries
CREATE INDEX IF NOT EXISTS idx_pages_book_volume
  ON pages(book_id, volume_number);
