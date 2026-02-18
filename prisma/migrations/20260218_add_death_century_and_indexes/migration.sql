-- Add computed death century column to authors
ALTER TABLE authors ADD COLUMN death_century_hijri INTEGER;

-- Populate from death_date_hijri
UPDATE authors
SET death_century_hijri = CEIL(CAST(death_date_hijri AS DOUBLE PRECISION) / 100)::int
WHERE death_date_hijri ~ '^[0-9]+$';

-- Index for century filter queries
CREATE INDEX idx_authors_death_century ON authors(death_century_hijri);

-- Composite index for hadith translation lookups (language + bookId + hadithNumber)
CREATE INDEX idx_hadith_translations_lang_book_hadith
ON hadith_translations(language, book_id, hadith_number);
