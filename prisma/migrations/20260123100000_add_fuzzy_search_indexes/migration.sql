-- Enable pg_trgm extension for fuzzy/typo-tolerant search
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Add GIN index for full-text search on pages content
-- This is the critical missing index for keyword search performance
CREATE INDEX IF NOT EXISTS "pages_content_plain_fts_idx"
ON "pages" USING GIN (to_tsvector('simple', content_plain));

-- Add trigram indexes for fuzzy matching on pages
CREATE INDEX IF NOT EXISTS "pages_content_plain_trgm_idx"
ON "pages" USING GIN (content_plain gin_trgm_ops);

-- Add trigram indexes for fuzzy matching on hadiths
CREATE INDEX IF NOT EXISTS "hadiths_text_plain_trgm_idx"
ON "hadiths" USING GIN (text_plain gin_trgm_ops);

-- Add trigram indexes for fuzzy matching on ayahs
CREATE INDEX IF NOT EXISTS "ayahs_text_plain_trgm_idx"
ON "ayahs" USING GIN (text_plain gin_trgm_ops);

-- Add trigram indexes for author name search
CREATE INDEX IF NOT EXISTS "authors_name_arabic_trgm_idx"
ON "authors" USING GIN (name_arabic gin_trgm_ops);

-- Add trigram indexes for book title search
CREATE INDEX IF NOT EXISTS "books_title_arabic_trgm_idx"
ON "books" USING GIN (title_arabic gin_trgm_ops);
