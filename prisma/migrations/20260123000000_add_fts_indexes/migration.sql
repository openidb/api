-- Full-text search indexes for improved hybrid search performance
-- These GIN indexes support the keyword search part of hybrid search

-- Index for hadith full-text search
CREATE INDEX IF NOT EXISTS "hadiths_text_plain_fts_idx" ON "hadiths" USING GIN (to_tsvector('simple', text_plain));

-- Index for Quran ayah full-text search
CREATE INDEX IF NOT EXISTS "ayahs_text_plain_fts_idx" ON "ayahs" USING GIN (to_tsvector('simple', text_plain));
