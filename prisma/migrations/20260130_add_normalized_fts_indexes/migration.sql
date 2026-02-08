-- CreateIndex: Expression index for normalized Arabic text FTS on hadiths
-- This allows efficient full-text search with Arabic text normalization
-- (removes diacritics, normalizes alef variants, converts teh marbuta to heh)
CREATE INDEX CONCURRENTLY IF NOT EXISTS "hadiths_text_normalized_fts_idx" ON "hadiths"
USING gin (to_tsvector('simple',
  translate(
    regexp_replace(text_plain, E'[\u064B-\u065F\u0670]', '', 'g'),
    E'\u0622\u0623\u0625\u0671\u0629',
    E'\u0627\u0627\u0627\u0627\u0647'
  )
));

-- CreateIndex: Expression index for normalized Arabic text FTS on pages
CREATE INDEX CONCURRENTLY IF NOT EXISTS "pages_content_normalized_fts_idx" ON "pages"
USING gin (to_tsvector('simple',
  translate(
    regexp_replace(content_plain, E'[\u064B-\u065F\u0670]', '', 'g'),
    E'\u0622\u0623\u0625\u0671\u0629',
    E'\u0627\u0627\u0627\u0627\u0647'
  )
));

-- CreateIndex: Expression index for normalized Arabic text FTS on ayahs
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ayahs_text_normalized_fts_idx" ON "ayahs"
USING gin (to_tsvector('simple',
  translate(
    regexp_replace(text_plain, E'[\u064B-\u065F\u0670]', '', 'g'),
    E'\u0622\u0623\u0625\u0671\u0629',
    E'\u0627\u0627\u0627\u0627\u0647'
  )
));
