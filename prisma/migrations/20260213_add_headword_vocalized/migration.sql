-- Add headword_vocalized column for diacritics-aware matching
ALTER TABLE "dictionary_entries" ADD COLUMN "headword_vocalized" TEXT NOT NULL DEFAULT '';
ALTER TABLE "dictionary_sub_entries" ADD COLUMN "headword_vocalized" TEXT NOT NULL DEFAULT '';

-- Indexes for vocalized lookup
CREATE INDEX "dictionary_entries_headword_vocalized_idx" ON "dictionary_entries"("headword_vocalized");
CREATE INDEX "dictionary_sub_entries_headword_vocalized_idx" ON "dictionary_sub_entries"("headword_vocalized");
