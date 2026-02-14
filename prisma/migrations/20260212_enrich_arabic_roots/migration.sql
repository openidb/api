-- Enrich ArabicRoot table with metadata from Arramooz
-- Adds vocalized forms, wazn patterns, word types, definitions, and source tracking

ALTER TABLE "arabic_roots" ADD COLUMN "vocalized" TEXT;
ALTER TABLE "arabic_roots" ADD COLUMN "pattern" TEXT;
ALTER TABLE "arabic_roots" ADD COLUMN "word_type" TEXT;
ALTER TABLE "arabic_roots" ADD COLUMN "definition" TEXT;
ALTER TABLE "arabic_roots" ADD COLUMN "part_of_speech" TEXT;
ALTER TABLE "arabic_roots" ADD COLUMN "source" VARCHAR(30);

-- Add index on root column for word-family lookups (root → all derived words)
CREATE INDEX "arabic_roots_root_idx" ON "arabic_roots"("root");
