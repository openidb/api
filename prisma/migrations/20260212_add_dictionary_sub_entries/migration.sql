-- CreateTable
CREATE TABLE "dictionary_sub_entries" (
    "id" SERIAL NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "entry_id" INTEGER NOT NULL,
    "source_id" INTEGER NOT NULL,
    "headword" TEXT NOT NULL,
    "headword_normalized" TEXT NOT NULL,
    "root" TEXT NOT NULL,
    "root_normalized" TEXT NOT NULL,
    "definition_html" TEXT NOT NULL,
    "definition_plain" TEXT NOT NULL,
    "position" INTEGER NOT NULL DEFAULT 0,

    CONSTRAINT "dictionary_sub_entries_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "dictionary_sub_entries_source_id_idx" ON "dictionary_sub_entries"("source_id");

-- CreateIndex
CREATE INDEX "dictionary_sub_entries_entry_id_idx" ON "dictionary_sub_entries"("entry_id");

-- CreateIndex
CREATE INDEX "dictionary_sub_entries_headword_normalized_idx" ON "dictionary_sub_entries"("headword_normalized");

-- CreateIndex
CREATE INDEX "dictionary_sub_entries_root_normalized_idx" ON "dictionary_sub_entries"("root_normalized");

-- CreateIndex
CREATE UNIQUE INDEX "dictionary_sub_entries_entry_id_position_key" ON "dictionary_sub_entries"("entry_id", "position");

-- AddForeignKey
ALTER TABLE "dictionary_sub_entries" ADD CONSTRAINT "dictionary_sub_entries_entry_id_fkey" FOREIGN KEY ("entry_id") REFERENCES "dictionary_entries"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "dictionary_sub_entries" ADD CONSTRAINT "dictionary_sub_entries_source_id_fkey" FOREIGN KEY ("source_id") REFERENCES "dictionary_sources"("id") ON DELETE CASCADE ON UPDATE CASCADE;
