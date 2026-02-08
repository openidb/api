-- CreateTable
CREATE TABLE "ayah_translations" (
    "id" SERIAL NOT NULL,
    "surah_number" INTEGER NOT NULL,
    "ayah_number" INTEGER NOT NULL,
    "language" TEXT NOT NULL,
    "edition_id" TEXT NOT NULL,
    "text" TEXT NOT NULL,

    CONSTRAINT "ayah_translations_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "ayah_translations_surah_number_ayah_number_idx" ON "ayah_translations"("surah_number", "ayah_number");

-- CreateIndex
CREATE INDEX "ayah_translations_language_idx" ON "ayah_translations"("language");

-- CreateIndex
CREATE UNIQUE INDEX "ayah_translations_surah_number_ayah_number_language_key" ON "ayah_translations"("surah_number", "ayah_number", "language");
