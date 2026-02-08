-- AlterTable
ALTER TABLE "books" DROP COLUMN "book_type";

-- AlterTable
ALTER TABLE "pages" DROP COLUMN "ayah_end",
DROP COLUMN "ayah_start",
DROP COLUMN "juz_number",
DROP COLUMN "surah_name_arabic",
DROP COLUMN "surah_number";

-- CreateTable
CREATE TABLE "surahs" (
    "id" SERIAL NOT NULL,
    "number" INTEGER NOT NULL,
    "name_arabic" TEXT NOT NULL,
    "name_english" TEXT NOT NULL,
    "revelation_type" TEXT NOT NULL,
    "ayah_count" INTEGER NOT NULL,

    CONSTRAINT "surahs_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ayahs" (
    "id" SERIAL NOT NULL,
    "surah_id" INTEGER NOT NULL,
    "ayah_number" INTEGER NOT NULL,
    "text_uthmani" TEXT NOT NULL,
    "text_plain" TEXT NOT NULL,
    "juz_number" INTEGER NOT NULL,
    "page_number" INTEGER NOT NULL,

    CONSTRAINT "ayahs_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "surahs_number_key" ON "surahs"("number");

-- CreateIndex
CREATE INDEX "ayahs_surah_id_idx" ON "ayahs"("surah_id");

-- CreateIndex
CREATE UNIQUE INDEX "ayahs_surah_id_ayah_number_key" ON "ayahs"("surah_id", "ayah_number");

-- AddForeignKey
ALTER TABLE "ayahs" ADD CONSTRAINT "ayahs_surah_id_fkey" FOREIGN KEY ("surah_id") REFERENCES "surahs"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
