-- AlterTable
ALTER TABLE "books" ADD COLUMN     "book_type" TEXT NOT NULL DEFAULT 'regular';

-- AlterTable
ALTER TABLE "pages" ADD COLUMN     "ayah_end" INTEGER,
ADD COLUMN     "ayah_start" INTEGER,
ADD COLUMN     "juz_number" INTEGER,
ADD COLUMN     "surah_name_arabic" TEXT,
ADD COLUMN     "surah_number" INTEGER;
