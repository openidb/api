-- CreateTable
CREATE TABLE "hadith_collections" (
    "id" SERIAL NOT NULL,
    "slug" TEXT NOT NULL,
    "name_english" TEXT NOT NULL,
    "name_arabic" TEXT NOT NULL,

    CONSTRAINT "hadith_collections_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "hadith_books" (
    "id" SERIAL NOT NULL,
    "collection_id" INTEGER NOT NULL,
    "book_number" INTEGER NOT NULL,
    "name_english" TEXT NOT NULL,
    "name_arabic" TEXT NOT NULL,

    CONSTRAINT "hadith_books_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "hadiths" (
    "id" SERIAL NOT NULL,
    "book_id" INTEGER NOT NULL,
    "hadith_number" TEXT NOT NULL,
    "text_arabic" TEXT NOT NULL,
    "text_plain" TEXT NOT NULL,
    "chapter_arabic" TEXT,
    "chapter_english" TEXT,

    CONSTRAINT "hadiths_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "hadith_collections_slug_key" ON "hadith_collections"("slug");

-- CreateIndex
CREATE UNIQUE INDEX "hadith_books_collection_id_book_number_key" ON "hadith_books"("collection_id", "book_number");

-- CreateIndex
CREATE INDEX "hadiths_book_id_idx" ON "hadiths"("book_id");

-- CreateIndex
CREATE UNIQUE INDEX "hadiths_book_id_hadith_number_key" ON "hadiths"("book_id", "hadith_number");

-- AddForeignKey
ALTER TABLE "hadith_books" ADD CONSTRAINT "hadith_books_collection_id_fkey" FOREIGN KEY ("collection_id") REFERENCES "hadith_collections"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "hadiths" ADD CONSTRAINT "hadiths_book_id_fkey" FOREIGN KEY ("book_id") REFERENCES "hadith_books"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
