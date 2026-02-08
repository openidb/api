-- CreateTable
CREATE TABLE "authors" (
    "id" SERIAL NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,
    "shamela_author_id" TEXT,
    "name_arabic" TEXT NOT NULL,
    "name_latin" TEXT NOT NULL,
    "kunya" TEXT,
    "nasab" TEXT,
    "nisba" TEXT,
    "laqab" TEXT,
    "birth_date_hijri" TEXT,
    "death_date_hijri" TEXT,
    "birth_date_gregorian" TEXT,
    "death_date_gregorian" TEXT,
    "biography" TEXT,
    "biography_source" TEXT,

    CONSTRAINT "authors_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "categories" (
    "id" SERIAL NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,
    "shamela_category_id" TEXT,
    "name_arabic" TEXT NOT NULL,
    "name_english" TEXT,
    "parent_id" INTEGER,

    CONSTRAINT "categories_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "publishers" (
    "id" SERIAL NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,
    "name" TEXT NOT NULL,
    "location" TEXT,

    CONSTRAINT "publishers_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "editors" (
    "id" SERIAL NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,
    "name" TEXT NOT NULL,

    CONSTRAINT "editors_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "books" (
    "id" SERIAL NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,
    "shamela_book_id" TEXT NOT NULL,
    "title_arabic" TEXT NOT NULL,
    "title_latin" TEXT NOT NULL,
    "author_id" INTEGER NOT NULL,
    "category_id" INTEGER,
    "publisher_id" INTEGER,
    "editor_id" INTEGER,
    "publication_year_hijri" TEXT,
    "publication_year_gregorian" TEXT,
    "publication_edition" TEXT,
    "publication_location" TEXT,
    "isbn" TEXT,
    "total_volumes" INTEGER NOT NULL DEFAULT 1,
    "total_pages" INTEGER,
    "page_alignment_note" TEXT,
    "verification_status" TEXT,
    "editorial_type" TEXT,
    "institution" TEXT,
    "supervisor" TEXT,
    "description_html" TEXT,
    "summary" TEXT,
    "filename" TEXT NOT NULL,
    "time_period" TEXT,

    CONSTRAINT "books_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "book_keywords" (
    "book_id" INTEGER NOT NULL,
    "keyword" TEXT NOT NULL,

    CONSTRAINT "book_keywords_pkey" PRIMARY KEY ("book_id","keyword")
);

-- CreateTable
CREATE TABLE "author_works" (
    "id" SERIAL NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "author_id" INTEGER NOT NULL,
    "shamela_book_id" TEXT NOT NULL,
    "title" TEXT NOT NULL,
    "is_in_catalog" BOOLEAN NOT NULL DEFAULT false,

    CONSTRAINT "author_works_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "pages" (
    "id" SERIAL NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "book_id" INTEGER NOT NULL,
    "page_number" INTEGER NOT NULL,
    "volume_number" INTEGER NOT NULL DEFAULT 1,
    "url_page_index" TEXT,
    "printed_page_number" INTEGER,
    "content_plain" TEXT NOT NULL,
    "content_html" TEXT NOT NULL,
    "has_poetry" BOOLEAN NOT NULL DEFAULT false,
    "has_hadith" BOOLEAN NOT NULL DEFAULT false,
    "has_quran" BOOLEAN NOT NULL DEFAULT false,
    "has_dialogue" BOOLEAN NOT NULL DEFAULT false,
    "source_url" TEXT,
    "pdf_url" TEXT,

    CONSTRAINT "pages_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "footnotes" (
    "id" SERIAL NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "page_id" INTEGER NOT NULL,
    "marker" TEXT NOT NULL,
    "content" TEXT NOT NULL,
    "position_in_page" INTEGER NOT NULL,

    CONSTRAINT "footnotes_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "table_of_contents" (
    "id" SERIAL NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "book_id" INTEGER NOT NULL,
    "volume_number" INTEGER NOT NULL DEFAULT 1,
    "chapter_title" TEXT NOT NULL,
    "page_number" INTEGER NOT NULL,
    "order_index" INTEGER NOT NULL,
    "parent_id" INTEGER,

    CONSTRAINT "table_of_contents_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "authors_shamela_author_id_key" ON "authors"("shamela_author_id");

-- CreateIndex
CREATE INDEX "authors_name_latin_idx" ON "authors"("name_latin");

-- CreateIndex
CREATE INDEX "authors_shamela_author_id_idx" ON "authors"("shamela_author_id");

-- CreateIndex
CREATE UNIQUE INDEX "categories_shamela_category_id_key" ON "categories"("shamela_category_id");

-- CreateIndex
CREATE INDEX "categories_parent_id_idx" ON "categories"("parent_id");

-- CreateIndex
CREATE UNIQUE INDEX "books_shamela_book_id_key" ON "books"("shamela_book_id");

-- CreateIndex
CREATE INDEX "books_author_id_idx" ON "books"("author_id");

-- CreateIndex
CREATE INDEX "books_category_id_idx" ON "books"("category_id");

-- CreateIndex
CREATE INDEX "books_shamela_book_id_idx" ON "books"("shamela_book_id");

-- CreateIndex
CREATE INDEX "author_works_author_id_idx" ON "author_works"("author_id");

-- CreateIndex
CREATE INDEX "author_works_shamela_book_id_idx" ON "author_works"("shamela_book_id");

-- CreateIndex
CREATE INDEX "pages_book_id_page_number_idx" ON "pages"("book_id", "page_number");

-- CreateIndex
CREATE UNIQUE INDEX "pages_book_id_page_number_key" ON "pages"("book_id", "page_number");

-- CreateIndex
CREATE INDEX "footnotes_page_id_idx" ON "footnotes"("page_id");

-- CreateIndex
CREATE INDEX "table_of_contents_book_id_idx" ON "table_of_contents"("book_id");

-- CreateIndex
CREATE INDEX "table_of_contents_parent_id_idx" ON "table_of_contents"("parent_id");

-- AddForeignKey
ALTER TABLE "categories" ADD CONSTRAINT "categories_parent_id_fkey" FOREIGN KEY ("parent_id") REFERENCES "categories"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "books" ADD CONSTRAINT "books_author_id_fkey" FOREIGN KEY ("author_id") REFERENCES "authors"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "books" ADD CONSTRAINT "books_category_id_fkey" FOREIGN KEY ("category_id") REFERENCES "categories"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "books" ADD CONSTRAINT "books_publisher_id_fkey" FOREIGN KEY ("publisher_id") REFERENCES "publishers"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "books" ADD CONSTRAINT "books_editor_id_fkey" FOREIGN KEY ("editor_id") REFERENCES "editors"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "book_keywords" ADD CONSTRAINT "book_keywords_book_id_fkey" FOREIGN KEY ("book_id") REFERENCES "books"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "author_works" ADD CONSTRAINT "author_works_author_id_fkey" FOREIGN KEY ("author_id") REFERENCES "authors"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "pages" ADD CONSTRAINT "pages_book_id_fkey" FOREIGN KEY ("book_id") REFERENCES "books"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "footnotes" ADD CONSTRAINT "footnotes_page_id_fkey" FOREIGN KEY ("page_id") REFERENCES "pages"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "table_of_contents" ADD CONSTRAINT "table_of_contents_parent_id_fkey" FOREIGN KEY ("parent_id") REFERENCES "table_of_contents"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "table_of_contents" ADD CONSTRAINT "table_of_contents_book_id_fkey" FOREIGN KEY ("book_id") REFERENCES "books"("id") ON DELETE CASCADE ON UPDATE CASCADE;
