-- Book reader interaction events
CREATE TABLE "book_events" (
    "id" TEXT NOT NULL DEFAULT gen_random_uuid(),
    "session_id" TEXT,
    "book_id" TEXT NOT NULL,
    "action" TEXT NOT NULL,
    "page_number" INTEGER,
    "duration_ms" INTEGER,
    "word" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "book_events_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "book_events_created_at_idx" ON "book_events"("created_at");
CREATE INDEX "book_events_session_id_idx" ON "book_events"("session_id");
CREATE INDEX "book_events_book_id_idx" ON "book_events"("book_id");
