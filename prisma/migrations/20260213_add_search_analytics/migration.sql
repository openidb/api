-- CreateTable
CREATE TABLE "search_events" (
    "id" TEXT NOT NULL,
    "session_id" TEXT,
    "query" TEXT NOT NULL,
    "mode" TEXT NOT NULL,
    "is_refine" BOOLEAN NOT NULL DEFAULT false,
    "result_count" INTEGER NOT NULL DEFAULT 0,
    "total_time_ms" INTEGER,
    "top_results" JSONB,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "search_events_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "search_clicks" (
    "id" TEXT NOT NULL,
    "search_event_id" TEXT NOT NULL,
    "result_type" TEXT NOT NULL,
    "doc_id" TEXT NOT NULL,
    "rank" INTEGER NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "search_clicks_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "search_events_created_at_idx" ON "search_events"("created_at");

-- CreateIndex
CREATE INDEX "search_events_session_id_idx" ON "search_events"("session_id");

-- CreateIndex
CREATE INDEX "search_clicks_search_event_id_idx" ON "search_clicks"("search_event_id");

-- AddForeignKey
ALTER TABLE "search_clicks" ADD CONSTRAINT "search_clicks_search_event_id_fkey" FOREIGN KEY ("search_event_id") REFERENCES "search_events"("id") ON DELETE CASCADE ON UPDATE CASCADE;
