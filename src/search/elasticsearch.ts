/**
 * Elasticsearch Client Singleton
 *
 * Provides a singleton Elasticsearch client instance for the application.
 * Prevents multiple instances in development (hot reload).
 */

import { Client } from "@elastic/elasticsearch";

const globalForES = globalThis as unknown as {
  elasticsearch: Client | undefined;
};

export const elasticsearch =
  globalForES.elasticsearch ??
  new Client({
    node: process.env.ELASTICSEARCH_URL || "http://localhost:9200",
  });

if (process.env.NODE_ENV !== "production") {
  globalForES.elasticsearch = elasticsearch;
}

// Index names
export const ES_PAGES_INDEX = "arabic_pages";
export const ES_HADITHS_INDEX = "arabic_hadiths";
export const ES_AYAHS_INDEX = "arabic_ayahs";
