/**
 * Create Elasticsearch Indices
 *
 * Creates the Arabic text indices with proper analyzer settings.
 * Run this before syncing data.
 *
 * Usage: bun run scripts/create-elasticsearch-indices.ts
 */

import { elasticsearch } from "../../src/search/elasticsearch";
import {
  pagesIndexConfig,
  hadithsIndexConfig,
  ayahsIndexConfig,
} from "../../src/search/elasticsearch-indices";

async function createIndices() {
  console.log("Creating Elasticsearch indices...\n");

  const indices = [
    { name: "arabic_pages", config: pagesIndexConfig },
    { name: "arabic_hadiths", config: hadithsIndexConfig },
    { name: "arabic_ayahs", config: ayahsIndexConfig },
  ];

  for (const { name, config } of indices) {
    try {
      // Check if index exists
      const exists = await elasticsearch.indices.exists({ index: name });

      if (exists) {
        console.log(`Index "${name}" exists, deleting...`);
        await elasticsearch.indices.delete({ index: name });
        console.log(`Index "${name}" deleted.`);
      }

      // Create index with settings
      await elasticsearch.indices.create(config);
      console.log(`Index "${name}" created successfully.`);

      // Verify settings
      const settings = await elasticsearch.indices.getSettings({ index: name });
      const mapping = await elasticsearch.indices.getMapping({ index: name });

      console.log(`  Shards: ${settings[name]?.settings?.index?.number_of_shards}`);
      console.log(`  Replicas: ${settings[name]?.settings?.index?.number_of_replicas}`);
      console.log(`  Fields: ${Object.keys(mapping[name]?.mappings?.properties || {}).join(", ")}`);
      console.log();
    } catch (error) {
      console.error(`Error creating index "${name}":`, error);
      process.exit(1);
    }
  }

  console.log("All indices created successfully!");

  // Show cluster health
  const health = await elasticsearch.cluster.health();
  console.log(`\nCluster health: ${health.status}`);
  console.log(`Number of nodes: ${health.number_of_nodes}`);
}

// Run
createIndices().catch((err) => {
  console.error("Failed to create indices:", err);
  process.exit(1);
});
