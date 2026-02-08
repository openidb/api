/**
 * Initialize Neo4j Schema for Knowledge Graph
 *
 * Creates constraints, indexes, and full-text search indexes
 * for the Quran entity-relationship knowledge graph.
 *
 * Usage: bun run scripts/knowledge-graph/init-neo4j.ts
 */

import neo4jDriver from "../../src/graph/driver";

async function main() {
  const session = neo4jDriver.session();

  try {
    console.log("Initializing Neo4j schema...");
    console.log("=".repeat(60));

    // Unique constraints
    console.log("Creating constraints...");

    await session.run(`
      CREATE CONSTRAINT entity_id IF NOT EXISTS
      FOR (e:Entity) REQUIRE e.id IS UNIQUE
    `);
    console.log("  ✓ Entity.id unique constraint");

    await session.run(`
      CREATE CONSTRAINT ayahgroup_id IF NOT EXISTS
      FOR (g:AyahGroup) REQUIRE g.id IS UNIQUE
    `);
    console.log("  ✓ AyahGroup.id unique constraint");

    // Indexes for common queries
    console.log("\nCreating indexes...");

    await session.run(`
      CREATE INDEX entity_type IF NOT EXISTS
      FOR (e:Entity) ON (e.type)
    `);
    console.log("  ✓ Entity.type index");

    await session.run(`
      CREATE INDEX ayahgroup_surah IF NOT EXISTS
      FOR (g:AyahGroup) ON (g.surahNumber)
    `);
    console.log("  ✓ AyahGroup.surahNumber index");

    // Full-text indexes for bilingual search
    console.log("\nCreating full-text indexes...");

    await session.run(`
      CREATE FULLTEXT INDEX entity_name_arabic IF NOT EXISTS
      FOR (e:Entity) ON EACH [e.nameArabic]
    `);
    console.log("  ✓ Entity nameArabic full-text index");

    await session.run(`
      CREATE FULLTEXT INDEX entity_name_english IF NOT EXISTS
      FOR (e:Entity) ON EACH [e.nameEnglish]
    `);
    console.log("  ✓ Entity nameEnglish full-text index");

    // Verify
    const result = await session.run("SHOW INDEXES");
    console.log(`\nTotal indexes: ${result.records.length}`);

    console.log("\n" + "=".repeat(60));
    console.log("Schema initialization complete!");
  } finally {
    await session.close();
    await neo4jDriver.close();
  }
}

main().catch((e) => {
  console.error("Schema init failed:", e);
  process.exit(1);
});
