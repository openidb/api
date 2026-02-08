/**
 * Seed Neo4j with Extracted Knowledge Graph Data
 *
 * Reads extracted-*.json files and creates:
 * - AyahGroup nodes (with surah metadata, ayah range, text)
 * - Entity nodes (deduplicated by ID across surahs, multi-labeled by type)
 * - Typed relationship edges between entities
 * - MENTIONED_IN edges from entities to ayah groups
 *
 * Usage: bun run scripts/knowledge-graph/seed-neo4j.ts [--clear]
 */

import neo4jDriver from "../../src/graph/driver";
import { readdirSync, readFileSync } from "fs";
import { join } from "path";

const DATA_DIR = join(import.meta.dir, "data");
const clearFlag = process.argv.includes("--clear");

interface SourceRef {
  type: "quran" | "hadith" | "tafsir" | "book";
  ref: string;
}

interface Entity {
  id: string;
  type: string;
  nameArabic: string;
  nameEnglish: string;
  descriptionArabic: string;
  descriptionEnglish: string;
  sources?: SourceRef[];
}

interface Relationship {
  source: string;
  target: string;
  type: string;
  description?: string;
  role?: string;
  context?: string;
  sources?: SourceRef[];
}

interface ExtractionFile {
  entities: Entity[];
  relationships: Relationship[];
}

interface SurahDump {
  surah: number;
  surahNameArabic: string;
  surahNameEnglish: string;
  groups: Array<{
    id: string;
    startAyah: number;
    endAyah: number;
    ayahTextArabic: string;
  }>;
}

async function main() {
  const session = neo4jDriver.session();

  try {
    console.log("Seeding Neo4j knowledge graph...");
    console.log("=".repeat(60));

    if (clearFlag) {
      console.log("Clearing existing data...");
      await session.run("MATCH (n) DETACH DELETE n");
      console.log("  ✓ All nodes and relationships deleted");
    }

    // Read all extraction files
    const extractionFiles = readdirSync(DATA_DIR)
      .filter((f) => f.startsWith("extracted-") && f.endsWith(".json"))
      .sort();

    // Read all surah dump files (for AyahGroup text)
    const surahDumpFiles = readdirSync(DATA_DIR)
      .filter((f) => f.startsWith("surah-") && f.endsWith(".json"))
      .sort();

    if (extractionFiles.length === 0) {
      console.error("No extracted-*.json files found in", DATA_DIR);
      process.exit(1);
    }

    console.log(`Found ${extractionFiles.length} extraction files`);
    console.log(`Found ${surahDumpFiles.length} surah dump files`);

    // 1. Create AyahGroup nodes from dump files
    console.log("\nCreating AyahGroup nodes...");
    let groupCount = 0;

    for (const file of surahDumpFiles) {
      const dump: SurahDump = JSON.parse(
        readFileSync(join(DATA_DIR, file), "utf-8")
      );

      for (const group of dump.groups) {
        await session.run(
          `
          MERGE (g:AyahGroup {id: $id})
          SET g.surahNumber = $surahNumber,
              g.surahNameArabic = $surahNameArabic,
              g.surahNameEnglish = $surahNameEnglish,
              g.startAyah = $startAyah,
              g.endAyah = $endAyah,
              g.textArabic = $textArabic
          `,
          {
            id: group.id,
            surahNumber: dump.surah,
            surahNameArabic: dump.surahNameArabic,
            surahNameEnglish: dump.surahNameEnglish,
            startAyah: group.startAyah,
            endAyah: group.endAyah,
            textArabic: group.ayahTextArabic,
          }
        );
        groupCount++;
      }
    }
    console.log(`  ✓ ${groupCount} AyahGroup nodes`);

    // 2. Create Entity nodes (deduplicated across files)
    console.log("\nCreating Entity nodes...");
    const allEntities = new Map<string, Entity>();
    const allRelationships: Relationship[] = [];

    for (const file of extractionFiles) {
      const data: ExtractionFile = JSON.parse(
        readFileSync(join(DATA_DIR, file), "utf-8")
      );

      for (const entity of data.entities) {
        // Keep first occurrence or merge (first wins for descriptions)
        if (!allEntities.has(entity.id)) {
          allEntities.set(entity.id, entity);
        }
      }

      allRelationships.push(...data.relationships);
    }

    for (const entity of allEntities.values()) {
      // Create entity with multi-label: :Entity:Type
      // Store sources as JSON string (Neo4j doesn't support nested arrays)
      await session.run(
        `
        MERGE (e:Entity {id: $id})
        SET e.type = $type,
            e.nameArabic = $nameArabic,
            e.nameEnglish = $nameEnglish,
            e.descriptionArabic = $descriptionArabic,
            e.descriptionEnglish = $descriptionEnglish,
            e.sources = $sources
        `,
        {
          id: entity.id,
          type: entity.type,
          nameArabic: entity.nameArabic,
          nameEnglish: entity.nameEnglish,
          descriptionArabic: entity.descriptionArabic,
          descriptionEnglish: entity.descriptionEnglish,
          sources: JSON.stringify(entity.sources || []),
        }
      );

      // Add type-specific label (e.g., :Prophet, :Place, :AfterlifePlace)
      await session.run(
        `MATCH (e:Entity {id: $id}) SET e:${entity.type}`,
        { id: entity.id }
      );
    }
    console.log(`  ✓ ${allEntities.size} Entity nodes`);

    // 3. Create relationships
    console.log("\nCreating relationships...");
    let relCount = 0;
    let mentionedInCount = 0;

    for (const rel of allRelationships) {
      if (rel.type === "MENTIONED_IN") {
        // MENTIONED_IN edges go from Entity to AyahGroup
        // Target format is "ayahgroup:28:1-3" but AyahGroup id is "28:1-3"
        const ayahGroupId = rel.target.replace(/^ayahgroup:/, "");
        await session.run(
          `
          MATCH (e:Entity {id: $source})
          MATCH (g:AyahGroup {id: $target})
          MERGE (e)-[r:MENTIONED_IN]->(g)
          SET r.role = $role,
              r.context = $context
          `,
          {
            source: rel.source,
            target: ayahGroupId,
            role: rel.role || "referenced",
            context: rel.context || "",
          }
        );
        mentionedInCount++;
      } else {
        // Dynamic relationship types between entities
        // Use APOC to create dynamic relationship types
        // Store sources as JSON string on the relationship
        await session.run(
          `
          MATCH (a:Entity {id: $source})
          MATCH (b:Entity {id: $target})
          CALL apoc.merge.relationship(a, $relType, {description: $description, sources: $sources}, {}, b, {}) YIELD rel
          RETURN rel
          `,
          {
            source: rel.source,
            target: rel.target,
            relType: rel.type,
            description: rel.description || "",
            sources: JSON.stringify(rel.sources || []),
          }
        );
        relCount++;
      }
    }
    console.log(`  ✓ ${relCount} entity relationships`);
    console.log(`  ✓ ${mentionedInCount} MENTIONED_IN edges`);

    // 4. Summary stats
    console.log("\n" + "=".repeat(60));
    console.log("SUMMARY");

    const entityStats = await session.run(`
      MATCH (e:Entity) RETURN e.type AS type, count(*) AS count
      ORDER BY count DESC
    `);
    console.log("\nEntities by type:");
    for (const record of entityStats.records) {
      console.log(`  ${record.get("type")}: ${record.get("count").toNumber()}`);
    }

    const relStats = await session.run(`
      MATCH ()-[r]->() RETURN type(r) AS type, count(*) AS count
      ORDER BY count DESC
    `);
    console.log("\nRelationships by type:");
    for (const record of relStats.records) {
      console.log(`  ${record.get("type")}: ${record.get("count").toNumber()}`);
    }

    console.log("\n" + "=".repeat(60));
    console.log("Seeding complete!");
  } finally {
    await session.close();
    await neo4jDriver.close();
  }
}

main().catch((e) => {
  console.error("Seed failed:", e);
  process.exit(1);
});
