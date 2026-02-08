/**
 * Neo4j Driver Singleton
 *
 * Provides a singleton Neo4j driver instance.
 * Prevents multiple instances in development (hot reload).
 */

import neo4j, { Driver } from "neo4j-driver";

const NEO4J_URI = process.env.NEO4J_URI || "bolt://localhost:7687";
const NEO4J_USER = process.env.NEO4J_USER || "neo4j";
const NEO4J_PASSWORD = process.env.NEO4J_PASSWORD;

if (!NEO4J_PASSWORD) {
  throw new Error("NEO4J_PASSWORD environment variable is required");
}

const globalForNeo4j = globalThis as unknown as {
  neo4jDriver: Driver | undefined;
};

export const neo4jDriver =
  globalForNeo4j.neo4jDriver ??
  neo4j.driver(NEO4J_URI, neo4j.auth.basic(NEO4J_USER, NEO4J_PASSWORD));

if (process.env.NODE_ENV !== "production") {
  globalForNeo4j.neo4jDriver = neo4jDriver;
}

export default neo4jDriver;
