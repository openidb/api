import { Hono } from "hono";

export const quranRoutes = new Hono();

// GET /surahs — list all surahs
quranRoutes.get("/surahs", (c) => {
  return c.json({ message: "TODO: list surahs" });
});

// GET /surahs/:number — get surah with ayahs
quranRoutes.get("/surahs/:number", (c) => {
  return c.json({ message: `TODO: get surah ${c.req.param("number")}` });
});

// GET /ayahs — query ayahs with filters
quranRoutes.get("/ayahs", (c) => {
  return c.json({ message: "TODO: query ayahs" });
});
