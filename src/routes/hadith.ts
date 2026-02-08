import { Hono } from "hono";

export const hadithRoutes = new Hono();

// GET /collections — list all collections
hadithRoutes.get("/collections", (c) => {
  return c.json({ message: "TODO: list collections" });
});

// GET /collections/:slug — get collection with books
hadithRoutes.get("/collections/:slug", (c) => {
  return c.json({ message: `TODO: get collection ${c.req.param("slug")}` });
});

// GET /collections/:slug/:number — get hadith by number
hadithRoutes.get("/collections/:slug/:number", (c) => {
  return c.json({ message: `TODO: get hadith ${c.req.param("slug")}/${c.req.param("number")}` });
});
