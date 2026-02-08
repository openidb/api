import { Hono } from "hono";

export const categoriesRoutes = new Hono();

// GET / — get category tree
categoriesRoutes.get("/", (c) => {
  return c.json({ message: "TODO: list categories" });
});
