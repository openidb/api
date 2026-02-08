import { Hono } from "hono";

export const authorsRoutes = new Hono();

// GET / — list authors (paginated, searchable)
authorsRoutes.get("/", (c) => {
  return c.json({ message: "TODO: list authors" });
});

// GET /:id — get author by id
authorsRoutes.get("/:id", (c) => {
  return c.json({ message: `TODO: get author ${c.req.param("id")}` });
});
