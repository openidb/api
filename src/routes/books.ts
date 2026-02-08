import { Hono } from "hono";

export const booksRoutes = new Hono();

// GET / — list books (paginated, searchable)
booksRoutes.get("/", (c) => {
  return c.json({ message: "TODO: list books" });
});

// GET /:id — get book by id
booksRoutes.get("/:id", (c) => {
  return c.json({ message: `TODO: get book ${c.req.param("id")}` });
});

// GET /:id/pages/:page — get book page
booksRoutes.get("/:id/pages/:page", (c) => {
  return c.json({ message: `TODO: get page ${c.req.param("page")} of book ${c.req.param("id")}` });
});
