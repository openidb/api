import { Hono } from "hono";
import { quranRoutes } from "./routes/quran";
import { hadithRoutes } from "./routes/hadith";
import { booksRoutes } from "./routes/books";
import { authorsRoutes } from "./routes/authors";
import { categoriesRoutes } from "./routes/categories";
import { searchRoutes } from "./routes/search";

const app = new Hono();

app.route("/api/search", searchRoutes);
app.route("/api/quran", quranRoutes);
app.route("/api/hadith", hadithRoutes);
app.route("/api/books", booksRoutes);
app.route("/api/authors", authorsRoutes);
app.route("/api/categories", categoriesRoutes);

app.get("/api/health", (c) => c.json({ status: "ok" }));

export default { port: 4000, fetch: app.fetch };
