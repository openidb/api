# openidb

REST API for Islamic texts — Quran, Hadith, and classical Arabic books — with hybrid search combining semantic similarity, keyword matching, knowledge graphs, and LLM reranking.

Built with Hono, PostgreSQL, Qdrant, Elasticsearch, and Neo4j.

## Data

- **Quran** — 114 surahs, 6,236 ayahs (Uthmani script), 12 translation languages, 2 tafsirs (Al-Jalalayn, Ibn Kathir)
- **Hadith** — 17 collections (Bukhari, Muslim, Abu Dawud, Tirmidhi, Nasa'i, Ibn Majah, Ahmad, Malik, Darimi, Riyad as-Salihin, Al-Adab Al-Mufrad, Ash-Shama'il, Mishkat al-Masabih, Bulugh al-Maram, Nawawi's 40, 40 Qudsi, Hisn al-Muslim)
- **Books** — Classical Arabic texts from Maktaba Shamela with full-text content, authors, categories, and publishers

## API

Base URL: `http://localhost:4000`

| Endpoint | Description |
|----------|-------------|
| `GET /api/search` | Hybrid search across all content |
| `GET /api/quran/surahs` | List all surahs |
| `GET /api/quran/surahs/:number` | Surah with ayahs |
| `GET /api/quran/ayahs` | Query ayahs (filter by surah, juz, page) |
| `GET /api/quran/tafsir/:surah/:ayah` | Tafsir for an ayah |
| `GET /api/quran/translations/:surah/:ayah` | Translations for an ayah |
| `GET /api/hadith/collections` | List hadith collections |
| `GET /api/hadith/collections/:slug` | Collection with books |
| `GET /api/hadith/collections/:slug/books/:bookNumber` | Hadiths in a book |
| `GET /api/hadith/collections/:slug/:number` | Specific hadith |
| `GET /api/books` | List books (search, filter by author/category) |
| `GET /api/books/:id` | Book details |
| `GET /api/books/:id/pages` | List pages for a book |
| `GET /api/books/:id/pages/:page` | Page content |
| `POST /api/books/:id/pages/:page/translate` | Translate page paragraphs |
| `GET /api/authors` | List authors |
| `GET /api/authors/:id` | Author with bibliography |
| `GET /api/categories` | Category tree |
| `GET /api/categories/:id` | Category with books |
| `GET /api/stats` | Database counts |
| `POST /api/transcribe` | Audio transcription (Groq Whisper) |
| `GET /api/health` | Health check |

## Search

The `/api/search` endpoint combines multiple retrieval strategies:

1. **Semantic search** — Gemini embeddings (3072d) or BGE-M3 (1024d) via Qdrant
2. **Keyword search** — Elasticsearch BM25 with phrase matching and fuzzy fallback
3. **Reciprocal Rank Fusion** — Merges semantic and keyword rankings (K=60)
4. **Knowledge graph** — Neo4j entity lookup with source resolution and ayah boosting
5. **LLM reranking** — Optional reranking via GPT-OSS or Gemini Flash through OpenRouter
6. **Query expansion** — Expands queries with semantically related terms for broader recall

Key parameters: `mode` (hybrid/semantic/keyword), `reranker`, `embeddingModel` (gemini/bge-m3), `includeQuran`, `includeHadith`, `includeBooks`, `includeGraph`, `refine`.

## Setup

### Prerequisites

- [Bun](https://bun.sh)
- Docker

### 1. Start infrastructure

```bash
docker compose up -d
```

This starts PostgreSQL (port 5433), Qdrant (port 6333), Elasticsearch (port 9200), and Neo4j (ports 7474/7687).

### 2. Configure environment

```bash
cp .env.example .env
```

Required variables:
```
DATABASE_URL=postgresql://postgres:your_password@localhost:5433/openislamicdb
POSTGRES_PASSWORD=your_password
QDRANT_URL=http://localhost:6333
ELASTICSEARCH_URL=http://localhost:9200
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=sanad_graph_123
ALLOWED_ORIGINS=http://localhost:3000
```

Optional:
```
OPENROUTER_API_KEY=    # Gemini embeddings and LLM reranking
BGE_M3_URL=            # Local BGE-M3 embedding server
GROQ_API_KEY=          # Voice transcription
```

### 3. Install dependencies and migrate

```bash
bun install
bun run db:push
```

### 4. Run

```bash
bun run dev
```

The API starts at http://localhost:4000.

## Project Structure

```
src/
├── index.ts                    # Entry point, routes, CORS
├── db.ts                       # Prisma singleton (PostgreSQL)
├── qdrant.ts                   # Qdrant singleton + collection config
├── constants.ts                # Shared constants
├── embeddings/                 # Gemini and BGE-M3 embedding clients
├── graph/                      # Neo4j driver + entity search
├── search/                     # Elasticsearch client + keyword search
├── lib/                        # OpenRouter client, TTL cache
├── routes/
│   ├── search/                 # Hybrid search pipeline
│   │   ├── index.ts            # Main search endpoint
│   │   ├── standard-search.ts  # Standard hybrid search
│   │   ├── refine-search.ts    # Query expansion
│   │   ├── engines.ts          # Semantic search (Qdrant)
│   │   ├── fusion.ts           # RRF + score merging
│   │   ├── rerankers.ts        # LLM reranking
│   │   ├── response.ts         # Graph context + metadata
│   │   └── config.ts           # Thresholds and constants
│   ├── quran.ts
│   ├── hadith.ts
│   ├── books.ts
│   ├── authors.ts
│   ├── categories.ts
│   ├── transcribe.ts
│   └── stats.ts
├── utils/
│   ├── pagination.ts
│   ├── source-urls.ts
│   └── timing.ts
└── prisma/
    └── schema.prisma           # Database schema
```

## Data Import

Import scripts live in `pipelines/import/`:

```bash
bun run pipelines/import/import-quran.ts              # Quran from Al Quran Cloud API
bun run pipelines/import/import-quran-translations.ts  # Quran translations
bun run pipelines/import/import-tafsir.ts              # Al-Jalalayn tafsir
bun run pipelines/import/import-ibn-kathir.ts          # Ibn Kathir tafsir
bun run pipelines/import/scrape-sunnah.ts              # Hadith collections from sunnah.com
bun run pipelines/import/import-epubs.ts               # Books from Shamela EPUBs
```

Additional pipelines:

```bash
bun run pipelines/embed/generate-embeddings.ts               # Generate vector embeddings
bun run pipelines/index/sync-elasticsearch.ts                 # Sync data to Elasticsearch
bun run pipelines/knowledge-graph/seed-neo4j.ts               # Seed Neo4j knowledge graph
```

## Part of [OpenIDB](https://github.com/openidb)

This is the API server. See also:
- [sabeel](https://github.com/openidb/sabeel) — Frontend (Next.js)
- [scrapers](https://github.com/openidb/scrapers) — Data acquisition
