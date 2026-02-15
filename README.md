<div align="right" dir="rtl">

**بسم الله الرحمن الرحيم**

الحمد لله ربّ العالمين، والصلاة والسلام على أشرف الأنبياء والمرسلين، نبيّنا محمّد وعلى آله وصحبه أجمعين.

قال رسول الله ﷺ:

> بَلِّغُوا عَنِّي وَلَوْ آيَةً، وَحَدِّثُوا عَنْ بَنِي إِسْرَائِيلَ وَلاَ حَرَجَ، وَمَنْ كَذَبَ عَلَىَّ مُتَعَمِّدًا فَلْيَتَبَوَّأْ مَقْعَدَهُ مِنَ النَّارِ
>
> — صحيح البخاري ٣٤٦١

> مَنْ دَعَا إِلَى هُدَى كَانَ لَهُ مِنَ الأَجْرِ مِثْلُ أُجُورِ مَنْ تَبِعَهُ لاَ يَنْقُصُ ذَلِكَ مِنْ أُجُورِهِمْ شَيْئًا
>
> — رياض الصالحين ١٣٨٢

> نَضَّرَ اللَّهُ امْرَأً سَمِعَ مِنَّا شَيْئًا فَبَلَّغَهُ كَمَا سَمِعَهُ فَرُبَّ مُبَلِّغٍ أَوْعَى مِنْ سَامِعٍ
>
> — رياض الصالحين ١٣٨٩

</div>

---

# api

REST API for Islamic texts — Quran, Hadith, classical Arabic books, and Arabic dictionaries — with hybrid search combining semantic similarity, keyword matching, knowledge graphs, and LLM reranking.

Built with Hono, PostgreSQL, Qdrant, Elasticsearch, and Neo4j.

## Data

- **Quran** — 114 surahs, 6,236 ayahs (Uthmani script), 500+ translation editions across 90+ languages, 27 tafsirs in 6 languages, 100+ audio reciters from 4 sources
- **Hadith** — 24 collections (166,964 hadiths) from two sources:
  - **[sunnah.com](https://sunnah.com/)** — 17 collections (49,618 hadiths): Bukhari, Muslim, Abu Dawud, Tirmidhi, Nasa'i, Ibn Majah, Ahmad, Malik, Darimi, Riyad as-Salihin, Al-Adab Al-Mufrad, Ash-Shama'il, Mishkat al-Masabih, Bulugh al-Maram, Nawawi's 40, 40 Qudsi, Hisn al-Muslim
  - **[hadithunlocked.com](https://hadithunlocked.com/)** — 7 collections (117,346 hadiths) with full tashkeel, isnad/matn separation, grading, and English translations: Mustadrak al-Hakim, Sahih Ibn Hibban, Al-Mu'jam al-Kabir, Sunan al-Kubra (Bayhaqi), Sunan al-Kubra (Nasa'i), Jam' al-Jawami' (Suyuti), Al-Zuhd (Ahmad)
- **Books** — Classical Arabic texts from [Turath.io](https://turath.io/) — full-text HTML pages, volume/printed page numbers, table of contents, scanned PDFs, authors, categories, publishers, and editors
- **Dictionary** — 43 Arabic dictionaries (~115K entries, 155K+ sub-entries) including Lisan al-Arab, Taj al-Arus, al-Qamus, al-Wasit, and more — with root derivation (~457K word-root mappings), vocalized headword matching, and 7-tier lookup fallback

## API

Base URL: `http://localhost:4000`

| Endpoint | Description |
|----------|-------------|
| `GET /api/search` | Hybrid search across all content |
| `GET /api/quran/surahs` | List all surahs |
| `GET /api/quran/surahs/:number` | Surah with ayahs |
| `GET /api/quran/ayahs` | Query ayahs (filter by surah, juz, page) |
| `GET /api/quran/tafsirs` | List available tafsir editions |
| `GET /api/quran/tafsir/:surah/:ayah` | Tafsir for an ayah |
| `GET /api/quran/translations` | List available translation editions |
| `GET /api/quran/translations/:surah/:ayah` | Translations for an ayah |
| `GET /api/quran/reciters` | List available audio reciters |
| `GET /api/quran/audio/:surah/:ayah` | Stream per-ayah audio (MP3) |
| `GET /api/hadith/collections` | List hadith collections |
| `GET /api/hadith/collections/:slug` | Collection with books |
| `GET /api/hadith/collections/:slug/books/:bookNumber` | Hadiths in a book |
| `GET /api/hadith/collections/:slug/:number` | Specific hadith |
| `GET /api/books` | List books (search, filter by author/category) |
| `GET /api/books/:id` | Book details |
| `GET /api/books/:id/pages` | List pages for a book |
| `GET /api/books/:id/pages/:page` | Page content |
| `POST /api/books/:id/pages/:page/translate` | Translate page paragraphs |
| `GET /api/books/authors` | List authors |
| `GET /api/books/authors/:id` | Author with bibliography |
| `GET /api/books/categories` | Category tree |
| `GET /api/books/categories/:id` | Category with books |
| `GET /api/dictionary/lookup/:word` | Look up a word across all dictionaries |
| `GET /api/dictionary/root/:root` | Word family for a root |
| `GET /api/dictionary/resolve/:word` | Resolve a word to its root |
| `GET /api/dictionary/sources` | List available dictionary sources |
| `GET /api/stats` | Database counts |
| `GET /api/health` | Health check |

## Search

The `/api/search` endpoint combines multiple retrieval strategies:

1. **Semantic search** — Gemini embeddings (3072d) via Qdrant
2. **Keyword search** — Elasticsearch BM25 with phrase matching and fuzzy fallback
3. **Reciprocal Rank Fusion** — Merges semantic and keyword rankings (K=60)
4. **Knowledge graph** — Neo4j entity lookup with source resolution and ayah boosting
5. **LLM reranking** — Optional reranking via GPT-OSS or Gemini Flash through OpenRouter
6. **Query expansion** — Expands queries with semantically related terms for broader recall

Key parameters: `mode` (hybrid/semantic/keyword), `reranker`, `includeQuran`, `includeHadith`, `includeBooks`, `includeGraph`, `refine`.

## Middleware Stack

Requests pass through middleware in this order:

1. **CORS** — Origin validation via `ALLOWED_ORIGINS` env var
2. **Compression** — Gzip via `hono/compress`
3. **Timeout** — 30s default, 60s for `/translate` endpoints. Returns 504 on timeout.
4. **Request logging** — Logs `METHOD /path STATUS duration_ms` per request
5. **Rate limiting** — Per-IP rate limits on search endpoints
6. **Internal auth** — Shared-secret authentication for internal endpoints (transcribe, translate)
7. **Routes** — Application handlers

## Health Check

`GET /api/health` pings all backend services and returns their status:

```json
{
  "status": "ok",
  "services": {
    "postgres": "ok",
    "qdrant": "ok",
    "elasticsearch": "ok"
  }
}
```

Returns HTTP 503 if any service is unreachable.

## Caching

Static data endpoints include `Cache-Control` headers:

| Endpoint pattern | Cache duration |
|-----------------|---------------|
| `/api/quran/surahs`, `/api/hadith/collections`, `/api/stats` | 1 hour (`max-age=3600`) |
| `/api/quran/surahs/:number`, `/api/hadith/collections/:slug` | 24 hours (`max-age=86400`) |
| `/api/quran/audio/:surah/:ayah` | 24 hours (`max-age=86400, immutable`) |
| `/api/search` | No cache |

## Setup

### Prerequisites

- [Bun](https://bun.sh)
- Docker

### 1. Start infrastructure

```bash
docker compose up -d
```

This starts PostgreSQL (port 5433), Qdrant (port 6333), Elasticsearch (port 9200), and Neo4j (ports 7474/7687). All services have Docker health checks.

### 2. Configure environment

```bash
cp .env.example .env
```

Required variables:
```
DATABASE_URL=postgresql://postgres:your_password@localhost:5433/openislamicdb
POSTGRES_PASSWORD=your_password
NEO4J_PASSWORD=your_neo4j_password
QDRANT_URL=http://localhost:6333
ELASTICSEARCH_URL=http://localhost:9200
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
ALLOWED_ORIGINS=http://localhost:3000
INTERNAL_API_SECRET=   # Shared secret for frontend → api internal calls
```

Optional:
```
OPENROUTER_API_KEY=    # Gemini embeddings and LLM reranking
GROQ_API_KEY=          # Voice transcription
DB_POOL_MAX=20         # PostgreSQL connection pool max (default: 20)
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
├── index.ts                    # Entry point, middleware stack, routes
├── db.ts                       # Prisma singleton (PostgreSQL + pool config)
├── qdrant.ts                   # Qdrant singleton + collection config
├── constants.ts                # Shared constants
├── middleware/
│   ├── timeout.ts              # Request timeout (30s/60s)
│   ├── request-logger.ts       # Per-request logging
│   ├── rate-limit.ts           # Per-IP rate limits
│   └── internal-auth.ts        # Internal service auth
├── embeddings/                 # Gemini embedding client
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
│   ├── quran.ts                 # Surahs, ayahs, translations, tafsirs, reciters, audio
│   ├── hadith.ts
│   ├── books.ts
│   ├── authors.ts
│   ├── categories.ts
│   ├── centuries.ts
│   ├── dictionary.ts             # Dictionary lookup, root family, resolve
│   ├── transcribe.ts
│   └── stats.ts
├── utils/
│   ├── arabic-text.ts             # Root extraction, normalization, vocalized matching
│   ├── pagination.ts
│   ├── source-urls.ts
│   ├── audio-storage.ts           # Quran audio file paths + streaming
│   └── timing.ts
└── schemas/
    ├── dictionary.ts              # Dictionary API schemas
    └── ...

prisma/
└── schema.prisma               # Database schema
```

## Data Import

Import scripts live in `pipelines/import/`:

```bash
# Quran
bun run pipelines/import/import-quran.ts                                         # Quran text from Al Quran Cloud API
bun run pipelines/import/import-quran-translations.ts --all                      # All translations (500+ editions)
bun run pipelines/import/import-quran-translations.ts --lang=en,fr               # By language
bun run pipelines/import/import-quran-translations.ts --edition=eng-mustafakhattaba  # Single edition
bun run pipelines/import/import-tafsirs.ts --all                                 # All tafsirs (27 editions)
bun run pipelines/import/import-tafsirs.ts --lang=en                             # By language

# Quran Audio (100+ reciters from EveryAyah, Al Quran Cloud, Quran Foundation, Tarteel)
bun run pipelines/import/import-quran-audio.ts --source=everyayah                # Download from a source
bun run pipelines/import/import-quran-audio.ts --source=everyayah --reciter=alafasy-128kbps  # Single reciter
bun run pipelines/import/import-qul-audio.ts                                     # Tarteel AI / QUL reciters

# Hadith (sunnah.com — 17 collections)
bun run pipelines/import/scrape-sunnah.ts --download-only --all                  # Download HTML from sunnah.com
bun run pipelines/import/scrape-sunnah.ts --process-only --all                   # Import into database
bun run pipelines/import/scrape-hadith-translations.ts --all                     # English translations

# Hadith (hadithunlocked.com — 7 collections with isnad/matn, grading, tashkeel)
bun run pipelines/import/import-hadith-unlocked.ts --all --download-only                # Download TSV bulk exports
bun run pipelines/import/import-hadith-unlocked.ts --all                                # Import all 7 collections
bun run pipelines/import/import-hadith-unlocked.ts --collection=hakim                   # Single collection by alias
bun run pipelines/import/import-hadith-unlocked.ts --collection=hakim --dry-run         # Preview without writing

# Books (Turath API → PostgreSQL + RustFS)
bun run pipelines/import/import-turath.ts --id=26                                # Import book by ID
bun run pipelines/import/import-turath.ts --id=26 --dry-run                      # Preview without writing
bun run pipelines/import/import-turath.ts --id=26 --skip-transliteration         # Skip AI transliteration
```

The Turath pipeline fetches book text, metadata, and PDFs from `api.turath.io` and `files.turath.io`. PDFs are downloaded and stored in RustFS (S3-compatible storage) and served via presigned URLs. See [scrapers/books/README.md](https://github.com/openidb/scrapers/blob/main/books/README.md) for the full pipeline documentation.

```bash
# Dictionary (requires books to be imported first)
bun run pipelines/import/dictionary/import-entries.ts \
  --book-id=23193 --slug=mukhtar --name-ar="مختار الصحاح" --name-en="Mukhtar al-Sihah" \
  --entry-pattern=spaced                                                          # Regex-based entry detection
bun run pipelines/import/dictionary/extract-definitions.ts --slug=muhit            # LLM-based extraction (Claude)
bun run pipelines/import/dictionary/import-extracted-definitions.ts --slug=muhit   # Import extracted definitions
bun run pipelines/import/dictionary/split-sub-entries.ts --all                     # Split entries into sub-entries
bun run pipelines/import/dictionary/import-roots.ts                               # Arramooz root database
bun run pipelines/import/dictionary/import-taj-derivatives.ts                     # Taj al-Arus derivatives
bun run pipelines/import/dictionary/generate-derived-forms.ts                     # Morphological pattern generation
bun run pipelines/import/dictionary/backfill-vocalized.ts                         # Vocalized headword backfill
```

See [scrapers/dictionary/README.md](https://github.com/openidb/scrapers/blob/main/dictionary/README.md) for the full dictionary list, extraction pipelines, and root derivation details.

Additional pipelines:

```bash
bun run pipelines/embed/generate-embeddings.ts               # Generate vector embeddings for Qdrant
bun run pipelines/index/sync-elasticsearch.ts                 # Sync data to Elasticsearch
bun run pipelines/knowledge-graph/seed-neo4j.ts               # Seed Neo4j knowledge graph
```

## Attribution

### Quran

| What | Source | Details |
|------|--------|---------|
| **Text** | [Al Quran Cloud API](https://alquran.cloud/) | Uthmani script (Hafs an Asim), 6,236 ayahs |
| **Translations** | [fawazahmed0/quran-api](https://github.com/fawazahmed0/quran-api) | 500+ editions, 90+ languages, via jsDelivr CDN |
| **Tafsirs** | [spa5k/tafsir_api](https://github.com/spa5k/tafsir_api) | 27 editions, 6 languages — includes Ibn Kathir, al-Jalalayn, al-Tabari, al-Muyassar |
| **Audio** | [EveryAyah.com](https://everyayah.com/) | Per-ayah MP3 recitations, ~70 reciters, multiple bitrates and styles |
| | [Al Quran Cloud CDN](https://cdn.islamic.network/) | 27 audio editions |
| | [Quran Foundation](https://quran.com/) | 9 high-quality reciters (192 kbps) |
| | [QUL / Tarteel AI](https://qul.tarteel.ai/) | Additional reciter audio |

### Hadith

- **[sunnah.com](https://sunnah.com/)** — 17 collections (49,618 hadiths) with Arabic text and English translations
- **[hadithunlocked.com](https://hadithunlocked.com/)** — 7 collections (117,346 hadiths) with full tashkeel, isnad/matn separation, scholarly grading, and human-only English translations

### Books & Dictionaries

- **[Turath.io](https://turath.io/)** — 8,500+ classical Arabic texts and 43 Arabic dictionaries (same corpus as [Shamela.ws](https://shamela.ws/))
- **[Arramooz](https://github.com/linuxscout/arramooz)** — Arabic morphological database for root derivation
- **KhorsiCorpus** — Taj al-Arus word-root derivatives for morphological pattern generation

## Part of [OpenIDB](https://github.com/openidb)

This is the API server. See also:
- [frontend](https://github.com/openidb/frontend) — Frontend (Next.js)
- [scrapers](https://github.com/openidb/scrapers) — Data acquisition
