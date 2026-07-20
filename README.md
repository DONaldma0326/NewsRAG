# NewsRAG

Real-time RAG pipeline for news data. RSS feeds → Kafka → Flink (chunking) → Embeddings → ChromaDB → LLM → Web UI.

All models run locally. No cloud APIs required.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  INGESTION LAYER                                                                 │
│                                                                                  │
│  RSS Feeds ──► news_puller.py ──► Kafka ──► Flink chunker ──► Kafka             │
│  (BBC, CNN,             (news-raw)    (splits articles    (news-chunks)          │
│   Guardian,                            into chunks)                              │
│   NPR, NYT,                                                                      │
│   ABC, Sky)                                                                      │
│                                          │                                       │
│                                          ▼                                       │
│                               rag_embed_consumer.py                              │
│                               ┌──────────────────────┐                          │
│                               │ 1. Reads chunks from  │                          │
│                               │    Kafka news-chunks  │                          │
│                               │ 2. Batches them (32)  │                          │
│                               │ 3. Encodes via        │                          │
│                               │    Sentence-Transform. │                          │
│                               │    (bge-small-en-v1.5) │                          │
│                               │ 4. Stores in ChromaDB │                          │
│                               └──────────┬───────────┘                          │
│                                          │                                       │
│                                          ▼                                       │
│                                     ChromaDB                                     │
│                                     (vector DB)                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
                                          │
                                          ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  RAG PIPELINE (live query)                                                       │
│                                                                                  │
│  User Query ──► embed query ──► ChromaDB ──► retrieve top-k ──► augment prompt  │
│                  (via same       (cosine        (sources with                    │
│                   Sentence-       similarity)    title, score,                    │
│                   Transformers)                  url, text)                      │
│                                                                                  │
│  Augmented Prompt ──► Ollama ──► Response                                       │
│  (system + context    (llama3.1)                                                 │
│   + chat history                                                                 │
│   + query)                                                                       │
└─────────────────────────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  WEB UI (FastAPI + Jinja2 + Bootstrap 5)                                        │
│                                                                                  │
│  /chat       ── Chat with RAG agent (expanded trace: timing, chunks, prompt)    │
│  /sources    ── Manage RSS feeds (toggle, interval, test)                       │
│  /dashboard  ── Health overview (Kafka, ChromaDB, Ollama, Flink, ingestion)     │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## What is the Embed Consumer?

`rag_embed_consumer.py` is a standalone **Kafka consumer** that bridges the streaming pipeline to the vector store. It does the **E** in ETL:

1. **Reads** chunked articles from Kafka topic `news-chunks` (produced by the Flink chunker)
2. **Batches** them in groups of 32 for efficiency
3. **Embeds** each chunk into a 384-dim vector using `BAAI/bge-small-en-v1.5` via Sentence-Transformers
4. **Stores** the vector + text + metadata (source, title, url, published date) in ChromaDB

```
Kafka news-chunks ──► poll(1s) ──► batch(32) ──► Sentence-Transformers ──► ChromaDB
                          │                           │
                     graceful shutdown            bge-small-en-v1.5
                     (SIGINT/SIGTERM)             384-dim, normalized
```

It uses:
- **confluent_kafka** for Kafka consumption (auto-commit every 5s)
- **sentence-transformers** for embedding (singleton model, lazy-loaded)
- **chromadb HTTP client** for vector storage
- Signal handlers for graceful shutdown

## Data Flow (end-to-end)

### Ingestion (offline, continuous)
```
Step 1: news_puller.py
        └─ polls 7 RSS feeds every 60s
        └─ deduplicates by article ID hash
        └─ publishes JSON to Kafka topic "news-raw"
          {source, title, link, published, summary, id}

Step 2: Flink job (news_chunker_job.py)
        └─ consumes "news-raw" as DataStream
        └─ splits article text into chunks (512 chars, 64 overlap)
        └─ produces JSON to Kafka topic "news-chunks"
          {article_id, chunk_index, total_chunks, chunk_text, source, title, url, published}

Step 3: rag_embed_consumer.py  ◄── THIS IS THE EMBED CONSUMER
        └─ consumes "news-chunks" in batches of 32
        └─ encodes texts → 384-dim vectors via Sentence-Transformers
        └─ inserts into ChromaDB collection "news"
```

### Query (online, per user request)
```
Step 4: User types question in /chat
Step 5: FastAPI route → RAG pipeline
Step 6: pipeline.py orchestrates:
        a) embed query (same Sentence-Transformers model)
        b) vector search ChromaDB (cosine, top-k=5)
        c) build prompt with context + chat history (last 6 turns)
        d) call Ollama (llama3.1) → get answer
Step 7: Response sent to UI with full trace (timing, chunks, prompt)
```

## Partitioning & Scalability

| Component | Scaling |
|-----------|---------|
| **news-raw** topic | 3 partitions — can parallelize RSS pollers |
| **news-chunks** topic | 3 partitions — can parallelize embed consumers |
| **Flink chunker** | 1 slot (configurable) — stateless, can increase parallelism |
| **Embed consumer** | Single process, batch size 32 — can run multiple instances with same group.id for consumer-group balancing |
| **ChromaDB** | Single node (HTTP) — supports async, can scale to distributed mode |

## Tech Stack

| Category | Choice |
|----------|--------|
| Language | Python 3.12+ |
| Package manager | `uv` |
| Message broker | Apache Kafka (KRaft mode, no Zookeeper) |
| Stream processing | PyFlink (Apache Flink) |
| Vector store | ChromaDB (HTTP client) |
| Embedding | Sentence-Transformers (`BAAI/bge-small-en-v1.5`, 384-dim) |
| LLM | Ollama (`llama3.1`, native on host) |
| Web UI | FastAPI + Jinja2 + Bootstrap 5 |

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Ollama](https://ollama.com/) (`curl -fsSL https://ollama.com/install.sh | sh`)

## Setup

```bash
# 1. Create venv and install deps
uv venv
source .venv/bin/activate
uv sync

# 2. Pull the LLM model
ollama pull llama3.1

# 3. Start infrastructure (Kafka + ChromaDB + Flink)
docker compose up -d

# 4. Start the embedding consumer (consumes news-chunks, embeds, stores in ChromaDB)
PYTHONPATH=src uv run python src/ingestion/rag_embed_consumer.py &

# 5. Submit the Flink chunker job (consumes news-raw, chunks, emits to news-chunks)
docker exec -i rag-jobmanager-1 flink run -d -py /opt/src/streaming/news_chunker_job.py

# 6. Start RSS puller (publishes articles from RSS feeds to Kafka)
uv run python src/ingestion/news_puller.py &

# 7. Start the web UI at http://localhost:8080
PORT=8080 PYTHONPATH=src uv run uvicorn api.main:app --host 0.0.0.0 --port 8080
```

## Web UI

| Page | Route | Description |
|------|-------|-------------|
| **Chat** | `/chat` | Conversation with the AI agent grounded in news articles. Each assistant response has an expandable **trace** showing retrieval details, timing, and the full prompt sent to Ollama. |
| **Sources** | `/sources` | Toggle news sources, configure pull intervals, test feeds |
| **Dashboard** | `/dashboard` | Pipeline health overview — status cards for Kafka, ChromaDB, Ollama, Flink, and ingestion. Auto-refreshes every 10s via `GET /api/health`. |

### RAG Trace

Every assistant message includes an expandable **Show trace** panel with:

- **Timing breakdown**: Embed, vector search, and LLM generation times
- **Retrieved chunks**: source, title, similarity score
- **Model info**: model name, token count
- **Full prompt**: the exact prompt sent to Ollama (system + context + history + query)

## Health API

```json
GET /api/health → {
  "kafka":       { "ok": true/false, "topics": ["news-raw", "news-chunks"] },
  "chromadb":    { "ok": true/false, "collections": ["news"], "vectors": 3400 },
  "ollama":      { "ok": true/false, "models": ["llama3.1:latest"], "has_target_model": true },
  "flink":       { "ok": true/false, "jobs": [{"name":"News Chunker Job","state":"RUNNING"}] },
  "ingestion":   { "ok": true/false, "total_articles": 92, "last_run": "..." }
}
```

## Testing

```bash
# Run all tests
uv run pytest tests/

# Test chunking logic only (no external dependencies)
uv run pytest tests/common/test_text_splitter.py -v
```

## Project Structure

```
├── AGENTS.md              # AI coding conventions
├── pyproject.toml          # Dependencies and tool config
├── docker-compose.yml      # Kafka + ChromaDB + Flink
├── config/
│   └── sources.json        # RSS feed URLs (BBC, Guardian, NPR, etc.)
├── docker/
│   └── Dockerfile.flink    # Flink image with Kafka connector
├── src/
│   ├── api/                # FastAPI routes, chat store, source management
│   ├── UI/                 # Jinja2 templates + CSS (chat, sources, dashboard)
│   ├── ingestion/
│   │   ├── news_puller.py        # RSS → Kafka producer (6 feeds, 60s interval)
│   │   └── rag_embed_consumer.py # Kafka → embed → ChromaDB (batch consumer)
│   ├── streaming/
│   │   └── news_chunker_job.py   # PyFlink: text splitter Kafka→Kafka job
│   ├── RAG/
│   │   ├── embedder.py      # Lazy-loaded Sentence-Transformers singleton
│   │   ├── vector_store.py  # ChromaDB HTTP client (add/query)
│   │   ├── retriever.py     # Embed query → vector search
│   │   ├── generator.py     # Ollama API client (prompt builder)
│   │   └── pipeline.py      # Orchestrator: embed → retrieve → generate + trace
│   └── common/
│       ├── config.py        # Env-based configuration
│       ├── models.py        # Pydantic models (Chunk, SearchResult, RAGResult, RAGTrace)
│       ├── exceptions.py    # Custom exceptions (RAGError, EmbeddingError, etc.)
│       ├── text_splitter.py # RecursiveCharacterTextSplitter (used by Flink)
│       └── health.py        # Async health checks for all components
├── data/                   # Runtime state (chat history, source status)
└── tests/                  # Pytest suite
```

## Services (Docker Compose)

| Service | Port | Purpose |
|---------|------|---------|
| Kafka | 19092 | Message broker (KRaft mode) |
| ChromaDB | 8000 | Vector database |
| Flink JobManager | 8081 | Flink cluster manager / web UI |
| Flink TaskManager | — | Flink worker (1 slot) |
