# NewsRAG

Real-time RAG pipeline for news data with Kafka + PyFlink.

## Architecture

```
RSS feeds ──► news_puller.py ──► Kafka (topic: news-raw) ──► PyFlink job ──► Vector DB
                                                                                  │
                                                                                  ▼
                                                                              RAG + LLM
                                                                                  │
                                                                                  ▼
                                                                              Web UI
```

## Tech Stack

| Category | Choice |
|----------|--------|
| Language | Python 3.12+ |
| Package manager | `uv` |
| Stream processing | PyFlink (Apache Flink) |
| Message broker | Apache Kafka |
| Web UI | TBD |
| Infra | Docker Compose |

## Getting Started

```bash
# Setup
uv venv
source .venv/bin/activate
uv sync --extra dev

# Start infrastructure (Kafka + Flink)
docker compose up -d

# Pull news from RSS feeds into Kafka
uv run python src/ingestion/news_puller.py

# Verify with a local consumer
uv run python src/ingestion/news_consumer.py
```

## Project Structure

```
├── AGENTS.md              # Conventions for AI coding agents
├── pyproject.toml          # Dependencies and tool config
├── docker-compose.yml      # Kafka + Flink infra
├── config/
│   └── sources.json        # RSS feed URLs
├── docker/
│   └── Dockerfile.flink    # Flink image with Kafka connector
├── src/
│   ├── ingestion/          # RSS → Kafka pipeline
│   ├── streaming/          # PyFlink stream processing jobs
│   ├── RAG/                # Retrieval pipeline (WIP)
│   ├── UI/                 # Web interface (WIP)
│   ├── api/                # REST layer (WIP)
│   └── common/             # Shared utilities
└── tests/
```

## License

MIT
