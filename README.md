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
| Web UI | FastAPI + Jinja2 + Bootstrap 5 |
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

# Start the Web UI (from src/)
cd src && uv run uvicorn api.main:app --reload --port 8000
# Or use the helper script:
./scripts/run_ui.sh
# Open http://localhost:8000
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
│   ├── api/                # FastAPI routes and chat/source store
│   ├── UI/                 # Jinja2 templates + static CSS
│   ├── ingestion/          # RSS → Kafka pipeline
│   ├── streaming/          # PyFlink stream processing jobs
│   ├── RAG/                # Retrieval pipeline (WIP)
│   └── common/             # Shared utilities
├── data/                   # Runtime data (chat history, source status)
└── tests/
```

## Web UI

- **Chat page** — conversation with the AI agent, chat history in sidebar, create new chats
- **Sources page** — toggle news sources on/off, configure pull intervals, test feed connectivity
