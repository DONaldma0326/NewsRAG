# AGENTS.md — Project Conventions for AI Coding Agents

This file defines conventions, structure, and behavior guidelines for AI agents (Claude, Copilot, Cursor, etc.) working on this project. It is application-agnostic — adapt the details below to any similar project.

> [!IMPORTANT]
> **NEVER commit, push, or create PRs without asking the user first.** Always get explicit approval before any git write operation.

---

## 1. Repository Structure

```
/
├── AGENTS.md           # This file
├── pyproject.toml       # Project metadata, dependencies, tool config
├── docker-compose.yml   # Local infrastructure (Kafka, Flink, ChromaDB)
├── Dockerfile           # App container
├── config/              # Runtime configuration files
│   └── sources.json     # RSS feed sources
├── data/                # Runtime data (chat history, state, metrics)
├── scripts/             # Utility scripts (setup, migrate, seed)
├── tests/               # All tests, mirroring src/ structure
│   └── <module>/
│       └── test_<name>.py
├── docker/              # Docker support files (Dockerfiles, init scripts)
└── src/
    ├── __init__.py
    ├── UI/              # Web interface (FastAPI + Jinja2 templates)
    │   ├── static/
    │   └── templates/
    ├── RAG/             # LangGraph-based RAG pipeline
    │   ├── graph.py     # LangGraph StateGraph definition
    │   ├── pipeline.py  # Entry point (answer())
    │   ├── embedder.py  # LangChain OllamaEmbeddings wrapper
    │   ├── retriever.py # LangChain Chroma retriever
    │   ├── reranker.py  # Cross-encoder / score reranking
    │   ├── generator.py # LangChain ChatOllama generation
    │   └── vector_store.py  # ChromaDB HTTP client (used by Flink + API)
    ├── streaming/       # PyFlink stream processing jobs
    │   └── news_pipeline_job.py  # Chunk + embed + store + metrics
    ├── ingestion/       # Data ingestion from news sources
    │   ├── news_puller.py    # RSS poller → Kafka (news-raw)
    │   └── news_consumer.py  # Debug consumer (optional)
    ├── api/             # FastAPI REST/UI layer
    │   ├── main.py      # App setup
    │   ├── routes.py    # HTTP endpoints
    │   ├── chat.py      # Chat CRUD (JSON file store)
    │   └── sources.py   # Source config + status
    └── common/          # Shared utilities, models, config
        ├── config.py
        ├── exceptions.py
        ├── health.py
        ├── models.py
        └── text_splitter.py
```

### Principles
- **Monorepo**: All components live in one repository under `src/`.
- **Flat is better than nested**: Prefer `src/<module>/<file>.py` over deep hierarchy.
- **Tests mirror source**: `tests/<module>/test_<name>.py` matches `src/<module>/<name>.py`.

---

## 2. Tech Stack

| Category        | Choice                        |
|-----------------|-------------------------------|
| Language        | Python 3.12+                  |
| Package manager | `uv`                          |
| Build system    | `pyproject.toml`              |
| Stream process  | PyFlink (Apache Flink)        |
| Message broker  | Apache Kafka                   |
| Vector store    | ChromaDB (HTTP API)           |
| RAG framework   | LangGraph + LangChain         |
| LLM             | Ollama (llama3.1)             |
| Emb.model       | BAAI/bge-small-en-v1.5        |
| Web UI          | FastAPI + Jinja2              |
| Infra           | Docker Compose (local dev)    |
| Testing         | pytest                        |
| Formatting      | Black                         |
| Linting         | ruff                          |

---

## 3. Coding Conventions

### Python
- **Formatter**: Black (default settings, line length 88).
- **Linter**: ruff with sensible defaults.
- **Naming**: `snake_case` for files, functions, variables; `CamelCase` for classes; `UPPER_CASE` for constants.
- **Imports**: No strict rule — use absolute or relative imports consistently within a file. Group: stdlib → third-party → local, separated by blank lines.
- **Type hints**: Use type annotations for all public function signatures. Optional for internal helpers.
- **Comments**: Comments welcome for complex logic. Avoid redundant/obvious comments.
- **Error handling**: Use custom exception classes defined in `src/common/exceptions.py`. Prefer early returns over deep nesting.

### File conventions
- One class per file (unless closely related).
- Module `__init__.py` may re-export key symbols for a clean public API.
- Configuration lives in `config/`, not hardcoded.

### Data Pipeline

```
RSS sources → [news_puller] → Kafka news-raw → [Flink: news_pipeline_job] → ChromaDB
                                                                              ↓
                                                                        Kafka news-metrics
                                                                              ↓
User query → [LangGraph: rewrite → retrieve → rerank → generate] → Answer + trace
```

- **Ingestion**: `news_puller.py` polls RSS feeds, publishes articles to Kafka topic `news-raw`.
- **Streaming**: `news_pipeline_job.py` (PyFlink) reads `news-raw`, chunks articles, embeds via Ollama API, writes to ChromaDB, and emits source-level metrics to Kafka topic `news-metrics`.
- **RAG**: LangGraph `StateGraph` with four nodes: `rewrite_query` (rephrase with chat history), `retrieve` (ChromaDB vector search), `rerank` (score reordering), `generate` (Ollama LLM call). Chat history is part of graph state.
- **API**: FastAPI serves Jinja2 web UI, manages chat sessions (JSON file store), reads monitoring stats from `news-metrics` topic.

### Testing
- Use pytest exclusively.
- Test files named `test_<name>.py`.
- Prefer `tmp_path` fixture over hardcoded temp dirs.
- Use `pytest.mark.parametrize` for data-driven tests.
- Aim for one assertion per test (or logical group).

---

## 4. Development Workflow

```bash
# Setup
uv venv
source .venv/bin/activate
uv sync

# Run tests
uv run pytest

# Format code
uv run black src/ tests/

# Lint
uv run ruff check src/ tests/

# Type check (if configured)
uv run mypy src/
```

### Docker environment
```bash
docker compose up -d          # Start Kafka, Flink, etc.
docker compose logs -f        # Tail logs
docker compose down           # Stop everything
```

---

## 5. Agent Behavior Guidelines

These rules govern how AI agents should operate when editing this codebase.

### General Rules
- **Do not add explanations or summaries** after writing code unless explicitly asked.
- **Do not add inline comments** unless the logic is genuinely non-obvious.
- **Do not commit, push, or create PRs** unless explicitly instructed.
- **Do not create new files** unless necessary — prefer editing existing ones.
- **Do not assume libraries are available** — always check `pyproject.toml` first.
- **Do not generate docstrings** unless the user requests them.
- **Never hardcode secrets, API keys, or configs** — use environment variables or config files.
- **Never create README.md or documentation files** unless explicitly asked.

### Before Writing Code
1. Read the relevant file(s) to understand existing conventions.
2. Check `pyproject.toml` for available dependencies before importing anything.
3. Check neighboring files for patterns (naming, structure, imports).
4. For cross-cutting changes, search the codebase first with grep/glob.

### When Making Changes
- **Mimic existing code style** — match indentation, quoting, line breaks.
- **Minimize diffs** — make targeted changes, not wholesale rewrites.
- **Prefer composition over inheritance**.
- **Keep functions small** — extract helpers when a function exceeds ~30 lines.
- **Use existing utilities** in `src/common/` before writing new ones.

### After Writing Code
- Run `uv run black src/ tests/` to format.
- Run `uv run ruff check src/ tests/` to lint.
- Run relevant tests: `uv run pytest tests/<module>/`.
- Fix any lint/type/test failures before declaring the task done.

### Communication Style
- Keep responses concise — 1–3 sentences unless detail is requested.
- When referencing code, use `file_path:line_number` format.
- Do not use emoji unless the user uses them first.
- Answer the question directly without preamble or postamble.

---

## 6. Git Conventions

- **No automatic commits** — wait for explicit user request.
- Commit messages: concise, imperative mood, matching repo style.
- Before committing: always check `git status`, `git diff`, and recent history.
- Never force-push, skip hooks, or use interactive rebase unless asked.
