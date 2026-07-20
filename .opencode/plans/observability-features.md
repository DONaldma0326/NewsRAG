# Observability & Monitoring — Implementation

## P0 — RAG Trace

### 1. `src/common/models.py` — add RAGTrace, extend RAGResult

```python
@dataclass
class RAGTrace:
    time_embed_ms: float = 0.0
    time_retrieve_ms: float = 0.0
    time_generate_ms: float = 0.0
    total_ms: float = 0.0
    prompt: str = ""
    model: str = ""
    tokens: int = 0
    k: int = 5

@dataclass
class RAGResult:
    answer: str
    sources: list[SearchResult] = field(default_factory=list)
    trace: RAGTrace | None = None
```

### 2. `src/RAG/generator.py` — make build_prompt public, return token count

Change `_build_prompt` to public `build_prompt`.
Change `generate()` to return `(text: str, eval_count: int)`.

```python
def build_prompt(query, context, history) -> str:
    # same body as old _build_prompt

def generate(query, context, history, model=OLLAMA_MODEL) -> tuple[str, int]:
    prompt = build_prompt(query, context, history)
    with httpx.Client(timeout=60.0) as client:
        resp = client.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={"model": model, "prompt": prompt, "system": SYSTEM_PROMPT, "stream": False},
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("response", ""), data.get("eval_count", 0)
```

### 3. `src/RAG/pipeline.py` — add timing, capture prompt

```python
import time

from common.config import MAX_HISTORY_TURNS, OLLAMA_MODEL
from common.models import RAGResult, RAGTrace
from RAG.embedder import encode
from RAG.generator import build_prompt, generate
from RAG.retriever import retrieve
from RAG.vector_store import query as vector_query

def answer(query: str, history: list[dict] | None = None) -> RAGResult:
    t0 = time.perf_counter()
    query_emb = encode([query])[0]
    t1 = time.perf_counter()
    context = vector_query(query_emb, k=RETRIEVAL_K)
    t2 = time.perf_counter()
    chat_history = _build_history(history) if history else []
    answer_text, tokens = generate(query, context, chat_history)
    t3 = time.perf_counter()
    prompt = build_prompt(query, context, chat_history)
    return RAGResult(
        answer=answer_text,
        sources=context,
        trace=RAGTrace(
            time_embed_ms=(t1 - t0) * 1000,
            time_retrieve_ms=(t2 - t1) * 1000,
            time_generate_ms=(t3 - t2) * 1000,
            total_ms=(t3 - t0) * 1000,
            prompt=prompt,
            model=OLLAMA_MODEL,
            tokens=tokens,
            k=RETRIEVAL_K,
        ),
    )

def _build_history(raw: list[dict]) -> list[dict]:
    messages = [m for m in raw if m["role"] in ("user", "assistant")]
    return messages[-MAX_HISTORY_TURNS * 2 :]
```

Need to add imports: `from common.config import OLLAMA_MODEL, RETRIEVAL_K`, `from RAG.embedder import encode`, `from RAG.vector_store import query as vector_query`, `from RAG.generator import build_prompt`

### 4. `src/api/chat.py` — add metadata field to messages

Change `add_message` to accept optional `metadata` param:

```python
def add_message(chat_id: str, role: str, content: str, metadata: dict | None = None) -> dict | None:
    ...
    msg = {
        "role": role,
        "content": content,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if metadata:
        msg["metadata"] = metadata
    chat["messages"].append(msg)
    ...
```

### 5. `src/api/routes.py` — store trace, add /dashboard + /api/health routes

Change `_generate_reply` to return trace, update `api_send_message`:

```python
def _generate_reply(user_msg: str, chat_id: str | None = None) -> tuple[str, dict | None]:
    history = None
    if chat_id:
        chat = get_chat(chat_id)
        if chat:
            history = chat.get("messages", [])
    try:
        result: RAGResult = rag_answer(user_msg, history)
        trace = dataclasses.asdict(result.trace) if result.trace else None
        return result.answer, trace
    except Exception as e:
        log.error("RAG pipeline error: %s", e)
        return "Sorry, I encountered an error processing your request.", None
```

Update `api_send_message`:
```python
@router.post("/api/chats/{chat_id}/messages")
def api_send_message(chat_id: str, body: dict):
    content = body.get("content", "")
    chat = add_message(chat_id, "user", content)
    if not chat:
        return JSONResponse({"error": "Chat not found"}, status_code=404)

    response_text, trace = _generate_reply(content, chat_id)
    chat = add_message(chat_id, "assistant", response_text, metadata=trace)
    return JSONResponse(chat)
```

Add dashboard + health routes at bottom (before helpers or after):

```python
@router.get("/dashboard")
def dashboard_page(request: Request):
    return _render(request, "dashboard.html", {})

@router.get("/api/health")
def api_health():
    from common.health import check_all
    return JSONResponse(check_all())
```

Add import: `import dataclasses` at top.

### 6. `src/UI/templates/chat.html` — add trace panel

Replace the addBubble function and message rendering. In the template, after the assistant bubble content:

```html
{% if msg.role == "assistant" and msg.metadata %}
<details class="trace-panel" data-trace='{{ msg.metadata | tojson | safe }}'>
  <summary>Show trace</summary>
  <div class="trace-content"></div>
</details>
{% endif %}
```

Update JS `addBubble` to handle trace data, and update the message rendering loop.

### 7. `src/UI/static/style.css` — trace panel styles

```css
.trace-panel {
  margin-top: 8px;
  font-size: 12px;
  background: #f8f8fc;
  border-radius: 8px;
  padding: 4px 8px;
  max-width: 75%;
  margin-right: auto;
}
.trace-panel summary {
  cursor: pointer;
  color: #666;
  font-weight: 500;
}
.trace-content {
  margin-top: 6px;
  padding: 8px;
  background: #fff;
  border-radius: 6px;
  border: 1px solid #eee;
  max-height: 400px;
  overflow-y: auto;
  font-family: monospace;
  white-space: pre-wrap;
  font-size: 11px;
  line-height: 1.4;
}
```

---

## P2 — Health API

### 8. `src/common/health.py` (new)

```python
import logging
from datetime import datetime, timezone

import httpx

from common.config import (
    CHROMA_HOST,
    CHROMA_PORT,
    KAFKA_BOOTSTRAP_SERVERS,
    OLLAMA_BASE_URL,
    OLLAMA_MODEL,
)

log = logging.getLogger(__name__)

FLINK_BASE_URL = "http://localhost:8081"


async def _check(desc: str, fn, *args, **kw):
    try:
        result = await fn(*args, **kw)
        result["ok"] = True
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)}


async def check_kafka() -> dict:
    from confluent_kafka import Consumer

    parts = KAFKA_BOOTSTRAP_SERVERS.split(",")
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "_health_check",
        "session.timeout.ms": 4000,
    })
    topics = consumer.list_topics(timeout=3)
    consumer.close()
    topic_names = list(topics.topics.keys()) if topics.topics else []
    return {"topics": topic_names}


async def check_chromadb() -> dict:
    import chromadb
    from chromadb.config import Settings

    client = chromadb.HttpClient(
        host=CHROMA_HOST, port=CHROMA_PORT,
        settings=Settings(anonymized_telemetry=False),
    )
    heartbeat = client.heartbeat()
    collections = client.list_collections()
    total_vectors = sum(c.count() for c in collections)
    return {
        "collections": [c.name for c in collections],
        "vectors": total_vectors,
    }


async def check_ollama() -> dict:
    async with httpx.AsyncClient(timeout=5) as client:
        resp = await client.get(f"{OLLAMA_BASE_URL}/api/tags")
        resp.raise_for_status()
        data = resp.json()
        models = [m["name"] for m in data.get("models", [])]
        return {
            "models": models,
            "has_target_model": OLLAMA_MODEL in models,
        }


async def check_flink() -> dict:
    async with httpx.AsyncClient(timeout=5) as client:
        resp = await client.get(f"{FLINK_BASE_URL}/overview")
        resp.raise_for_status()
        data = resp.json()
        # Also get job details
        jobs_resp = await client.get(f"{FLINK_BASE_URL}/jobs/overview")
        jobs_data = jobs_resp.json()
        return {
            "taskmanagers": data.get("taskmanagers", 0),
            "slots_available": data.get("slots-available", 0),
            "slots_total": data.get("slots-total", 0),
            "jobs": [{"name": j["name"], "state": j["state"]} for j in jobs_data.get("jobs", [])],
        }


async def check_ingestion() -> dict:
    from api.sources import get_sources
    sources = get_sources()
    total = 0
    last_run = None
    for s in sources:
        total += s.get("article_count", 0)
        lr = s.get("last_run")
        if lr and (last_run is None or lr > last_run):
            last_run = lr
    return {
        "sources": len(sources),
        "total_articles": total,
        "last_run": last_run,
    }


async def check_all() -> dict:
    import asyncio

    results = await asyncio.gather(
        _check("kafka", check_kafka),
        _check("chromadb", check_chromadb),
        _check("ollama", check_ollama),
        _check("flink", check_flink),
        _check("ingestion", check_ingestion),
    )
    keys = ["kafka", "chromadb", "ollama", "flink", "ingestion"]
    return dict(zip(keys, results))
```

---

## P1 — Dashboard

### 9. `src/UI/templates/base.html` — add Dashboard nav link

```html
<li class="nav-item">
  <a class="nav-link {{ 'active' if request.url.path.startswith('/dashboard') else '' }}" href="/dashboard">
    Dashboard
  </a>
</li>
```

### 10. `src/UI/templates/dashboard.html` (new)

Template extending base.html, with cards for each component, auto-refreshing every 10s via JS.

### 11. `src/UI/static/style.css` — dashboard card styles

```css
/* ── Dashboard ─────────────────────────────────────── */
.dashboard-container { max-width: 900px; margin: 32px auto; padding: 0 16px; }
.dashboard-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 16px; }
.dash-card { background: #fff; border-radius: 12px; padding: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
.dash-card .card-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px; }
.dash-card .card-title { font-weight: 700; font-size: 14px; }
.dash-card .card-body { font-size: 13px; color: #555; }
.dash-card .card-body dt { font-weight: 600; color: #333; }
.dash-card .card-body dd { margin-left: 0; margin-bottom: 4px; }
.status-badge { font-size: 11px; padding: 2px 8px; border-radius: 10px; font-weight: 600; }
.status-badge.ok { background: #dcfce7; color: #166534; }
.status-badge.fail { background: #fce8e8; color: #991b1b; }
```

---

## Implementation Order

1. `src/common/models.py` — extend RAGResult + RAGTrace
2. `src/RAG/generator.py` — public build_prompt, return token count
3. `src/RAG/pipeline.py` — add timing, capture prompt
4. `src/api/chat.py` — metadata field on messages
5. `src/api/routes.py` — store trace, add /dashboard + /api/health
6. `src/UI/templates/chat.html` — trace panel
7. `src/UI/static/style.css` — trace + dashboard styles
8. `src/common/health.py` — new health check module
9. `src/UI/templates/base.html` — Dashboard nav link
10. `src/UI/templates/dashboard.html` — new dashboard template
