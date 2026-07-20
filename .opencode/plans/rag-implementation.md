# RAG Pipeline Implementation Plan

## Architecture

```
                    ┌──────────────────────┐
                    │    Ollama (native)    │
                    │  localhost:11434      │
                    │  llama3.1 8B         │
                    └──────────┬───────────┘
                               │
                               ▼
┌──────────────┐   ┌───────────────────────────────────────────┐
│ news_puller  │──▶│  Kafka :19092                             │
│ (existing)   │   │  topics: news-raw, news-chunks            │
└──────────────┘   └──────────────┬────────────────────────────┘
                                  │
                                  ▼ consume
                    ┌──────────────────────────────┐
                    │  Flink: news_chunker_job     │
                    │  • Python UDF chunking       │
                    │  • writes to news-chunks     │
                    └──────────────┬───────────────┘
                                  │
                                  ▼
                    ┌──────────────────────────────┐
                    │  Kafka: news-chunks          │
                    └──────────────┬───────────────┘
                                  │
                                  ▼ consume
                    ┌──────────────────────────────┐
                    │  rag_embed_consumer (Python) │
                    │  • sentence-transformers     │
                    │  • stores in ChromaDB        │
                    └──────────────┬───────────────┘
                                  │
                                  ▼
                    ┌──────────────────────────────┐
                    │  ChromaDB (Docker :8000)     │
                    └──────────────┬───────────────┘
                                  │
                                  ▼ query
User ──▶ FastAPI ──▶ RAG Pipeline ──▶ Ollama ──▶ Response
```

## Files to Create (11)

### Phase 1: Common Utilities

#### `src/common/config.py`
```python
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[2]

# ChromaDB
CHROMA_HOST = os.getenv("CHROMA_HOST", "localhost")
CHROMA_PORT = int(os.getenv("CHROMA_PORT", "8000"))
CHROMA_COLLECTION = os.getenv("CHROMA_COLLECTION", "news")

# Embedding
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5")
EMBEDDING_DIM = 384

# Chunking
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "512"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "64"))

# Kafka (for embed consumer)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_CHUNKS_TOPIC = os.getenv("KAFKA_CHUNKS_TOPIC", "news-chunks")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "rag-embed-consumer")

# Ollama
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1")

# RAG
RETRIEVAL_K = int(os.getenv("RETRIEVAL_K", "5"))
MAX_HISTORY_TURNS = int(os.getenv("MAX_HISTORY_TURNS", "6"))
```

#### `src/common/models.py`
```python
from dataclasses import dataclass, field


@dataclass
class Chunk:
    text: str
    source: str
    title: str
    url: str
    published: str
    chunk_index: int
    article_id: str
    embedding: list[float] | None = None


@dataclass
class SearchResult:
    text: str
    source: str
    title: str
    url: str
    published: str
    score: float


@dataclass
class ChatMessage:
    role: str
    content: str


@dataclass
class RAGResult:
    answer: str
    sources: list[SearchResult] = field(default_factory=list)
```

#### `src/common/exceptions.py`
```python
class RAGError(Exception):
    pass


class EmbeddingError(RAGError):
    pass


class VectorStoreError(RAGError):
    pass


class LLMError(RAGError):
    pass
```

### Phase 2: RAG Core

#### `src/RAG/embedder.py`
```python
import logging

from sentence_transformers import SentenceTransformer

from src.common.config import EMBEDDING_MODEL

log = logging.getLogger(__name__)

_model: SentenceTransformer | None = None


def get_embedder() -> SentenceTransformer:
    global _model
    if _model is None:
        log.info("Loading embedding model: %s", EMBEDDING_MODEL)
        _model = SentenceTransformer(EMBEDDING_MODEL, trust_remote_code=True)
    return _model


def encode(texts: list[str]) -> list[list[float]]:
    model = get_embedder()
    embeddings = model.encode(texts, normalize_embeddings=True, show_progress_bar=False)
    return embeddings.tolist()
```

#### `src/RAG/vector_store.py`
```python
import logging

import chromadb
from chromadb.config import Settings

from src.common.config import (
    CHROMA_COLLECTION,
    CHROMA_HOST,
    CHROMA_PORT,
    EMBEDDING_DIM,
)
from src.common.exceptions import VectorStoreError
from src.common.models import Chunk, SearchResult

log = logging.getLogger(__name__)

_client: chromadb.HttpClient | None = None
_collection: chromadb.Collection | None = None


def _get_client() -> chromadb.HttpClient:
    global _client
    if _client is None:
        _client = chromadb.HttpClient(
            host=CHROMA_HOST,
            port=CHROMA_PORT,
            settings=Settings(anonymized_telemetry=False),
        )
    return _client


def _get_collection() -> chromadb.Collection:
    global _collection
    if _collection is None:
        client = _get_client()
        _collection = client.get_or_create_collection(
            name=CHROMA_COLLECTION,
            metadata={"hnsw:space": "cosine"},
        )
    return _collection


def _chunk_id(chunk: Chunk) -> str:
    return f"{chunk.article_id}_{chunk.chunk_index}"


def add_chunks(chunks: list[Chunk]) -> int:
    try:
        collection = _get_collection()
        ids = [_chunk_id(c) for c in chunks]
        documents = [c.text for c in chunks]
        metadatas = [
            {
                "source": c.source,
                "title": c.title,
                "url": c.url,
                "published": c.published,
                "chunk_index": c.chunk_index,
                "article_id": c.article_id,
            }
            for c in chunks
        ]
        embeddings = [c.embedding for c in chunks]
        collection.add(
            ids=ids,
            documents=documents,
            metadatas=metadatas,
            embeddings=embeddings,
        )
        return len(chunks)
    except Exception as e:
        raise VectorStoreError(f"Failed to add chunks: {e}") from e


def query(embedding: list[float], k: int = 5) -> list[SearchResult]:
    try:
        collection = _get_collection()
        results = collection.query(
            query_embeddings=[embedding],
            n_results=k,
            include=["documents", "metadatas", "distances"],
        )
        if not results["ids"] or not results["ids"][0]:
            return []
        out = []
        for doc, meta, dist in zip(
            results["documents"][0],
            results["metadatas"][0],
            results["distances"][0],
        ):
            out.append(
                SearchResult(
                    text=doc,
                    source=meta.get("source", ""),
                    title=meta.get("title", ""),
                    url=meta.get("url", ""),
                    published=meta.get("published", ""),
                    score=1.0 - dist,
                )
            )
        return out
    except Exception as e:
        raise VectorStoreError(f"Failed to query vector store: {e}") from e
```

#### `src/RAG/retriever.py`
```python
from src.common.config import RETRIEVAL_K
from src.common.models import SearchResult
from src.RAG.embedder import encode
from src.RAG.vector_store import query


def retrieve(query_text: str, k: int = RETRIEVAL_K) -> list[SearchResult]:
    embedding = encode([query_text])[0]
    return query(embedding, k=k)
```

#### `src/RAG/generator.py`
```python
import logging

import httpx

from src.common.config import OLLAMA_BASE_URL, OLLAMA_MODEL
from src.common.exceptions import LLMError
from src.common.models import ChatMessage, SearchResult

log = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a helpful news assistant. Answer the user's question based on the provided context.
If the context doesn't contain enough information, say so clearly.
Keep answers concise and cite the source name when possible."""


def _build_prompt(
    query: str, context: list[SearchResult], history: list[ChatMessage]
) -> str:
    parts = []
    if context:
        parts.append("Relevant news articles:")
        for i, r in enumerate(context, 1):
            parts.append(f"[{i}] ({r.source}) {r.title}")
            parts.append(f"    {r.text}")
            parts.append("")

    if history:
        parts.append("Conversation history:")
        for msg in history:
            role = "User" if msg.role == "user" else "Assistant"
            parts.append(f"{role}: {msg.content}")
        parts.append("")

    parts.append(f"Question: {query}")
    parts.append("Answer:")
    return "\n".join(parts)


def generate(
    query: str,
    context: list[SearchResult],
    history: list[ChatMessage],
    model: str = OLLAMA_MODEL,
) -> str:
    prompt = _build_prompt(query, context, history)
    try:
        with httpx.Client(timeout=60.0) as client:
            resp = client.post(
                f"{OLLAMA_BASE_URL}/api/generate",
                json={"model": model, "prompt": prompt, "system": SYSTEM_PROMPT, "stream": False},
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("response", "")
    except Exception as e:
        raise LLMError(f"Ollama generation failed: {e}") from e
```

#### `src/RAG/pipeline.py`
```python
import logging

from src.common.config import MAX_HISTORY_TURNS
from src.common.models import ChatMessage, RAGResult
from src.RAG.generator import generate
from src.RAG.retriever import retrieve

log = logging.getLogger(__name__)


def answer(query: str, history: list[dict] | None = None) -> RAGResult:
    context = retrieve(query)
    chat_history = _build_history(history) if history else []
    answer_text = generate(query, context, chat_history)
    return RAGResult(answer=answer_text, sources=context)


def _build_history(raw: list[dict]) -> list[ChatMessage]:
    messages = [
        ChatMessage(role=m["role"], content=m["content"])
        for m in raw
        if m["role"] in ("user", "assistant")
    ]
    return messages[-MAX_HISTORY_TURNS * 2 :]
```

### Phase 3: Ingestion

#### `src/streaming/news_chunker_job.py`
```python
import json
import logging
import os

from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.formats.json import JsonRowDeserializationSchema

from langchain_text_splitters import RecursiveCharacterTextSplitter

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_SRC_TOPIC = os.getenv("KAFKA_SRC_TOPIC", "news-raw")
KAFKA_DST_TOPIC = os.getenv("KAFKA_DST_TOPIC", "news-chunks")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "flink-news-chunker")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "512"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "64"))

_splitter = RecursiveCharacterTextSplitter(
    chunk_size=CHUNK_SIZE,
    chunk_overlap=CHUNK_OVERLAP,
    separators=["\n\n", "\n", ". ", " "],
)


def chunk_article(row: tuple) -> list[str]:
    source, title, link, published, summary, article_id = row
    if not summary:
        return []
    chunks = _splitter.split_text(summary)
    results = []
    for i, chunk_text in enumerate(chunks):
        result = json.dumps({
            "source": source,
            "title": title,
            "url": link,
            "published": published,
            "article_id": article_id,
            "chunk_index": i,
            "chunk_text": chunk_text,
        })
        results.append(result)
    return results


def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    deserialization_schema = (
        JsonRowDeserializationSchema.builder()
        .type_info(
            Types.ROW_NAMED(
                ["source", "title", "link", "published", "summary", "id"],
                [
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                ],
            )
        )
        .build()
    )

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(KAFKA_SRC_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(deserialization_schema)
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(KAFKA_DST_TOPIC)
            .set_value_serialization_schema(
                SimpleStringSchema()
            )
            .build()
        )
        .build()
    )

    ds = env.from_source(
        source, WatermarkStrategy.for_monotonous_timestamps(), "kafka-news-source"
    )

    ds.flat_map(chunk_article, output_type=Types.STRING()).sink_to(sink)

    env.execute("News Chunker Job")


if __name__ == "__main__":
    main()
```

#### `src/ingestion/rag_embed_consumer.py`
```python
import json
import logging
import signal
import sys
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException

from src.common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CHUNKS_TOPIC,
    KAFKA_GROUP_ID,
)
from src.common.models import Chunk
from src.RAG.embedder import encode
from src.RAG.vector_store import add_chunks

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

_running = True


def _signal_handler(signum, frame):
    global _running
    log.info("Shutting down...")
    _running = False


def main():
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
        }
    )
    consumer.subscribe([KAFKA_CHUNKS_TOPIC])
    log.info("Embed consumer subscribed to %s", KAFKA_CHUNKS_TOPIC)

    poll_timeout = 1.0
    batch: list[Chunk] = []
    batch_size = 32

    try:
        while _running:
            msg = consumer.poll(poll_timeout)
            if msg is None:
                if batch:
                    _flush(batch)
                    batch.clear()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka error: %s", msg.error())
                continue

            chunk = _parse_chunk(msg.value())
            if chunk:
                batch.append(chunk)

            if len(batch) >= batch_size:
                _flush(batch)
                batch.clear()

        if batch:
            _flush(batch)

    finally:
        consumer.close()


def _parse_chunk(value: bytes) -> Chunk | None:
    try:
        data = json.loads(value.decode())
        return Chunk(
            text=data["chunk_text"],
            source=data["source"],
            title=data["title"],
            url=data["url"],
            published=data["published"],
            chunk_index=data["chunk_index"],
            article_id=data["article_id"],
        )
    except (json.JSONDecodeError, KeyError) as e:
        log.warning("Failed to parse chunk: %s", e)
        return None


def _flush(chunks: list[Chunk]):
    if not chunks:
        return
    try:
        texts = [c.text for c in chunks]
        embeddings = encode(texts)
        for chunk, emb in zip(chunks, embeddings):
            chunk.embedding = emb
        count = add_chunks(chunks)
        log.info("Embedded and stored %d chunks", count)
    except Exception as e:
        log.error("Failed to flush batch: %s", e)


if __name__ == "__main__":
    main()
```

### Phase 4: API Integration

#### Update `src/api/routes.py`
Replace the `_generate_reply` function with:

```python
from src.RAG.pipeline import answer as rag_answer
from src.common.models import RAGResult

def _generate_reply(user_msg: str, chat_id: str | None = None) -> str:
    history = None
    if chat_id:
        chat = get_chat(chat_id)
        if chat:
            history = chat.get("messages", [])
    try:
        result: RAGResult = rag_answer(user_msg, history)
        return result.answer
    except Exception as e:
        log.error("RAG pipeline error: %s", e)
        return f"Sorry, I encountered an error processing your request."
```

And update `api_send_message` to pass `chat_id`:

```python
@router.post("/api/chats/{chat_id}/messages")
def api_send_message(chat_id: str, body: dict):
    content = body.get("content", "")
    chat = add_message(chat_id, "user", content)
    if not chat:
        return JSONResponse({"error": "Chat not found"}, status_code=404)

    response_text = _generate_reply(content, chat_id)
    chat = add_message(chat_id, "assistant", response_text)
    return JSONResponse(chat)
```

## Implementation Order

1. `src/common/config.py`
2. `src/common/models.py`
3. `src/common/exceptions.py`
4. `src/RAG/embedder.py`
5. `src/RAG/vector_store.py`
6. `src/RAG/retriever.py`
7. `src/RAG/generator.py`
8. `src/RAG/pipeline.py`
9. `src/streaming/news_chunker_job.py`
10. `src/ingestion/rag_embed_consumer.py`
11. Update `pyproject.toml` (add deps)
12. Update `docker-compose.yml` (add chromadb + news-chunks topic)
13. Update `src/api/routes.py` (integrate RAG)

## Setup Steps After Implementation

```bash
# 1. Install new deps
uv sync

# 2. Start Docker services
docker compose up -d

# 3. Pull Ollama model
ollama pull llama3.1

# 4. Start the Kafka consumer for embedding
uv run python -m src.ingestion.rag_embed_consumer &

# 5. Submit Flink chunker job
docker exec -it rag-jobmanager-1 flink run -py /opt/src/streaming/news_chunker_job.py

# 6. Start RSS puller (publishes to Kafka)
uv run python -m src.ingestion.news_puller &

# 7. Start FastAPI UI
uv run uvicorn src.api.main:app --reload
```
