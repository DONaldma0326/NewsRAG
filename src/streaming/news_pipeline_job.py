import json
import logging
import os
import sys
import time
from collections import defaultdict

import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import RichMapFunction

from common.text_splitter import RecursiveCharacterTextSplitter

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_SRC_TOPIC = os.getenv("KAFKA_SRC_TOPIC", "news-raw")
KAFKA_METRICS_TOPIC = os.getenv("KAFKA_METRICS_TOPIC", "news-metrics")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "flink-news-pipeline")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "512"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "64"))
EMBED_BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", "32"))
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text")
CHROMA_HOST = os.getenv("CHROMA_HOST", "chromadb")
CHROMA_PORT = int(os.getenv("CHROMA_PORT", "8000"))
CHROMA_COLLECTION = os.getenv("CHROMA_COLLECTION", "news")

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
    return [
        json.dumps(
            {
                "source": source,
                "title": title,
                "url": link,
                "published": published,
                "article_id": article_id,
                "chunk_index": i,
                "chunk_text": chunk_text,
            }
        )
        for i, chunk_text in enumerate(chunks)
    ]


class EmbedStoreMetricsMap(RichMapFunction):

    def open(self, runtime_context):
        self._buffer: list[dict] = []
        self._source_stats: dict[str, dict] = defaultdict(
            lambda: {"articles": set(), "chunks": 0, "latencies": []}
        )
        self._http = httpx.Client(timeout=30.0)
        self._chroma = httpx.Client(
            base_url=f"http://{CHROMA_HOST}:{CHROMA_PORT}",
            timeout=30.0,
        )
        self._chunks_processed = 0

    def close(self):
        if self._buffer:
            self._flush_batch()
        self._http.close()
        self._chroma.close()

    def map(self, chunk_json: str) -> str | None:
        chunk = json.loads(chunk_json)
        self._buffer.append(chunk)
        source = chunk["source"]
        stats = self._source_stats[source]
        stats["chunks"] += 1
        stats["articles"].add(chunk["article_id"])

        if len(self._buffer) >= EMBED_BATCH_SIZE:
            return self._flush_batch()
        return None

    def _flush_batch(self) -> str | None:
        if not self._buffer:
            return None

        texts = [c["chunk_text"] for c in self._buffer]
        t0 = time.perf_counter()

        try:
            embeddings = self._call_ollama_embed(texts)
        except Exception as e:
            log.error("Embedding batch failed: %s", e)
            self._buffer.clear()
            return None

        t1 = time.perf_counter()
        batch_latency_ms = (t1 - t0) * 1000

        ids = [f"{c['article_id']}_{c['chunk_index']}" for c in self._buffer]
        documents = texts
        metadatas = [
            {
                "source": c["source"],
                "title": c["title"],
                "url": c["url"],
                "published": c["published"],
                "chunk_index": c["chunk_index"],
                "article_id": c["article_id"],
            }
            for c in self._buffer
        ]

        try:
            self._write_chroma(ids, documents, embeddings, metadatas)
            log.info("Stored %d chunks to ChromaDB", len(self._buffer))
        except Exception as e:
            log.error("ChromaDB write failed: %s", e)
            self._buffer.clear()
            return None

        for s in self._source_stats.values():
            s["latencies"].append(batch_latency_ms)

        self._chunks_processed += len(self._buffer)
        self._buffer.clear()
        return self._emit_metrics()

    def _call_ollama_embed(self, texts: list[str]) -> list[list[float]]:
        resp = self._http.post(
            f"{OLLAMA_BASE_URL}/api/embed",
            json={"model": EMBEDDING_MODEL, "input": texts},
        )
        resp.raise_for_status()
        data = resp.json()
        result = data.get("embeddings", [])
        if not result:
            raise RuntimeError("No embeddings returned from Ollama")
        return result

    def _write_chroma(
        self,
        ids: list[str],
        documents: list[str],
        embeddings: list[list[float]],
        metadatas: list[dict],
    ):
        payload = {
            "ids": ids,
            "documents": documents,
            "embeddings": embeddings,
            "metadatas": metadatas,
        }
        resp = self._chroma.post(
            f"/api/v1/collections/{CHROMA_COLLECTION}/add",
            json=payload,
        )
        resp.raise_for_status()

    def _emit_metrics(self) -> str:
        now = time.time()
        metrics = []
        for source, stats in self._source_stats.items():
            if stats["chunks"] == 0:
                continue
            avg_latency = (
                sum(stats["latencies"]) / len(stats["latencies"])
                if stats["latencies"]
                else 0
            )
            metrics.append(
                json.dumps(
                    {
                        "source": source,
                        "articles_count": len(stats["articles"]),
                        "chunks_count": stats["chunks"],
                        "avg_latency_ms": round(avg_latency, 1),
                        "window_end": now,
                    }
                )
            )
        self._source_stats.clear()
        return "\n".join(metrics)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

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

    metrics_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(KAFKA_METRICS_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    ds = env.from_source(
        source, WatermarkStrategy.for_monotonous_timestamps(), "kafka-news-source"
    )

    chunked = ds.flat_map(chunk_article, output_type=Types.STRING())
    metrics_stream = chunked.map(EmbedStoreMetricsMap(), output_type=Types.STRING())
    metrics_stream.filter(lambda m: m is not None).sink_to(metrics_sink)

    env.execute("News Pipeline Job")


if __name__ == "__main__":
    main()
