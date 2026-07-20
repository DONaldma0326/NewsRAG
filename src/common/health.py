import logging

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


async def _check(fn, *args, **kw):
    try:
        result = await fn(*args, **kw)
        result["ok"] = True
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)}


async def check_kafka() -> dict:
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "_health_check",
            "session.timeout.ms": 4000,
        }
    )
    topics = consumer.list_topics(timeout=3)
    consumer.close()
    topic_names = list(topics.topics.keys()) if topics.topics else []
    return {"topics": topic_names}


async def check_chromadb() -> dict:
    import chromadb
    from chromadb.config import Settings

    client = chromadb.HttpClient(
        host=CHROMA_HOST,
        port=CHROMA_PORT,
        settings=Settings(anonymized_telemetry=False),
    )
    client.heartbeat()
    collections = client.list_collections()
    total_vectors = sum(c.count() for c in collections)
    return {"collections": [c.name for c in collections], "vectors": total_vectors}


async def check_ollama() -> dict:
    async with httpx.AsyncClient(timeout=5) as client:
        resp = await client.get(f"{OLLAMA_BASE_URL}/api/tags")
        resp.raise_for_status()
        data = resp.json()
        models = [m["name"] for m in data.get("models", [])]
        target_prefix = (
            OLLAMA_MODEL
            if OLLAMA_MODEL.endswith(":latest")
            else f"{OLLAMA_MODEL}:latest"
        )
        return {
            "models": models,
            "has_target_model": OLLAMA_MODEL in models or target_prefix in models,
        }


async def check_flink() -> dict:
    async with httpx.AsyncClient(timeout=5) as client:
        resp = await client.get(f"{FLINK_BASE_URL}/overview")
        resp.raise_for_status()
        data = resp.json()
        jobs_resp = await client.get(f"{FLINK_BASE_URL}/jobs/overview")
        jobs_data = jobs_resp.json()
        return {
            "taskmanagers": data.get("taskmanagers", 0),
            "slots_available": data.get("slots-available", 0),
            "slots_total": data.get("slots-total", 0),
            "jobs": [
                {"name": j["name"], "state": j["state"]}
                for j in jobs_data.get("jobs", [])
            ],
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
    return {"sources": len(sources), "total_articles": total, "last_run": last_run}


async def check_all() -> dict:
    import asyncio

    results = await asyncio.gather(
        _check(check_kafka),
        _check(check_chromadb),
        _check(check_ollama),
        _check(check_flink),
        _check(check_ingestion),
    )
    keys = ["kafka", "chromadb", "ollama", "flink", "ingestion"]
    return dict(zip(keys, results))
