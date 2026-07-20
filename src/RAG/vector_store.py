from __future__ import annotations

import logging

import chromadb
from chromadb.config import Settings

from common.config import (
    CHROMA_COLLECTION,
    CHROMA_HOST,
    CHROMA_PORT,
)
from common.exceptions import VectorStoreError
from common.models import Chunk, SearchResult

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
