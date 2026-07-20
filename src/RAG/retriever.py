from common.config import RETRIEVAL_K
from common.models import SearchResult
from RAG.embedder import encode
from RAG.vector_store import query as vector_query


def retrieve(query_text: str, k: int = RETRIEVAL_K) -> list[SearchResult]:
    embedding = encode([query_text])[0]
    return vector_query(embedding, k=k)
