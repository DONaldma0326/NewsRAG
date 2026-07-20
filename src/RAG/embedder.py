import logging

from langchain_ollama import OllamaEmbeddings

from common.config import EMBEDDING_MODEL, OLLAMA_BASE_URL

log = logging.getLogger(__name__)

_embeddings: OllamaEmbeddings | None = None


def get_embeddings() -> OllamaEmbeddings:
    global _embeddings
    if _embeddings is None:
        log.info("Initializing Ollama embeddings: %s", EMBEDDING_MODEL)
        _embeddings = OllamaEmbeddings(
            model=EMBEDDING_MODEL,
            base_url=OLLAMA_BASE_URL,
        )
    return _embeddings


def encode(texts: list[str]) -> list[list[float]]:
    emb = get_embeddings()
    return emb.embed_documents(texts)
