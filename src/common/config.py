import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[2]

CHROMA_HOST = os.getenv("CHROMA_HOST", "localhost")
CHROMA_PORT = int(os.getenv("CHROMA_PORT", "8000"))
CHROMA_COLLECTION = os.getenv("CHROMA_COLLECTION", "news")

EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text")
EMBEDDING_DIM = 768

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "512"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "64"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_CHUNKS_TOPIC = os.getenv("KAFKA_CHUNKS_TOPIC", "news-chunks")
KAFKA_METRICS_TOPIC = os.getenv("KAFKA_METRICS_TOPIC", "news-metrics")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "rag-embed-consumer")

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1")

RETRIEVAL_K = int(os.getenv("RETRIEVAL_K", "5"))
MAX_HISTORY_TURNS = int(os.getenv("MAX_HISTORY_TURNS", "6"))
