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


@dataclass
class SourceMetric:
    source: str
    articles_count: int = 0
    chunks_count: int = 0
    avg_latency_ms: float = 0.0
    window_end: float = 0.0
