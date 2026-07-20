import logging
import time

from langchain_core.messages import AIMessage, HumanMessage

from common.config import MAX_HISTORY_TURNS, OLLAMA_MODEL, RETRIEVAL_K
from common.models import RAGResult, RAGTrace, SearchResult
from RAG.graph import compile_graph

log = logging.getLogger(__name__)


def answer(query: str, history: list[dict] | None = None) -> RAGResult:
    t0 = time.perf_counter()

    lc_messages = _convert_history(history) if history else []

    graph = compile_graph()
    inputs = {
        "messages": lc_messages,
        "query": query,
    }
    result = graph.invoke(inputs)

    t3 = time.perf_counter()

    rewritten_query = result.get("rewritten_query", query)
    documents = result.get("documents", [])
    answer_text = result.get("answer", "")

    sources = [
        SearchResult(
            text=d.page_content,
            source=d.metadata.get("source", ""),
            title=d.metadata.get("title", ""),
            url=d.metadata.get("url", ""),
            published=d.metadata.get("published", ""),
            score=d.metadata.get("score", 0.0),
        )
        for d in documents
    ]

    tokens = len(answer_text.split())

    return RAGResult(
        answer=answer_text,
        sources=sources,
        trace=RAGTrace(
            time_embed_ms=0,
            time_retrieve_ms=0,
            time_generate_ms=(t3 - t0) * 1000,
            total_ms=(t3 - t0) * 1000,
            prompt=rewritten_query,
            model=OLLAMA_MODEL,
            tokens=tokens,
            k=RETRIEVAL_K,
        ),
    )


def _convert_history(raw: list[dict]) -> list:
    messages = [m for m in raw if m["role"] in ("user", "assistant")]
    messages = messages[-MAX_HISTORY_TURNS * 2 :]
    out = []
    for m in messages:
        if m["role"] == "user":
            out.append(HumanMessage(content=m["content"]))
        else:
            out.append(AIMessage(content=m["content"]))
    return out
