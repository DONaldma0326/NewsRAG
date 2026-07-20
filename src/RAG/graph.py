import logging
from typing import Annotated, Sequence, TypedDict

from langchain_core.documents import Document
from langchain_core.messages import BaseMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import ChatOllama
from langgraph.graph import END, StateGraph, add_messages

from common.config import OLLAMA_BASE_URL, OLLAMA_MODEL, RETRIEVAL_K
from common.models import SearchResult
from RAG.embedder import get_embeddings
from RAG.generator import build_prompt
from RAG.reranker import rerank
from RAG.vector_store import query as vector_query

log = logging.getLogger(__name__)

REWRITE_SYSTEM_PROMPT = (
    "Given the conversation history and the user's latest question, "
    "rewrite the question to be standalone and self-contained. "
    "Output only the rewritten question, nothing else."
)


class GraphState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]
    query: str
    rewritten_query: str
    documents: list[Document]
    answer: str


def rewrite_query(state: GraphState) -> dict:
    chat_history = state.get("messages", [])
    query = state.get("query", "")

    if len(chat_history) <= 1:
        return {"rewritten_query": query}

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", REWRITE_SYSTEM_PROMPT),
            *[(m.type, m.content) for m in chat_history[:-1]],
            ("human", "{question}"),
        ]
    )
    llm = ChatOllama(base_url=OLLAMA_BASE_URL, model=OLLAMA_MODEL, temperature=0)
    chain = prompt | llm
    rewritten = chain.invoke({"question": query}).content.strip()
    log.info("Rewritten query: %s", rewritten)
    return {"rewritten_query": rewritten}


def retrieve(state: GraphState) -> dict:
    query = state.get("rewritten_query") or state.get("query", "")
    embedding = get_embeddings().embed_query(query)
    results = vector_query(embedding, k=RETRIEVAL_K)
    docs = [
        Document(
            page_content=r.text,
            metadata={
                "source": r.source,
                "title": r.title,
                "url": r.url,
                "published": r.published,
                "score": r.score,
            },
        )
        for r in results
    ]
    return {"documents": docs}


def rerank_documents(state: GraphState) -> dict:
    query = state.get("rewritten_query") or state.get("query", "")
    docs = state.get("documents", [])

    results = [
        SearchResult(
            text=d.page_content,
            source=d.metadata.get("source", ""),
            title=d.metadata.get("title", ""),
            url=d.metadata.get("url", ""),
            published=d.metadata.get("published", ""),
            score=d.metadata.get("score", 0.0),
        )
        for d in docs
    ]

    reranked = rerank(query, results)
    reranked_docs = [
        Document(
            page_content=r.text,
            metadata={
                "source": r.source,
                "title": r.title,
                "url": r.url,
                "published": r.published,
                "score": r.score,
            },
        )
        for r in reranked
    ]
    return {"documents": reranked_docs}


def generate(state: GraphState) -> dict:
    query = state.get("rewritten_query") or state.get("query", "")
    docs = state.get("documents", [])
    chat_history = state.get("messages", [])

    results = [
        SearchResult(
            text=d.page_content,
            source=d.metadata.get("source", ""),
            title=d.metadata.get("title", ""),
            url=d.metadata.get("url", ""),
            published=d.metadata.get("published", ""),
            score=d.metadata.get("score", 0.0),
        )
        for d in docs
    ]

    prompt_text = build_prompt(query, results, [])
    llm = ChatOllama(
        base_url=OLLAMA_BASE_URL,
        model=OLLAMA_MODEL,
        temperature=0,
        num_predict=1024,
    )

    history_messages = (
        [{"role": m.type, "content": m.content} for m in chat_history]
        if chat_history
        else []
    )

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", prompt_text),
            *[(m["role"], m["content"]) for m in history_messages],
            ("human", query),
        ]
    )

    chain = prompt | llm
    response = chain.invoke({})
    return {"answer": response.content}


def compile_graph():
    workflow = StateGraph(GraphState)

    workflow.add_node("rewrite_query", rewrite_query)
    workflow.add_node("retrieve", retrieve)
    workflow.add_node("rerank", rerank_documents)
    workflow.add_node("generate", generate)

    workflow.set_entry_point("rewrite_query")
    workflow.add_edge("rewrite_query", "retrieve")
    workflow.add_edge("retrieve", "rerank")
    workflow.add_edge("rerank", "generate")
    workflow.add_edge("generate", END)

    return workflow.compile()
