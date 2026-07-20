import logging

from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import ChatOllama

from common.config import OLLAMA_BASE_URL, OLLAMA_MODEL
from common.exceptions import LLMError
from common.models import SearchResult

log = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "You are a helpful news assistant. Answer the user's question based on the "
    "provided context. If the context doesn't contain enough information, say "
    "so clearly. Keep answers concise and cite the source name when possible."
)


def build_prompt(
    query: str,
    context: list[SearchResult],
    history: list[dict],
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
            role = "User" if msg["role"] == "user" else "Assistant"
            parts.append(f"{role}: {msg['content']}")
        parts.append("")
    parts.append(f"Question: {query}")
    parts.append("Answer:")
    return "\n".join(parts)


def generate(
    query: str,
    context: list[SearchResult],
    history: list[dict],
    model: str = OLLAMA_MODEL,
) -> tuple[str, int]:
    context_str = build_prompt(query, context, history)
    try:
        llm = ChatOllama(
            base_url=OLLAMA_BASE_URL,
            model=model,
            temperature=0,
            num_predict=1024,
        )
        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", SYSTEM_PROMPT),
                ("human", context_str),
            ]
        )
        chain = prompt | llm
        response = chain.invoke({})
        return response.content, response.response_metadata.get("eval_count", 0)
    except Exception as e:
        raise LLMError(f"Ollama generation failed: {e}") from e
