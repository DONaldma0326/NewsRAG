import time
from pathlib import Path

import feedparser
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from api.chat import add_message, create_chat, get_chat, list_chats
from api.sources import get_sources, record_run, toggle_source, update_interval

router = APIRouter()

UI_DIR = Path(__file__).parents[1] / "UI"


# ── Page routes ──────────────────────────────────────────


@router.get("/chat")
def chat_page(request: Request):
    return _render(request, "chat.html", {"chats": list_chats()})


@router.get("/chat/{chat_id}")
def chat_detail(request: Request, chat_id: str):
    chat = get_chat(chat_id)
    if not chat:
        return _render(request, "chat.html", {"chats": list_chats()})
    return _render(request, "chat.html", {"chats": list_chats(), "active_chat": chat})


@router.get("/sources")
def sources_page(request: Request):
    return _render(request, "sources.html", {"sources": get_sources()})


# ── Chat API ──────────────────────────────────────────────


@router.post("/api/chats")
def api_create_chat():
    chat = create_chat()
    return JSONResponse(chat)


@router.post("/api/chats/{chat_id}/messages")
def api_send_message(chat_id: str, body: dict):
    content = body.get("content", "")
    chat = add_message(chat_id, "user", content)
    if not chat:
        return JSONResponse({"error": "Chat not found"}, status_code=404)

    response_text = _generate_reply(content)
    chat = add_message(chat_id, "assistant", response_text)
    return JSONResponse(chat)


# ── Sources API ───────────────────────────────────────────


class ToggleBody(BaseModel):
    enabled: bool


class IntervalBody(BaseModel):
    interval: int


@router.post("/api/sources/{name}/toggle")
def api_toggle_source(name: str, body: ToggleBody):
    toggle_source(name, body.enabled)
    return JSONResponse({"ok": True})


@router.post("/api/sources/{name}/interval")
def api_interval_source(name: str, body: IntervalBody):
    update_interval(name, body.interval)
    return JSONResponse({"ok": True})


@router.post("/api/sources/{name}/test")
def api_test_source(name: str):
    sources = get_sources()
    src = next((s for s in sources if s["name"] == name), None)
    if not src:
        return JSONResponse({"error": "Not found"}, status_code=404)

    start = time.time()
    try:
        feed = feedparser.parse(src["url"])
        success = bool(feed.entries) and not feed.bozo
        count = len(feed.entries) if success else 0
        error = None if success else str(feed.bozo_exception)
        record_run(name, success, count, error)
    except Exception as e:
        record_run(name, False, 0, str(e))
        success = False
        count = 0

    elapsed = round(time.time() - start, 1)
    return JSONResponse(
        {"success": success, "article_count": count, "elapsed": elapsed}
    )


# ── Helpers ───────────────────────────────────────────────


def _render(request: Request, template: str, context: dict):
    templates = request.app.state.templates
    return templates.TemplateResponse(request, template, context)


def _generate_reply(user_msg: str) -> str:
    return (
        f"This is a simulated response. Your RAG pipeline will "
        f"replace this with real answers grounded in news articles.\n\n"
        f"> *You asked: {user_msg}*"
    )
