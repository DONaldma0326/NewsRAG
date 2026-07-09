import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path(__file__).parents[2] / "data"
CHATS_FILE = DATA_DIR / "chats.json"

DATA_DIR.mkdir(exist_ok=True)
if not CHATS_FILE.exists():
    CHATS_FILE.write_text("{}")


def _load() -> dict:
    return json.loads(CHATS_FILE.read_text())


def _save(data: dict):
    CHATS_FILE.write_text(json.dumps(data, indent=2))


def create_chat() -> dict:
    data = _load()
    chat_id = str(uuid.uuid4())[:8]
    now = datetime.now(timezone.utc).isoformat()
    data[chat_id] = {
        "id": chat_id,
        "title": "New Chat",
        "created_at": now,
        "messages": [],
    }
    _save(data)
    return data[chat_id]


def list_chats() -> list[dict]:
    data = _load()
    chats = sorted(data.values(), key=lambda c: c["created_at"], reverse=True)
    for c in chats:
        last = c["messages"][-1]["content"][:60] if c["messages"] else ""
        c["preview"] = last
    return chats


def get_chat(chat_id: str) -> dict | None:
    return _load().get(chat_id)


def add_message(chat_id: str, role: str, content: str) -> dict | None:
    data = _load()
    chat = data.get(chat_id)
    if not chat:
        return None
    if role == "user" and chat["title"] == "New Chat":
        chat["title"] = content[:50]
    chat["messages"].append(
        {
            "role": role,
            "content": content,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    )
    _save(data)
    return chat
