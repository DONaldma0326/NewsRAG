import json
from datetime import datetime, timezone
from pathlib import Path

CONFIG_PATH = Path(__file__).parents[2] / "config" / "sources.json"
DATA_DIR = Path(__file__).parents[2] / "data"
STATUS_FILE = DATA_DIR / "source_status.json"

DATA_DIR.mkdir(exist_ok=True)
if not STATUS_FILE.exists():
    STATUS_FILE.write_text("{}")


def _status() -> dict:
    return json.loads(STATUS_FILE.read_text())


def _save_status(data: dict):
    STATUS_FILE.write_text(json.dumps(data, indent=2))


def get_sources() -> list[dict]:
    with open(CONFIG_PATH) as f:
        sources = json.load(f)
    status_data = _status()
    for src in sources:
        name = src["name"]
        st = status_data.get(name, {})
        src["enabled"] = st.get("enabled", True)
        src["interval"] = st.get("interval", 60)
        src["last_run"] = st.get("last_run")
        src["last_success"] = st.get("last_success")
        src["article_count"] = st.get("article_count", 0)
        src["error"] = st.get("error")
    return sources


def toggle_source(name: str, enabled: bool) -> dict:
    data = _status()
    entry = data.setdefault(name, {})
    entry["enabled"] = enabled
    _save_status(data)
    return entry


def update_interval(name: str, interval: int) -> dict:
    data = _status()
    entry = data.setdefault(name, {})
    entry["interval"] = interval
    _save_status(data)
    return entry


def record_run(
    name: str, success: bool, article_count: int = 0, error: str | None = None
):
    data = _status()
    entry = data.setdefault(name, {})
    now = datetime.now(timezone.utc).isoformat()
    entry["last_run"] = now
    if success:
        entry["last_success"] = now
        entry["article_count"] = article_count
        entry.pop("error", None)
    else:
        entry["error"] = error
    _save_status(data)
