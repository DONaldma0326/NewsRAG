import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path

from common.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_METRICS_TOPIC

log = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parents[2] / "config" / "sources.json"
DATA_DIR = Path(__file__).parents[2] / "data"
STATUS_FILE = DATA_DIR / "source_status.json"

DATA_DIR.mkdir(exist_ok=True)
if not STATUS_FILE.exists():
    STATUS_FILE.write_text("{}")

_lock = threading.Lock()


def _status() -> dict:
    with _lock:
        return json.loads(STATUS_FILE.read_text())


def _save_status(data: dict):
    with _lock:
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
        src["chunks_count"] = st.get("chunks_count", 0)
        src["avg_latency_ms"] = st.get("avg_latency_ms", 0)
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


def _apply_metric(metric: dict):
    source = metric.get("source", "")
    if not source:
        return
    data = _status()
    entry = data.setdefault(source, {})
    entry["articles_count"] = metric.get(
        "articles_count", entry.get("articles_count", 0)
    )
    entry["chunks_count"] = metric.get("chunks_count", entry.get("chunks_count", 0))
    entry["avg_latency_ms"] = metric.get(
        "avg_latency_ms", entry.get("avg_latency_ms", 0)
    )
    entry["last_run"] = datetime.now(timezone.utc).isoformat()
    entry["last_success"] = entry["last_run"]
    _save_status(data)


def start_metrics_consumer():
    def _run():
        try:
            from confluent_kafka import Consumer, KafkaError

            consumer = Consumer(
                {
                    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                    "group.id": "api-metrics-consumer",
                    "auto.offset.reset": "latest",
                    "enable.auto.commit": True,
                }
            )
            consumer.subscribe([KAFKA_METRICS_TOPIC])
            log.info("Metrics consumer subscribed to %s", KAFKA_METRICS_TOPIC)
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    log.warning("Metrics consumer error: %s", msg.error())
                    continue
                try:
                    metric = json.loads(msg.value().decode())
                    _apply_metric(metric)
                except Exception as e:
                    log.warning("Failed to parse metric: %s", e)
        except Exception as e:
            log.warning("Metrics consumer not available (Kafka down?): %s", e)

    thread = threading.Thread(target=_run, daemon=True, name="metrics-consumer")
    thread.start()
    return thread
