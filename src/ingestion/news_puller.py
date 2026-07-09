import hashlib
import json
import logging
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import feedparser
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
KAFKA_TOPIC = "news-raw"
POLL_INTERVAL_SECONDS = 60
CONFIG_PATH = Path(__file__).parents[2] / "config" / "sources.json"
STATE_DB_PATH = Path(__file__).parents[2] / "data" / "ingestion_state.db"


def load_sources(path: Path) -> list[dict]:
    with open(path) as f:
        return json.load(f)


def normalize_entry(source_name: str, entry: dict) -> dict:
    return {
        "source": source_name,
        "title": entry.get("title", ""),
        "link": entry.get("link", ""),
        "published": entry.get("published", datetime.now(timezone.utc).isoformat()),
        "summary": entry.get("summary", ""),
        "id": hashlib.sha256(entry.get("link", "").encode()).hexdigest(),
    }


def delivery_report(err, msg):
    if err:
        log.error("Delivery failed: %s", err)
    else:
        log.debug("Delivered to %s [%d]", msg.topic(), msg.partition())


def init_state_store(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE IF NOT EXISTS seen (id TEXT PRIMARY KEY)")
    conn.commit()
    return conn


def load_seen(conn: sqlite3.Connection) -> set:
    rows = conn.execute("SELECT id FROM seen").fetchall()
    return {row[0] for row in rows}


def save_new_ids(conn: sqlite3.Connection, article_ids: set):
    conn.executemany(
        "INSERT OR IGNORE INTO seen (id) VALUES (?)",
        [(aid,) for aid in article_ids],
    )
    conn.commit()


def pull_and_produce(
    producer: Producer, sources: list[dict], seen: set, conn: sqlite3.Connection
) -> int:
    count = 0
    new_ids: set = set()
    for source in sources:
        name = source["name"]
        url = source["url"]
        log.info("Fetching: %s (%s)", name, url)
        try:
            feed = feedparser.parse(url)
        except Exception as e:
            log.warning("Failed to fetch %s: %s", name, e)
            continue

        if feed.bozo and not feed.entries:
            log.warning("Malformed feed for %s", name)
            continue

        for entry in feed.entries:
            article_id = hashlib.sha256(entry.get("link", "").encode()).hexdigest()
            if article_id in seen:
                continue
            seen.add(article_id)
            new_ids.add(article_id)

            msg = normalize_entry(name, entry)
            producer.produce(
                KAFKA_TOPIC,
                key=article_id.encode(),
                value=json.dumps(msg).encode(),
                callback=delivery_report,
            )
            count += 1

    producer.flush()
    if new_ids:
        save_new_ids(conn, new_ids)
    return count


def main():
    sources = load_sources(CONFIG_PATH)
    log.info("Loaded %d news sources", len(sources))

    conn = init_state_store(STATE_DB_PATH)
    seen = load_seen(conn)
    log.info("Loaded %d previously seen articles from state store", len(seen))

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    log.info("Starting news puller (poll interval: %ds)", POLL_INTERVAL_SECONDS)
    while True:
        count = pull_and_produce(producer, sources, seen, conn)
        log.info("Produced %d new articles", count)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
