import calendar
import email.utils
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
SEEN_TTL_HOURS = 24


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


# ─── State store (per-source HTTP cache + bounded overlap dedup) ─────────


def init_state_store(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS source_state (
            name TEXT PRIMARY KEY,
            etag TEXT,
            modified_ts INTEGER,
            last_fetch TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS seen_articles (
            id TEXT PRIMARY KEY,
            seen_at TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


def load_source_headers(conn: sqlite3.Connection) -> dict[str, dict]:
    rows = conn.execute("SELECT name, etag, modified_ts FROM source_state").fetchall()
    return {row[0]: {"etag": row[1], "modified_ts": row[2]} for row in rows}


def save_source_headers(
    conn: sqlite3.Connection, name: str, etag: str | None, modified_ts: int | None
):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO source_state (name, etag, modified_ts, last_fetch)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            etag = excluded.etag,
            modified_ts = excluded.modified_ts,
            last_fetch = excluded.last_fetch
        """,
        (name, etag, modified_ts, now),
    )
    conn.commit()


def load_seen_ids(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT id FROM seen_articles").fetchall()
    return {row[0] for row in rows}


def save_seen_ids(conn: sqlite3.Connection, ids: set[str]):
    now = datetime.now(timezone.utc).isoformat()
    conn.executemany(
        "INSERT OR IGNORE INTO seen_articles (id, seen_at) VALUES (?, ?)",
        [(aid, now) for aid in ids],
    )
    conn.commit()


def prune_seen_ids(conn: sqlite3.Connection):
    cutoff = datetime.now(timezone.utc).timestamp() - SEEN_TTL_HOURS * 3600
    cutoff_str = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()
    conn.execute("DELETE FROM seen_articles WHERE seen_at < ?", (cutoff_str,))
    conn.commit()


# ─── Core logic ────────────────────────────────────────────────────────


def process_source(
    producer: Producer,
    source: dict,
    seen: set[str],
    source_headers: dict[str, dict],
    conn: sqlite3.Connection,
) -> int:
    name = source["name"]
    url = source["url"]
    headers = source_headers.get(name, {})
    etag = headers.get("etag")
    modified_ts = headers.get("modified_ts")
    modified = time.gmtime(modified_ts) if modified_ts else None

    try:
        feed = feedparser.parse(url, etag=etag, modified=modified)
    except Exception as e:
        log.warning("Failed to fetch %s: %s", name, e)
        return 0

    if feed.get("status") == 304:
        log.info("Feed %s unchanged (304)", name)
        return 0

    if feed.bozo and not feed.entries:
        log.warning("Malformed feed for %s", name)
        return 0

    count = 0
    new_ids: set = set()
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
        save_seen_ids(conn, new_ids)

    new_etag = feed.get("etag")
    modified_value = feed.get("modified")
    if modified_value is not None:
        if isinstance(modified_value, str):
            dt = email.utils.parsedate_to_datetime(modified_value)
            new_modified_ts = int(dt.timestamp())
        else:
            new_modified_ts = calendar.timegm(modified_value)
    else:
        new_modified_ts = None
    save_source_headers(conn, name, new_etag, new_modified_ts)
    source_headers[name] = {"etag": new_etag, "modified_ts": new_modified_ts}

    return count


def main():
    conn = init_state_store(STATE_DB_PATH)
    source_headers = load_source_headers(conn)
    seen = load_seen_ids(conn)
    log.info(
        "Loaded %d sources and %d seen articles from state store",
        len(source_headers),
        len(seen),
    )

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    log.info("Starting news puller (poll interval: %ds)", POLL_INTERVAL_SECONDS)
    while True:
        sources = load_sources(CONFIG_PATH)
        total = 0
        for source in sources:
            count = process_source(producer, source, seen, source_headers, conn)
            total += count
            if count:
                log.info("Produced %d new articles from %s", count, source["name"])

        prune_seen_ids(conn)
        log.info(
            "Cycle complete: %d new articles, %d tracked in dedup", total, len(seen)
        )
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
