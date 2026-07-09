import hashlib
import json
import logging
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

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "news-raw"
POLL_INTERVAL_SECONDS = 60
CONFIG_PATH = Path(__file__).parents[2] / "config" / "sources.json"


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


def pull_and_produce(producer: Producer, sources: list[dict], seen: set) -> int:
    count = 0
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

            msg = normalize_entry(name, entry)
            producer.produce(
                KAFKA_TOPIC,
                key=article_id.encode(),
                value=json.dumps(msg).encode(),
                callback=delivery_report,
            )
            count += 1

    producer.flush()
    return count


def main():
    sources = load_sources(CONFIG_PATH)
    log.info("Loaded %d news sources", len(sources))

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    seen: set = set()

    log.info("Starting news puller (poll interval: %ds)", POLL_INTERVAL_SECONDS)
    while True:
        count = pull_and_produce(producer, sources, seen)
        log.info("Produced %d new articles", count)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
