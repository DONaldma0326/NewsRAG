import json
import logging

from confluent_kafka import Consumer

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "news-raw"
KAFKA_GROUP_ID = "news-consumer"


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    log.info("Listening to topic '%s' ...", KAFKA_TOPIC)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("Consumer error: %s", msg.error())
                continue
            article = json.loads(msg.value().decode())
            print(f"[{article['source']}] {article['title']}")
            print(f"    Published: {article['published']}")
            print(f"    Link: {article['link']}")
            print(f"    Snippet: {article['summary'][:200]}")
            print("-" * 60)
    except KeyboardInterrupt:
        log.info("Shutting down ...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
