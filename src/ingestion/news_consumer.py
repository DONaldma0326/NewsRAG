import json
import logging
import signal

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
KAFKA_TOPIC = "news-raw"


def get_partitions(consumer: Consumer, topic: str) -> list[TopicPartition]:
    metadata = consumer.list_topics(topic, timeout=10)
    topic_meta = metadata.topics[topic]
    return [TopicPartition(topic, p) for p in topic_meta.partitions]


def main():
    running = True

    def shutdown(_sig, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    consumer = Consumer(
        {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "group.id": "news-consumer"}
    )

    partitions = get_partitions(consumer, KAFKA_TOPIC)
    for p in partitions:
        p.offset = OFFSET_BEGINNING
    consumer.assign(partitions)
    log.info(
        "Listening to topic '%s' (partitions: %s) ...",
        KAFKA_TOPIC,
        [p.partition for p in partitions],
    )

    while running:
        msgs = consumer.consume(num_messages=10, timeout=2.0)
        for msg in msgs:
            if msg.error():
                log.error("Consumer error: %s", msg.error())
                continue
            article = json.loads(msg.value().decode())
            print(f"[{article.get('source', '?')}] {article.get('title', '?')}")
            print(f"    Published: {article.get('published', '?')}")
            print(f"    Link: {article.get('link', '?')}")
            print(f"    Snippet: {article.get('summary', '')[:200]}")
            print("-" * 60)

    consumer.close()


if __name__ == "__main__":
    main()
