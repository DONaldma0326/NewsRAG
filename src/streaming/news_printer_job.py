import logging
import os

from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.formats.json import JsonRowDeserializationSchema

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "news-raw")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "flink-news-printer")


def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    deserialization_schema = (
        JsonRowDeserializationSchema.builder()
        .type_info(
            Types.ROW_NAMED(
                ["source", "title", "link", "published", "summary", "id"],
                [
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                ],
            )
        )
        .build()
    )

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(deserialization_schema)
        .build()
    )

    ds = env.from_source(
        source, WatermarkStrategy.for_monotonous_timestamps(), "kafka-news-source"
    )

    ds.map(lambda row: f"[{row[0]}] {row[1]} — {row[3]}\n    {row[4][:200]}").print()

    env.execute("News Printer Job")


if __name__ == "__main__":
    main()
