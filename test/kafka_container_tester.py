import contextlib
import json
import os
from typing import Iterator

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# Set this variable to run the Kafka tests against an existing broker instead of Testcontainers
EXTERNAL_BOOTSTRAP_ENV = "KAFKA_TEST_BOOTSTRAP_SERVERS"


@contextlib.contextmanager
def rise_kafka_container() -> Iterator[str]:
    """Yields the bootstrap servers of a Kafka broker: the external one or a Testcontainers broker."""
    external = os.getenv(EXTERNAL_BOOTSTRAP_ENV)
    if external:
        yield external
        return

    from testcontainers.community.kafka import KafkaContainer
    with KafkaContainer().with_kraft() as kafka:
        yield kafka.get_bootstrap_server()


def consumer_tester(bootstrap: str, topic: str, group_id: str | None = None) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    )


def producer_tester(bootstrap: str) -> AIOKafkaProducer:
    return AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
