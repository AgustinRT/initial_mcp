"""
Tests of the Kafka update consumer.
- Unit tests: message processing, resilience and lifecycle (no broker needed).
- Integration tests (marker "kafka"): real producer -> topic "update" -> consumer -> resource file.
"""
import asyncio
import json
import socket
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from fast_depends import inject

from mcps1.infraestructure.messaging.adapters.messaging_adaptator import KafkaConsumerUpdateDependency
from mcps1.infraestructure.messaging.kafka_workers.kafka_consumer_update import KafkaConsumerUpdate
from mcps1.infraestructure.messaging.ports.kafka_port_in import KafkaPortIn
from test.kafka_container_tester import producer_tester
from test.utils_resources import build_update_envelope, unique_name, user_example_content, wait_until

RESOURCES_TEST = Path("test/resources")


@inject
def get_update_consumer(update_consumer: KafkaConsumerUpdateDependency) -> KafkaPortIn:
    return update_consumer


def resource_file(name: str, family: str = "datas", data_use: str = "asinc_messages", extension: str = "json") -> Path:
    return RESOURCES_TEST / family / data_use / f"{name}.{extension}"


def encode(message: dict) -> bytes:
    return json.dumps(message).encode("utf-8")


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


# ---------------------------------------------------------------------------
# Unit tests (no broker)
# ---------------------------------------------------------------------------

def test_di_builds_consumer_subscribed_only_to_update():
    consumer = get_update_consumer()

    assert isinstance(consumer, KafkaConsumerUpdate)
    assert isinstance(consumer, KafkaPortIn)
    assert consumer.topics == ["update"]
    assert not consumer.is_running


def test_process_record_stores_user_example_as_resource():
    consumer = get_update_consumer()
    name = unique_name()
    content = user_example_content(name)

    result = consumer.process_record("update", encode(build_update_envelope(content)))

    stored_file = resource_file(name)
    assert result == str(stored_file)
    stored = json.loads(stored_file.read_text(encoding="utf-8"))
    assert stored["description"] == {"es_ES": content["description"][0]}
    assert stored["competence"] == "messaging"
    assert stored["type_data"] == "json"
    assert stored["process_apply"] is None
    assert stored["fields"] == content["fields"]
    assert consumer.processed_messages == 1


def test_process_record_update_replaces_existing_resource():
    consumer = get_update_consumer()
    name = unique_name()
    first = user_example_content(name)
    second = user_example_content(name)
    second["competence"] = "messaging_v2"
    second["fields"] = second["fields"][:1]

    consumer.process_record("update", encode(build_update_envelope(first)))
    consumer.process_record("update", encode(build_update_envelope(second)))

    stored = json.loads(resource_file(name).read_text(encoding="utf-8"))
    assert stored["competence"] == "messaging_v2"
    assert len(stored["fields"]) == 1
    assert len(list(RESOURCES_TEST.joinpath("datas").rglob(f"{name}.*"))) == 1


def test_process_record_uses_family_and_type_of_the_envelope():
    consumer = get_update_consumer()
    name = unique_name()

    consumer.process_record("update", encode(build_update_envelope(user_example_content(name), type_resource="database")))

    assert resource_file(name, data_use="database", extension="sql").exists()


@pytest.mark.parametrize("raw", [
    b"{not json",
    None,
    encode({"any": "thing"}),
    encode(build_update_envelope(user_example_content("../escape"))),
    encode(user_example_content("plain_object_without_envelope")),
])
def test_process_record_never_raises_on_invalid_messages(raw):
    consumer = get_update_consumer()

    assert consumer.process_record("update", raw) is None
    assert consumer.failed_messages == 1
    assert not Path("test/escape.json").exists()


@pytest.mark.asyncio
async def test_consumer_is_disabled_without_bootstrap_servers():
    consumer = get_update_consumer()

    consumer.start()
    await asyncio.wait_for(consumer.consume_updates(), timeout=5)
    await consumer.stop()

    assert not consumer.is_running


@pytest.mark.asyncio
async def test_consumer_keeps_retrying_while_broker_is_down(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", f"127.0.0.1:{free_port()}")
    consumer = get_update_consumer()

    consumer.start()
    await asyncio.sleep(1.5)  # several reconnection attempts (test backoff: 0.5s)

    assert consumer.is_running
    assert not consumer.is_connected
    await asyncio.wait_for(consumer.stop(), timeout=5)
    assert not consumer.is_running


# ---------------------------------------------------------------------------
# Integration tests (real Kafka broker)
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def running_consumer(kafka_bootstrap, monkeypatch):
    # dedicated consumer group per test
    monkeypatch.setenv("CONSUMER_GROUP_ID", f"mcps-test-{uuid.uuid4().hex[:8]}")
    consumer = get_update_consumer()
    consumer.start()
    yield consumer
    await consumer.stop()


@pytest_asyncio.fixture
async def producer(kafka_bootstrap):
    kafka_producer = producer_tester(kafka_bootstrap)
    await kafka_producer.start()
    yield kafka_producer
    await kafka_producer.stop()


@pytest.mark.kafka
@pytest.mark.asyncio
async def test_consumer_receives_update_message_from_producer(running_consumer, producer):
    name = unique_name()
    content = user_example_content(name)

    await producer.send_and_wait("update", build_update_envelope(content))

    assert await wait_until(lambda: resource_file(name).exists(), timeout=60), "message not processed"
    assert running_consumer.is_connected
    stored = json.loads(resource_file(name).read_text(encoding="utf-8"))
    assert stored["description"] == {"es_ES": content["description"][0]}
    assert stored["fields"] == content["fields"]


@pytest.mark.kafka
@pytest.mark.asyncio
async def test_consumer_applies_successive_updates(running_consumer, producer):
    name = unique_name()
    first = user_example_content(name)
    second = user_example_content(name)
    second["competence"] = "messaging_v2"

    await producer.send_and_wait("update", build_update_envelope(first))
    await producer.send_and_wait("update", build_update_envelope(second))

    def updated() -> bool:
        path = resource_file(name)
        return path.exists() and json.loads(path.read_text(encoding="utf-8"))["competence"] == "messaging_v2"

    assert await wait_until(updated, timeout=60), "update not applied"


@pytest.mark.kafka
@pytest.mark.asyncio
async def test_consumer_survives_invalid_messages(running_consumer, producer):
    name = unique_name()

    await producer.send_and_wait("update", "not an envelope")
    await producer.send_and_wait("update", {"message_data": {"family_resource": "datas"}})
    await producer.send_and_wait("update", build_update_envelope(user_example_content(name)))

    assert await wait_until(lambda: resource_file(name).exists(), timeout=60), "valid message not processed"
    assert running_consumer.failed_messages >= 2
    assert running_consumer.is_running


@pytest.mark.kafka
@pytest.mark.asyncio
async def test_consumer_only_listens_update_topic(running_consumer, producer):
    ignored = unique_name("ignored")
    processed = unique_name()

    await producer.send_and_wait("create", build_update_envelope(user_example_content(ignored)))
    await producer.send_and_wait("update", build_update_envelope(user_example_content(processed)))

    assert await wait_until(lambda: resource_file(processed).exists(), timeout=60)
    assert not resource_file(ignored).exists()
