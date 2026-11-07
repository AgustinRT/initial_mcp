"""
Tests of the MCP server.
- Tools called directly.
- MCP sessions (in-memory client): lifespan + Kafka consumer activation through DI, resources.
- Integration (marker "kafka"): a Kafka producer updates the resources served by the running server.
"""
import json
import socket
import uuid
from contextlib import AsyncExitStack

import pytest
import pytest_asyncio
from fast_depends import dependency_provider
from fastmcp.client import Client

from mcps1.application.resources.adapters.management_adaptator import get_management_port_in
from mcps1.infraestructure.mcp.server import collatz, mcp, prime, runtime
from mcps1.infraestructure.messaging.adapters.messaging_adaptator import get_kafka_consumer_update, get_kafka_port_out
from mcps1.infraestructure.messaging.dtos.kafka_envelope_dto import KafkaEnvelopeDTO
from mcps1.infraestructure.messaging.kafka_workers.kafka_consumer_update import KafkaConsumerUpdate
from test.kafka_container_tester import consumer_tester, producer_tester
from test.utils_resources import (FakeKafkaPortIn, FakeKafkaPortOut, build_update_envelope, unique_name,
                                  user_example_content, wait_until)


def create_resource(name: str) -> dict:
    """Stores a resource as the consumer would do and returns its content."""
    content = user_example_content(name)
    envelope = KafkaEnvelopeDTO.model_validate(build_update_envelope(content))
    get_management_port_in().message_procesate("update", envelope.to_message_resource())
    return content


async def read_data_resource(client: Client, name: str) -> str:
    contents = await client.read_resource(f"data://datas/{name}")
    return contents[0].text


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(autouse=True)
def reset_fakes():
    FakeKafkaPortIn.instances.clear()
    yield
    assert runtime.active_sessions == 0


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_prime_success():
    assert await prime(2) == 1
    assert await prime(3) == 1
    assert await prime(17) == 1
    assert await prime(97) == 1


@pytest.mark.asyncio
async def test_prime_failure():
    assert await prime(4) == 2
    assert await prime(9) == 3
    assert await prime(100) == 2
    assert await prime(105) == 3


@pytest.mark.asyncio
async def test_collatz_standard_sequence():
    assert "6, 3, 10, 5, 16, 8, 4" == await collatz(6)


@pytest.mark.asyncio
async def test_collatz_edge_cases():
    assert "1, 4" == await collatz(1)
    assert "4" == await collatz(4)


# ---------------------------------------------------------------------------
# MCP sessions: lifespan and Kafka consumer activation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_session_exposes_tools_and_data_resource():
    with dependency_provider.scope(get_kafka_consumer_update, FakeKafkaPortIn):
        async with Client(mcp) as client:
            tools = {tool.name for tool in await client.list_tools()}
            templates = {template.uriTemplate for template in await client.list_resource_templates()}
            hello = await client.call_tool("hello", {"origin": "pytest"})
            prime_result = await client.call_tool("prime", {"number": 91})

    assert {"hello", "prime", "collatz"} <= tools
    assert "data://{type_data}/{value}" in templates
    assert "pytest" in hello.content[0].text
    assert prime_result.content[0].text == "7"


@pytest.mark.asyncio
async def test_lifespan_activates_update_consumer_through_di():
    with dependency_provider.scope(get_kafka_consumer_update, FakeKafkaPortIn):
        async with Client(mcp):
            [consumer] = FakeKafkaPortIn.instances
            assert runtime.update_consumer is consumer
            assert consumer.is_running

    assert consumer.stopped
    assert runtime.update_consumer is None


@pytest.mark.asyncio
async def test_lifespan_uses_real_kafka_consumer_by_default():
    # no broker configured in the unit tests: the real consumer is activated but stays idle
    async with Client(mcp):
        assert isinstance(runtime.update_consumer, KafkaConsumerUpdate)
        assert runtime.update_consumer.topics == ["update"]

    assert runtime.update_consumer is None


@pytest.mark.asyncio
async def test_single_consumer_shared_by_concurrent_sessions():
    with dependency_provider.scope(get_kafka_consumer_update, FakeKafkaPortIn):
        async with AsyncExitStack() as sessions:
            first = await sessions.enter_async_context(Client(mcp))
            await sessions.enter_async_context(Client(mcp))
            assert runtime.active_sessions == 2
            [consumer] = FakeKafkaPortIn.instances

            await first.close()
            assert runtime.active_sessions == 1
            assert consumer.is_running

        assert consumer.stopped
        assert len(FakeKafkaPortIn.instances) == 1


@pytest.mark.asyncio
async def test_consumer_activation_failure_does_not_break_server():
    def broken_consumer():
        raise RuntimeError("consumer misconfigured")

    with dependency_provider.scope(get_kafka_consumer_update, broken_consumer):
        async with Client(mcp) as client:
            result = await client.call_tool("prime", {"number": 97})

    assert result.content[0].text == "1"


@pytest.mark.asyncio
async def test_data_resource_is_served_and_reported():
    name = unique_name()
    content = create_resource(name)
    fake_producer = FakeKafkaPortOut()

    with dependency_provider.scope(get_kafka_consumer_update, FakeKafkaPortIn), \
            dependency_provider.scope(get_kafka_port_out, lambda: fake_producer):
        async with Client(mcp) as client:
            served = json.loads(await read_data_resource(client, name))
            missing = await read_data_resource(client, "does_not_exist")

    assert served["description"] == {"es_ES": content["description"][0]}
    assert served["fields"] == content["fields"]
    assert missing == "unkwon resource"
    destination, report = fake_producer.sent[0]
    assert destination == {"resource_type": "datas", "resource_value": name}
    assert report["dictionary_content"] == served
    assert fake_producer.closed  # the lifespan releases the producer


@pytest.mark.asyncio
async def test_server_works_while_kafka_broker_is_down(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", f"127.0.0.1:{free_port()}")
    name = unique_name()
    content = create_resource(name)

    async with Client(mcp) as client:
        assert runtime.update_consumer.is_running
        assert not runtime.update_consumer.is_connected
        served = json.loads(await read_data_resource(client, name))
        result = await client.call_tool("collatz", {"number": 6})

    assert served["competence"] == content["competence"]
    assert result.content[0].text == "6, 3, 10, 5, 16, 8, 4"


# ---------------------------------------------------------------------------
# Integration: Kafka producer -> topic "update" -> running MCP server
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def producer(kafka_bootstrap):
    kafka_producer = producer_tester(kafka_bootstrap)
    await kafka_producer.start()
    yield kafka_producer
    await kafka_producer.stop()


@pytest.fixture
def consumer_group(monkeypatch) -> str:
    group = f"mcps-test-{uuid.uuid4().hex[:8]}"
    monkeypatch.setenv("CONSUMER_GROUP_ID", group)
    return group


async def wait_for_resource(client: Client, name: str, timeout: float = 60) -> dict:
    async def available() -> bool:
        return await read_data_resource(client, name) != "unkwon resource"

    assert await wait_until(available, timeout=timeout, interval=0.5), f"resource {name} not updated from Kafka"
    return json.loads(await read_data_resource(client, name))


@pytest.mark.kafka
@pytest.mark.asyncio
async def test_active_server_processes_update_messages(kafka_bootstrap, consumer_group, producer):
    name = unique_name()
    content = user_example_content(name)

    async with Client(mcp) as client:
        await producer.send_and_wait("update", build_update_envelope(content))
        served = await wait_for_resource(client, name)
        assert runtime.update_consumer.is_connected

        updated_content = user_example_content(name)
        updated_content["competence"] = "messaging_v2"
        await producer.send_and_wait("update", build_update_envelope(updated_content))

        async def updated() -> bool:
            return json.loads(await read_data_resource(client, name))["competence"] == "messaging_v2"
        assert await wait_until(updated, timeout=60, interval=0.5)

    assert served["description"] == {"es_ES": content["description"][0]}
    assert served["type_data"] == "json"
    assert served["fields"] == content["fields"]


@pytest.mark.kafka
@pytest.mark.asyncio
async def test_messages_sent_while_server_is_down_are_processed_on_startup(kafka_bootstrap, consumer_group, producer):
    name = unique_name()
    await producer.send_and_wait("update", build_update_envelope(user_example_content(name)))

    async with Client(mcp) as client:
        served = await wait_for_resource(client, name)

    assert served["competence"] == "messaging"


@pytest.mark.kafka
@pytest.mark.asyncio
async def test_resource_reads_are_reported_to_kafka(kafka_bootstrap, consumer_group, producer):
    name = unique_name()
    create_resource(name)

    async with Client(mcp) as client:
        await read_data_resource(client, name)

    reports = consumer_tester(kafka_bootstrap, "report", group_id=f"reports-{uuid.uuid4().hex[:8]}")
    await reports.start()
    try:
        found = False
        for _ in range(30):
            batches = await reports.getmany(timeout_ms=1000)
            values = [record.value for records in batches.values() for record in records]
            if any(value["destination"]["resource_value"] == name for value in values):
                found = True
                break
    finally:
        await reports.stop()

    assert found, "report message not published"
