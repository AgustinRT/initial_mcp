import contextlib
import os
import shutil

import pytest

from test.kafka_container_tester import EXTERNAL_BOOTSTRAP_ENV, rise_kafka_container

# The DI providers read this variable every time they are resolved: use the test configuration
# (test/.env.test -> RESOURCES_LOCATION="test/resources", test/.env.messaging.test)
os.environ["ENVIROMENT_EXECUTION"] = "test"
# Unit tests run without broker; the kafka_bootstrap fixture provides one for the integration tests
os.environ.pop("KAFKA_BOOTSTRAP_SERVERS", None)
os.environ.pop("bootstrap_servers", None)

RESOURCES_TEST = "test/resources"


@pytest.fixture(scope="session", autouse=True)
def clean_test_resources():
    yield
    shutil.rmtree(RESOURCES_TEST, ignore_errors=True)


@pytest.fixture(scope="session")
def kafka_broker():
    """
    Kafka broker shared by the integration tests (marker "kafka"):
    - KAFKA_TEST_BOOTSTRAP_SERVERS when defined (e.g. the docker-compose broker),
    - otherwise a Testcontainers broker (requires Docker).
    """
    with contextlib.ExitStack() as stack:
        try:
            bootstrap = stack.enter_context(rise_kafka_container())
        except Exception as error:
            pytest.skip(f"Kafka broker not available (Docker for Testcontainers or {EXTERNAL_BOOTSTRAP_ENV}): {error}")
        yield bootstrap


@pytest.fixture
def kafka_bootstrap(kafka_broker, monkeypatch):
    """Points the server components of the current test to the Kafka broker."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", kafka_broker)
    return kafka_broker
