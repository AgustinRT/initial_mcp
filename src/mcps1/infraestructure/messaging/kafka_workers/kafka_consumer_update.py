import asyncio
import contextlib
import logging
from typing import Optional

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from mcps1.application.resources.ports.resource_maintender_port_in import ResourceMaintenderPortIn
from mcps1.infraestructure.messaging.dtos.kafka_envelope_dto import KafkaEnvelopeDTO
from mcps1.infraestructure.messaging.dtos.message_resource_dto import MessageResourceDTO
from mcps1.infraestructure.messaging.kafka_workers.kafka_settings import KafkaSettings


name = "mcps1"
logger = logging.getLogger(name)

_STOP_TIMEOUT_SECONDS = 10


class KafkaConsumerUpdate:
    """
    Background consumer of the update topics (CONSUMER_TOPICS, "update" by default).

    - Runs as an asyncio task of the MCP server event loop (start / stop), so the server keeps
      answering requests while messages are consumed.
    - The broker is optional: while it is not reachable the consumer retries with backoff.
    - Every message is validated (KafkaEnvelopeDTO) and delegated to ResourceMaintenderPortIn.
      Invalid messages are logged and skipped, they never stop the consumer.
    """

    def __init__(self, manager_resources: ResourceMaintenderPortIn, type_run: str = "dev",
                 settings: Optional[KafkaSettings] = None):
        self._manager_resources = manager_resources
        self._settings = settings or KafkaSettings.load(type_run)
        self._task: Optional[asyncio.Task] = None
        self._connected = False
        self.processed_messages = 0
        self.failed_messages = 0

    @property
    def topics(self) -> list[str]:
        return list(self._settings.consumer_topics)

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def is_connected(self) -> bool:
        return self._connected

    def start(self) -> None:
        if self.is_running:
            return
        self._task = asyncio.get_running_loop().create_task(self.consume_updates(), name="kafka-consumer-update")

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        logger.info("Kafka update consumer stopped")

    async def consume_updates(self) -> None:
        """Consumes until cancelled. Reconnects with exponential backoff while the broker is unavailable."""
        if not self._settings.bootstrap_servers:
            logger.warning("KAFKA_BOOTSTRAP_SERVERS is not configured: the Kafka update consumer is disabled")
            return

        backoff = self._settings.retry_backoff_seconds
        while True:
            consumer = self._build_consumer()
            try:
                await consumer.start()
            except KafkaError as error:
                await self._close_consumer(consumer)
                logger.warning(f"Kafka broker {self._settings.bootstrap_servers} not available ({error}); "
                               f"retrying in {backoff:.0f}s. The MCP server keeps working without updates.")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._settings.retry_backoff_max_seconds)
                continue

            backoff = self._settings.retry_backoff_seconds
            self._connected = True
            logger.info(f"Kafka update consumer listening topics {self.topics} on "
                        f"{self._settings.bootstrap_servers} (group '{self._settings.group_id}')")
            try:
                while True:
                    batches = await consumer.getmany(timeout_ms=self._settings.poll_timeout_ms)
                    for records in batches.values():
                        for record in records:
                            self.process_record(record.topic, record.value)
            except KafkaError as error:
                logger.error(f"Kafka update consumer failed ({error}); reconnecting")
            finally:
                self._connected = False
                await self._close_consumer(consumer)

    def process_record(self, topic: str, raw_value: Optional[bytes]) -> Optional[str]:
        """Validates and processes one Kafka message. Returns the processing result or None when it fails."""
        try:
            message_dto: MessageResourceDTO = self._conversion_payload(raw_value)
            logger.error(f"message_dto is procesed")
            result = self._manager_resources.message_procesate(topic, message_dto)
            self.processed_messages += 1
            logger.info(f"Kafka message from topic '{topic}' processed: {result}")
            return result
        except Exception as error:
            self.failed_messages += 1
            logger.error(f"ERROR processing Kafka message from topic '{topic}': {error}")
            return None

    def _conversion_payload(self, raw_value: Optional[bytes]) -> MessageResourceDTO:
        if raw_value is None:
            raise ValueError("empty Kafka message")
        return KafkaEnvelopeDTO.model_validate_json(raw_value).to_message_resource()

    def _build_consumer(self) -> AIOKafkaConsumer:
        return AIOKafkaConsumer(
            *self._settings.consumer_topics,
            bootstrap_servers=self._settings.bootstrap_servers,
            client_id=self._settings.client_id,
            group_id=self._settings.group_id,
            auto_offset_reset=self._settings.auto_offset_reset,
            enable_auto_commit=True,
        )

    @staticmethod
    async def _close_consumer(consumer: AIOKafkaConsumer) -> None:
        try:
            await asyncio.wait_for(consumer.stop(), _STOP_TIMEOUT_SECONDS)
        except Exception as error:  # closing must never break the shutdown
            logger.debug(f"Error closing Kafka consumer: {error}")
