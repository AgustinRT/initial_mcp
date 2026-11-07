import asyncio
import contextlib
import json
import logging
import time
from typing import Optional

from aiokafka import AIOKafkaProducer

from mcps1.infraestructure.messaging.kafka_workers.kafka_settings import KafkaSettings

name = "mcps1"
logger = logging.getLogger(name)


class KafkaProducerService():
    """
    Manages Kafka production for the MCP system.
    Implements KafkaPortOut to integrate with the core domain logic 
    following the hexagonal architecture pattern.

    The connection is lazy and best-effort: the MCP server must work without a broker,
    so a report that cannot be delivered is logged and discarded (never raises).
    """
    def __init__(self, type_run: str = "dev", settings: Optional[KafkaSettings] = None):
        """
        Reads the configuration. No connection is opened until the first message is sent.
        """
        self._settings = settings or KafkaSettings.load(type_run)
        self.default_topic = self._settings.producer_topic
        self._producer: Optional[AIOKafkaProducer] = None
        self._producer_loop: Optional[asyncio.AbstractEventLoop] = None
        self._start_lock: Optional[asyncio.Lock] = None
        self._lock_loop: Optional[asyncio.AbstractEventLoop] = None
        self._retry_after = 0.0

    async def send_message_reporter(self, destiny_message: dict, message: dict) -> bool:
        """
        Sends a structured report message to Kafka using the configured default topic.
        This implements the KafkaPortOut interface method.

        :param destiny_message: Information about where the message is intended.
        :param message: The core data payload of the message.
        :return: True when the broker acknowledged the message.
        """
        payload = {
            "destination": destiny_message,
            "message_data": message
        }
        producer = await self._get_producer()
        if producer is None:
            logger.info(f"Kafka not available: report for topic '{self.default_topic}' discarded")
            return False
        try:
            await asyncio.wait_for(producer.send_and_wait(self.default_topic, value=payload),
                                   self._settings.producer_timeout_seconds)
            logger.info(f"Successfully sent message report to topic: {self.default_topic}")
            return True
        except Exception as e:
            logger.warning(f"Error sending message reporter via Kafka: {e}")
            return False

    async def close(self) -> None:
        producer, self._producer = self._producer, None
        if producer is not None and self._producer_loop is asyncio.get_running_loop():
            with contextlib.suppress(Exception):
                await producer.stop()

    async def _get_producer(self) -> Optional[AIOKafkaProducer]:
        loop = asyncio.get_running_loop()
        if self._producer is not None and self._producer_loop is loop:
            return self._producer
        if not self._settings.bootstrap_servers or time.monotonic() < self._retry_after:
            return None

        if self._start_lock is None or self._lock_loop is not loop:
            self._start_lock, self._lock_loop = asyncio.Lock(), loop
        async with self._start_lock:
            if self._producer is not None and self._producer_loop is loop:
                return self._producer
            # A producer bound to another (closed) event loop can not be reused
            self._producer = None
            producer = AIOKafkaProducer(
                bootstrap_servers=self._settings.bootstrap_servers,
                client_id=self._settings.client_id,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            )
            try:
                await asyncio.wait_for(producer.start(), self._settings.producer_timeout_seconds)
            except Exception as e:
                with contextlib.suppress(Exception):
                    await producer.stop()
                self._retry_after = time.monotonic() + self._settings.producer_retry_cooldown_seconds
                logger.warning(f"Kafka producer can not connect to {self._settings.bootstrap_servers} ({e}); "
                               f"retrying in {self._settings.producer_retry_cooldown_seconds:.0f}s")
                return None
            self._producer, self._producer_loop = producer, loop
            return producer
