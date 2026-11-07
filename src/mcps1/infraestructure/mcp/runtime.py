import contextlib
import logging
from typing import Any, AsyncIterator, Callable, Optional

import anyio

from mcps1.application.resources.ports.resource_port_out import ResourcePortOut
from mcps1.infraestructure.messaging.ports.kafka_port_in import KafkaPortIn

logger = logging.getLogger("mcps1")

# Max time to release the Kafka connections when a session ends
_SHUTDOWN_TIMEOUT_SECONDS = 15


class McpServerRuntime:
    """
    Process-wide owner of the infrastructure shared by every MCP session:
    the Kafka update consumer and the resource reader (with its Kafka producer).

    ``mcp.server.fastmcp.FastMCP`` enters the lifespan once per session (stdio: once per
    process; streamable-http: once per client session). The runtime counts the active
    sessions so that only ONE consumer runs per process: it starts with the first
    session and stops when the last one ends.

    Components are created through factories (fast-depends providers) when they are
    needed, never at import time, so the server can start without a Kafka broker.
    """

    def __init__(self,
                 resource_reader_factory: Callable[[], ResourcePortOut],
                 update_consumer_factory: Callable[[], KafkaPortIn]):
        self._resource_reader_factory = resource_reader_factory
        self._update_consumer_factory = update_consumer_factory
        self._resource_reader: Optional[ResourcePortOut] = None
        self._update_consumer: Optional[KafkaPortIn] = None
        self._active_sessions = 0

    @property
    def resource_reader(self) -> ResourcePortOut:
        if self._resource_reader is None:
            self._resource_reader = self._resource_reader_factory()
        return self._resource_reader

    @property
    def update_consumer(self) -> Optional[KafkaPortIn]:
        return self._update_consumer

    @property
    def active_sessions(self) -> int:
        return self._active_sessions

    async def start(self) -> None:
        self._active_sessions += 1
        if self._active_sessions > 1:
            return
        try:
            consumer = self._update_consumer_factory()
            consumer.start()
            self._update_consumer = consumer
            logger.info("Kafka update consumer activated with the MCP server lifespan")
        except Exception as error:
            # Without the consumer the server still serves tools and resources
            logger.error(f"Kafka update consumer could not be activated: {error}")

    async def stop(self) -> None:
        if self._active_sessions == 0:
            return
        self._active_sessions -= 1
        if self._active_sessions > 0:
            return
        consumer, self._update_consumer = self._update_consumer, None
        if consumer is not None:
            await consumer.stop()
        reader, self._resource_reader = self._resource_reader, None
        if reader is not None:
            with contextlib.suppress(Exception):
                await reader.aclose()

    @contextlib.asynccontextmanager
    async def lifespan(self, server: Any) -> AsyncIterator[dict[str, Any]]:
        """FastMCP lifespan: keeps the Kafka consumer alive while the server (session) is active."""
        await self.start()
        try:
            yield {"runtime": self}
        finally:
            # Sessions are usually torn down by cancelling their anyio scope: shield the
            # cleanup so the consumer and the producer are always closed.
            with anyio.move_on_after(_SHUTDOWN_TIMEOUT_SECONDS, shield=True):
                await self.stop()
