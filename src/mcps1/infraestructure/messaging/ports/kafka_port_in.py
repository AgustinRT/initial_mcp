from typing import Protocol, runtime_checkable


@runtime_checkable
class KafkaPortIn(Protocol):
    """
    Driving port for the Kafka consumers of the MCP server.
    The implementation listens to its topics in a background task and delegates every
    message to the application layer (ResourceMaintenderPortIn).
    """

    @property
    def is_running(self) -> bool:
        """True while the background consumption task is alive."""
        ...

    def start(self) -> None:
        """Starts consuming in a background task of the running event loop (idempotent)."""
        ...

    async def stop(self) -> None:
        """Stops the background task and releases the Kafka connection."""
        ...
