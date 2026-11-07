from typing import Protocol, runtime_checkable

@runtime_checkable
class KafkaPortOut(Protocol):
    """
    Defines the port for sending messages out of the core system using Kafka.
    This adheres to the hexagonal architecture pattern.
    """
    async def send_message_reporter(self, destiny_message: dict, message: dict) -> bool:
        """Best-effort report: returns True when the message was delivered, False otherwise (never raises)."""
        ...

    async def close(self) -> None:
        """Releases the connection with the broker."""
        ...
