import asyncio
import os
from pathlib import Path
from typing import Dict
from dotenv import dotenv_values
from kafka import KafkaProducer
import json

class KafkaProducerService():
    """
    Manages Kafka production for the MCP system.
    Implements KafkaPortOut to integrate with the core domain logic 
    following the hexagonal architecture pattern.
    """
    def __init__(self, type_run: str = "dev"):
        """
        Initializes the Kafka Producer using environment variables.
        """

        messaging_path = str([f for f in (Path(os.path.abspath("")).rglob(f".env.messaging.{type_run}")) if f.is_file()][0])
        self._messaging_config: Dict[str, str] = dotenv_values(messaging_path)
        bootstrap_servers = self._messaging_config['KAFKA_BOOTSTRAP_SERVERS'] if "KAFKA_BOOTSTRAP_SERVERS" in self._messaging_config else os.getenv("bootstrap_servers")
    
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.default_topic = self._messaging_config.get("DEFAULT_PRODUCER_TOPIC", "report")

    def get_or_create_loop(self):
        try:
            return asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop

    async def send_message_reporter(self, destiny_message: dict, message: dict) -> None:
        """
        Sends a structured report message to Kafka using the configured default topic.
        This implements the KafkaPortOut interface method.

        :param destiny_message: Information about where the message is intended.
        :param message: The core data payload of the message.
        """
        try:
            payload = {
                "destination": destiny_message,
                "message_data": message
            }

            # Send the asynchronous message
            self.producer.send(self.default_topic, value=payload)
            self.producer.flush()

            # Optionally wait for confirmation or handle callback 
            # For simplicity, we just send and assume async success here.
            print(f"Successfully sent message report to topic: {self.default_topic}")

        except Exception as e:
            print(f"Error sending message reporter via Kafka: {e}")
            # In a real system, logging/metrics should capture this failure
            raise
