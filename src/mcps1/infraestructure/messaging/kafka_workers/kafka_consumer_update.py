import logging
import os
from pathlib import Path
from typing import Dict
from dotenv import dotenv_values
from kafka import KafkaConsumer
import json

from mcps1.application.resources.ports.resource_maintender_port_in import ResourceMaintenderPortIn
from mcps1.domain.emun.data_use import DataUse
from mcps1.domain.emun.family_resources import FamilyResource
from mcps1.infraestructure.messaging.dtos.message_content_payload import MessageContentPayload
from mcps1.infraestructure.messaging.dtos.message_resource_dto import MessageResourceDTO


name = "mcps1"
logger = logging.getLogger(name)

class KafkaConsumerUpdate:
    """
    Handles consumption for updates (data_dictionary_update, context_update).
    Adheres to the 'mcps' consumer group concept and uses environment variables 
    for all connection parameters.
    """


    def __init__(self, manager_resources: ResourceMaintenderPortIn, type_run: str = "dev"):
        env_path = str([f for f in (Path(os.path.abspath("")).rglob(f".env.{type_run}")) if f.is_file()][0])
        self._resource_config: Dict[str, str] = dotenv_values(env_path)
        messaging_path = str([f for f in (Path(os.path.abspath("")).rglob(f".env.messaging.{type_run}")) if f.is_file()][0])
        self._messaging_config: Dict[str, any] = dotenv_values(messaging_path)
        
        self._manager_resources = manager_resources

        self._bootstrap_servers = self._messaging_config['KAFKA_BOOTSTRAP_SERVERS'] if "KAFKA_BOOTSTRAP_SERVERS" in self._messaging_config else os.getenv("bootstrap_servers")
        group_id = os.getenv("CONSUMER_GROUP_ID", "mcps_consumer_group") # Usar un default más explícito

        if not self._bootstrap_servers:
            raise EnvironmentError("KAFKA_BOOTSTRAP_SERVERS must be set in the environment variables.")

        default_topic = self._messaging_config['DEFAULT_TOPIC']
        
        logger.info(f":::::::::::::::::::::: bootstrap: {self._bootstrap_servers}")
        logger.info(f":::::::::::::::::::::: default_topic: {default_topic}")
        if default_topic:
            self._topics = [t.strip() for t in default_topic.split(',')]
        else:
            # Fallback a la lista explícita si el default no está configurado o para casos de uso específico
            self._topics = [t.strip() for t in os.environ.get("TOPIC_UPDATES", "update").split(',') if t.strip()]

        self._consumer = KafkaConsumer(
            *self._topics,
            bootstrap_servers=self._bootstrap_servers,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        logger.info(f"*********** consumer created en {self._bootstrap_servers}")

    async def consume_updates(self):
        """
        Starts consuming messages indefinitely, handling updates for dictionary and context.
        """
        logger.info("**************************************************Starting Kafka Consumer Update Listener...")
        try:
            listen: bool = True
            while listen:
                logger.info("**************************************************is listen the poll")
                messages = self._consumer.poll(timeout_ms=int(self._messaging_config['CONSUMER_POLL_TIMEOUT']))
                for tp, msg_list in messages.items():
                    logger.info("((((((((((((((((((((((((())))))))))))))))))))))))) messages recieved")
                    for message in msg_list:
                        try:
                            message_dto: MessageResourceDTO = self._conversion_payload(message=message)
                            self._manager_resources.message_procesate(message.topic, message_dto)
                        except Exception as e:
                            logger.error(f"ERROR processing Kafka message payload: {e}")
                if not messages and self._resource_config['ENVIRONMENT_EXECUTION'] == "test":
                    logger.info("************************************************** stop listed")
                    listen = False

        except KeyboardInterrupt:
            logger.error("Kafka Consumer Update Listener stopped by user.")

    def stop_consuming(self):
        """
        Starts consuming messages indefinitely, handling updates for dictionary and context.
        """
        logger.info("Starting Kafka Consumer Update Listener...")
        try:
            self._consumer.stop()
        except Exception as e:
            logger.error(f"[ERROR FATAL DE CONSUMO] Ocurrió un error crítico durante el consumo: {e}")
        finally:
            self._consumer.stop()

    def _conversion_payload(self, message) -> MessageResourceDTO:
        # Validaciones de estructura del mensaje asumidas del payload recibido
        content: MessageContentPayload = MessageContentPayload.model_validate(message.value['message_data']['dictionary_content']['content'])
        family: FamilyResource = FamilyResource[str(message.value['message_data']['type_data']).upper()] if str(message.value['message_data']['type_data']).upper() in FamilyResource.__members__ else FamilyResource.CONTEXT
        type_resource: DataUse = DataUse[str(message.value['message_data']['value']).upper()] if str(message.value['message_data']['value']).upper() in DataUse.__members__ else DataUse.DB_TABLE

        return MessageResourceDTO(content=content,
                                    name_resource=str(message.value['message_data']['source']),
                                    type_resource=type_resource,
                                    family_resource=family)
