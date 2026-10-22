# Adaptator para manejar la lógica de mensajería del MCP Server
# Implementará abstracciones sobre Kafka y otros sistemas de mensajes.

import contextlib
import logging
import os
from typing import Annotated

from fast_depends import Depends, inject

from mcps1.application.resources.adapters.management_adaptator import ManagerPortInDependency
from mcps1.infraestructure.messaging.kafka_workers.kafka_consumer_update import KafkaConsumerUpdate
from mcps1.infraestructure.messaging.kafka_workers.kafka_producer import KafkaProducerService
from mcps1.infraestructure.messaging.ports.kafka_port_out import KafkaPortOut

def get_kafka_port_out() -> KafkaPortOut:
    environment_execution = os.getenv("ENVIROMENT_EXECUTION", "dev")
    adapter = KafkaProducerService(type_run=environment_execution)
    return adapter


name = "mcps1"
logger = logging.getLogger(name)

@inject
@contextlib.asynccontextmanager
async def activate_manager_resource(resource_manager: ManagerPortInDependency):
    environment_execution = os.getenv("ENVIROMENT_EXECUTION", "dev")
    logger.info(f"*****************environment_execution: {environment_execution}")
    consumer = KafkaConsumerUpdate(resource_manager, environment_execution)
    consumer.consume_updates()
    yield consumer
    consumer.stop_consuming()

KafkaPortOutDependency = Annotated[KafkaPortOut, Depends(get_kafka_port_out)]

KafkaPortInConsumer = Annotated[None, Depends(activate_manager_resource)]
