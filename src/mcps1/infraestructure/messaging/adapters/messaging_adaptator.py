# Adaptator para manejar la lógica de mensajería del MCP Server
# Implementará abstracciones sobre Kafka y otros sistemas de mensajes.
#
# The providers only BUILD the workers; their lifecycle (start/stop) belongs to the
# FastMCP server lifespan (see mcps1.infraestructure.mcp.runtime).

import os
from typing import Annotated

from fast_depends import Depends

from mcps1.application.resources.adapters.management_adaptator import ManagerPortInDependency
from mcps1.infraestructure.messaging.kafka_workers.kafka_consumer_update import KafkaConsumerUpdate
from mcps1.infraestructure.messaging.kafka_workers.kafka_producer import KafkaProducerService
from mcps1.infraestructure.messaging.ports.kafka_port_in import KafkaPortIn
from mcps1.infraestructure.messaging.ports.kafka_port_out import KafkaPortOut


def get_kafka_port_out() -> KafkaPortOut:
    environment_execution = os.getenv("ENVIROMENT_EXECUTION", "dev")
    adapter = KafkaProducerService(type_run=environment_execution)
    return adapter


def get_kafka_consumer_update(resource_manager: ManagerPortInDependency) -> KafkaPortIn:
    """Builds (does not start) the consumer of the update topic, wired to the resource manager."""
    environment_execution = os.getenv("ENVIROMENT_EXECUTION", "dev")
    return KafkaConsumerUpdate(resource_manager, environment_execution)


KafkaPortOutDependency = Annotated[KafkaPortOut, Depends(get_kafka_port_out)]

KafkaConsumerUpdateDependency = Annotated[KafkaPortIn, Depends(get_kafka_consumer_update)]
