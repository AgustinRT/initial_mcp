import os
from typing import Annotated
from fast_depends import Depends, inject

from mcps1.application.resources.ports.resource_port_out import ResourcePortOut
from mcps1.application.resources.resources_service.resources_assignation import ResourcesAssignation
from mcps1.infraestructure.messaging.adapters.messaging_adaptator import KafkaPortOutDependency


@inject
def get_resource_port_out(kafka_producer: KafkaPortOutDependency) -> ResourcePortOut:
    """
    Función adaptadora responsable de inicializar y devolver la 
    implementación concreta para el puerto ResourcePortOut.
    """
    print("INFO: Inicializando ResourcesAssignment en el adaptador.")

    environment_execution = os.getenv("ENVIROMENT_EXECUTION", "dev")
    resource_object: ResourcesAssignation = ResourcesAssignation(kafka_producer, environment_execution)
    return resource_object

ResourcePortOutDependency = Annotated[
    ResourcePortOut,
    Depends(get_resource_port_out)
]
