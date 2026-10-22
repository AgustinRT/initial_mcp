import os
from typing import Annotated
from fast_depends import Depends

from mcps1.application.resources.ports.resource_maintender_port_in import ResourceMaintenderPortIn
from mcps1.application.resources.resources_service.resource_management import ResourceManagement


def get_management_port_in() -> ResourceMaintenderPortIn:
    """
    Función adaptadora responsable de inicializar y devolver la 
    implementación concreta para el puerto ResourceMaintenderPortIn.
    """
    print("INFO: Inicializando ResourcesAssignment en el adaptador.")
    environment_execution = os.getenv("ENVIROMENT_EXECUTION", "dev")
    manger_object: ResourceManagement = ResourceManagement(environment_execution)
    return manger_object

ManagerPortInDependency = Annotated[
    ResourceMaintenderPortIn,
    Depends(get_management_port_in)
]
