from typing import Protocol, Dict, Any, runtime_checkable

from mcps1.infraestructure.messaging.dtos.message_resource_dto import MessageResourceDTO

@runtime_checkable
class ResourceMaintenderPortIn(Protocol):
    """
    Interfaz (Port) defining the contract for services responsible 
    with modifying and managing project resources within the MCP Server ecosystem.
    This port is used by domain modules to request state changes on resources.
    """
    
    def message_procesate(self, topic: str, payload: MessageResourceDTO) -> str:
        """
        Procesate asincronal messages from kafka and update the resources on MCP
        """
        ...
