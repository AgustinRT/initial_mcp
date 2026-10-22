from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

from mcps1.domain.emun.data_use import DataUse
from mcps1.domain.emun.family_resources import FamilyResource

# Importación corregida del componente de contenido separado (ESTRUCTURA V2)
from .message_content_payload import MessageContentPayload 

class MessageResourceDTO(BaseModel):
    """
    DTO para mensajes de recursos serializables vía Kafka (VERSIÓN CORREGIDA). 
    Envuelve la información necesaria para el procesamiento en el consumidor.
    """
    family_resource: FamilyResource = Field(..., description="Familia del recurso que se está manejando (Origen).")
    type_resource: DataUse = Field(..., description="Tipo de uso del recurso.")
    name_resource: str = Field(..., description="Nombre único del recurso en el origen.")
    content: MessageContentPayload = Field(..., description="Carga útil completa y detallada del recurso.")

    def model_dump_json(self, *args, **kwargs) -> str:
        """Serializa el objeto a JSON."""
        return super().model_dump_json(*args, **kwargs)