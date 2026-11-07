import re

from pydantic import BaseModel, Field, field_validator
from typing import List, Dict, Any, Optional

from mcps1.domain.emun.data_use import DataUse
from mcps1.domain.emun.family_resources import FamilyResource

# Importación corregida del componente de contenido separado (ESTRUCTURA V2)
from .message_content_payload import MessageContentPayload 

# Relative path made of "word" segments (letters, digits, _, -, ., space) separated by "/".
# Prevents path traversal ("..", absolute paths) and glob characters in resource names.
_SAFE_RESOURCE_NAME = re.compile(r"^[\w\-. ]+(/[\w\-. ]+)*$")

class MessageResourceDTO(BaseModel):
    """
    DTO para mensajes de recursos serializables vía Kafka (VERSIÓN CORREGIDA). 
    Envuelve la información necesaria para el procesamiento en el consumidor.
    """
    family_resource: FamilyResource = Field(..., description="Familia del recurso que se está manejando (Origen).")
    type_resource: DataUse = Field(..., description="Tipo de uso del recurso.")
    name_resource: str = Field(..., description="Nombre único del recurso en el origen.")
    content: MessageContentPayload = Field(..., description="Carga útil completa y detallada del recurso.")

    @field_validator("name_resource")
    @classmethod
    def _validate_name_resource(cls, value: str) -> str:
        name = value.strip()
        segments = name.split("/")
        if not _SAFE_RESOURCE_NAME.match(name) or any(segment.strip() in ("", ".", "..") for segment in segments):
            raise ValueError(f"invalid resource name '{value}': must be a relative path without '..' segments")
        return name

    def model_dump_json(self, *args, **kwargs) -> str:
        """Serializa el objeto a JSON."""
        return super().model_dump_json(*args, **kwargs)