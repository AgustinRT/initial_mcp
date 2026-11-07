from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing import List, Dict, Any, Optional

# Locale assigned when a localized text arrives without locale keys (plain string or list)
DEFAULT_LOCALE = "es_ES"


def to_localized_text(value: Any) -> Any:
    """
    Normalizes a localized text to the ``{"locale": "text"}`` structure.
    - dict            -> unchanged
    - list / tuple    -> {"es_ES": first item} ({} when empty)
    - str             -> {"es_ES": value}
    Any other value is returned unchanged so that pydantic reports the error.
    """
    if isinstance(value, (list, tuple)):
        return {DEFAULT_LOCALE: str(value[0])} if value else {}
    if isinstance(value, str):
        return {DEFAULT_LOCALE: value}
    return value


class MessageContentPayload(BaseModel):
    """ 
    Contiene los campos de un recurso (DataResources), excluyendo el nombre.
    Representa el payload detallado dentro del mensaje Kafka.
    """
    model_config = ConfigDict(from_attributes=True)

    description: Dict[str, str] = Field(..., description="Descripción multilingüe del recurso.")
    competence: str = Field(..., description="Tipo funcional o competencia asociada al recurso (DataUse).")
    process_apply: Optional[List[str]] = Field(None, description="Lista de procesos aplicables.")
    type_data: str = Field(..., description="Tipo de dato del recurso (ej: json, dbRow, http/json).")
    fields: List[Dict] = Field(..., description="Listado de campos estructurados dentro del recurso.")

    @field_validator("description", mode="before")
    @classmethod
    def _normalize_description(cls, value: Any) -> Any:
        return to_localized_text(value)

    @field_validator("fields", mode="before")
    @classmethod
    def _normalize_fields_description(cls, value: Any) -> Any:
        if not isinstance(value, list):
            return value
        return [
            {**field, "description": to_localized_text(field["description"])}
            if isinstance(field, dict) and "description" in field else field
            for field in value
        ]
