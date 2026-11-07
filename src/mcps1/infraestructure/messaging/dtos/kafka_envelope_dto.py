"""
Envelope contract of the messages received from Kafka (topic "update").

Example::

    {
        "destination": {"resource_type": "datas", "resource_value": "asinc_messages"},
        "message_data": {
            "source": "ms_order",                 # emitter (informative)
            "family_resource": "datas",           # FamilyResource (value or name)
            "type_resource": "asinc_messages",    # DataUse (value or name)
            "content": {                          # plain resource object
                "name": "test_resource",          # file name of the resource
                "description": ["..."],
                "competence": "messaging",
                "process_apply": null,
                "type_data": "json",
                "fields": [...]
            }
        }
    }

The example above is stored as ``{RESOURCES_LOCATION}/datas/asinc_messages/test_resource.json``.
"""
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator

from mcps1.domain.emun.data_use import DataUse
from mcps1.domain.emun.family_resources import FamilyResource

from .message_content_payload import MessageContentPayload
from .message_resource_dto import MessageResourceDTO


def _enum_from_value_or_name(enum_cls: type[Enum], raw: Any) -> Any:
    """Accepts an enum member by value ("asinc_messages") or by name ("ASYNC_MESSAGE"), case-insensitive."""
    if isinstance(raw, enum_cls) or not isinstance(raw, str):
        return raw
    candidate = raw.strip()
    for member in enum_cls:
        if candidate.lower() == str(member.value).lower() or candidate.upper() == member.name:
            return member
    valid = ", ".join(str(member.value) for member in enum_cls)
    raise ValueError(f"'{raw}' is not a valid {enum_cls.__name__} (expected one of: {valid})")


class KafkaMessageDataDTO(BaseModel):
    source: Optional[str] = Field(None, description="Identifier of the service that emits the message.")
    family_resource: FamilyResource = Field(..., description="Family of the resource (datas, context, communication).")
    type_resource: DataUse = Field(..., description="Use of the resource (asinc_messages, database, rest_obj, markdown, pdf, csv).")
    content: Dict[str, Any] = Field(..., description="Plain resource object; its 'name' is the resource file name.")

    @field_validator("family_resource", mode="before")
    @classmethod
    def _parse_family(cls, raw: Any) -> Any:
        return _enum_from_value_or_name(FamilyResource, raw)

    @field_validator("type_resource", mode="before")
    @classmethod
    def _parse_data_use(cls, raw: Any) -> Any:
        return _enum_from_value_or_name(DataUse, raw)


class KafkaEnvelopeDTO(BaseModel):
    destination: Optional[Dict[str, Any]] = Field(None, description="Routing information (informative).")
    message_data: KafkaMessageDataDTO

    def to_message_resource(self) -> MessageResourceDTO:
        """Maps the envelope to the DTO consumed by ResourceMaintenderPortIn."""
        content: Dict[str, Any] = dict(self.message_data.content)
        source: str = str(self.message_data.source)
        name = content.pop("name", None)
        if not isinstance(name, str) or not name.strip():
            if source is not None:
                name = source
            else:
                raise ValueError("message_data.content.name is required (it is the resource file name)")

        return MessageResourceDTO(
            family_resource=self.message_data.family_resource,
            type_resource=self.message_data.type_resource,
            name_resource=name,
            content=MessageContentPayload.model_validate(content),
        )
