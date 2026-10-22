


from random import choice
import random
from typing import Dict
import uuid

from mcps1.application.utils import type_file_definition
from mcps1.domain.emun.data_use import DataUse
from mcps1.domain.emun.family_resources import FamilyResource
from mcps1.infraestructure.messaging.dtos.message_content_payload import MessageContentPayload
from mcps1.infraestructure.messaging.dtos.message_resource_dto import MessageResourceDTO


def create_random_message_resources(resource_build: FamilyResource = None) -> MessageResourceDTO:
    # Genera un objeto semi-aleatorio de tipo MessageResourceDTO

    type_resource = choice([DataUse.ASYNC_MESSAGE, DataUse.DB_TABLE, DataUse.REST_OBJ]) if FamilyResource.DATAS == resource_build else choice([DataUse.CONTEXT, DataUse.DOCUMENT, DataUse.RESULTS])
    family = resource_build

    # type_resource = choice([DataUse.ASYNC_MESSAGE, DataUse.DB_TABLE, DataUse.REST_OBJ])
    # family = choice([FamilyResource.COMMUNICATION, FamilyResource.CONTEXT, FamilyResource.DATAS])
    
    return MessageResourceDTO(
        type_resource=type_resource,
        family_resource=family,
        name_resource=f"folder1/folder_foo/data_{choice(['foo', 'bar', 'spanish_inquisition'])}_{str(uuid.uuid4())[:8]}",
        content=create_random_cotent_payload(type_resource)
    )

def create_random_cotent_payload(type_resource) -> MessageContentPayload:
    # Genera un payload semi-aleatorio de tipo MessageContentPayload
    descriptions = {
        "es_ES": f"Descripción aleatoria generada para el campo {str(uuid.uuid4())[:8]}.",
        "en_US": f"Random description generated for the field {str(uuid.uuid4())[:8]}."
    }
    fields_usable = [create_random_data_field_resources() for _ in range(3)]

    type_data: str = type_file_definition(type_resource)
    return MessageContentPayload(
        description=descriptions,
        competence=type_resource,
        process_apply=[choice(["api_MS", "listener_MS", "MCP_MS"]) for _ in range(random.randint(0, 2))],
        type_data=type_data,
        fields=fields_usable
    )

def create_random_data_field_resources() -> Dict[str, any]:
    """
    Factory function to generate and return a semi-randomly populated 
    DataResources object for testing purposes.
    """
    possible_types = ["string", "integer", "date", "boolean"]
    descriptions = {
        "es_ES": f"Descripción aleatoria generada para el campo {str(uuid.uuid4())[:8]}.",
        "en_US": f"Random description generated for the field {str(uuid.uuid4())[:8]}."
    }
    
    data_field = {
        "name": f"field_{choice(['reference', 'name', 'description'])}_{str(uuid.uuid4())[:8]}",
        "type": choice(possible_types),
        "description": descriptions,
        "validations": [choice(["mandatory", "unique", "no inject"]) for _ in range(random.randint(0, 2))]
    }

    return data_field