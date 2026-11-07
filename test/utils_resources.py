


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

# ---------------------------------------------------------------------------
# Kafka "update" messages
# ---------------------------------------------------------------------------

def user_example_content(name: str = "test_resource") -> Dict[str, any]:
    """The plain resource object of the reference example (its name is the resource file name)."""
    description = ("kafka_message es un objeto seriablizable como JSON para emitir mensages entre un kafka_producer "
                   "y un kafka_listener. El contenido del mismo es una pregunta formulada por un usuario humano, de "
                   "tipo simple. deberá contestarse con acceso a una tool directa del MCP o con una consulta siempre "
                   "a la base de datos. No deberá requerir contexto en la especificación de la pregunta ni "
                   "razonamiento en la parte del MCP")
    return {
        "name": name,
        "description": [description, description + " (segunda versión)"],
        "competence": "messaging",
        "process_apply": None,
        "type_data": "json",
        "fields": [
            {"name": "id", "type": "long",
             "description": {"es_ES": "identificador único de la pregunta"},
             "validations": ["not null"]},
            {"name": "prompt", "type": "text",
             "description": {"es_ES": "pregunta formulada por el usuario a nuestro sistema"},
             "validations": ["not null", "not blank", "not inject code"]},
            {"name": "tags", "type": "text",
             "description": {"es_ES": "Palabras clave o etiquetas que clasifican el contexto de la pregunta."},
             "validations": ["not blank"]},
        ],
    }


def build_update_envelope(content: Dict[str, any], family_resource: str = "datas",
                          type_resource: str = "asinc_messages", source: str = "test_producer") -> Dict[str, any]:
    """Envelope accepted by the update consumer (see KafkaEnvelopeDTO)."""
    return {
        "destination": {"resource_type": family_resource, "resource_value": type_resource},
        "message_data": {
            "source": source,
            "family_resource": family_resource,
            "type_resource": type_resource,
            "content": content,
        },
    }


def unique_name(prefix: str = "test_resource") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def wait_until(predicate, timeout: float = 30.0, interval: float = 0.2) -> bool:
    """Polls an (a)sync predicate until it is truthy or the timeout expires."""
    import asyncio
    import inspect
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        result = predicate()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return True
        await asyncio.sleep(interval)
    return False


# ---------------------------------------------------------------------------
# Port fakes (used with fast_depends.dependency_provider overrides)
# ---------------------------------------------------------------------------

class FakeKafkaPortIn:
    """KafkaPortIn fake that records its lifecycle."""
    instances: list["FakeKafkaPortIn"] = []

    def __init__(self):
        self.started = False
        self.stopped = False
        FakeKafkaPortIn.instances.append(self)

    @property
    def is_running(self) -> bool:
        return self.started and not self.stopped

    def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


class FakeKafkaPortOut:
    """KafkaPortOut fake that records the reported messages."""

    def __init__(self):
        self.sent: list[tuple[dict, dict]] = []
        self.closed = False

    async def send_message_reporter(self, destiny_message: dict, message: dict) -> bool:
        self.sent.append((destiny_message, message))
        return True

    async def close(self) -> None:
        self.closed = True
