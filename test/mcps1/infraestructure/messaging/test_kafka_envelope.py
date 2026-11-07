"""Unit tests of the Kafka "update" message contract (no broker needed)."""
import json

import pytest
from pydantic import ValidationError

from mcps1.domain.emun.data_use import DataUse
from mcps1.domain.emun.family_resources import FamilyResource
from mcps1.infraestructure.messaging.dtos.kafka_envelope_dto import KafkaEnvelopeDTO
from mcps1.infraestructure.messaging.dtos.message_content_payload import MessageContentPayload
from mcps1.infraestructure.messaging.dtos.message_resource_dto import MessageResourceDTO
from test.utils_resources import build_update_envelope, user_example_content


def to_resource(envelope: dict) -> MessageResourceDTO:
    return KafkaEnvelopeDTO.model_validate_json(json.dumps(envelope)).to_message_resource()


def test_user_example_is_mapped_to_resource_dto():
    content = user_example_content("test_resource")

    resource = to_resource(build_update_envelope(content))

    assert resource.family_resource == FamilyResource.DATAS
    assert resource.type_resource == DataUse.ASYNC_MESSAGE
    assert resource.name_resource == "test_resource"
    # description list -> {"es_ES": first item}
    assert resource.content.description == {"es_ES": content["description"][0]}
    assert resource.content.competence == "messaging"
    assert resource.content.process_apply is None
    assert resource.content.type_data == "json"
    assert resource.content.fields == content["fields"]
    # the name is the file name, it is not part of the stored content
    assert "name" not in resource.content.model_dump()


def test_envelope_accepts_enum_names_case_insensitive():
    resource = to_resource(build_update_envelope(user_example_content(), family_resource="DATAS",
                                                 type_resource="async_message"))

    assert resource.family_resource == FamilyResource.DATAS
    assert resource.type_resource == DataUse.ASYNC_MESSAGE


@pytest.mark.parametrize("description, expected", [
    (["primero", "segundo"], {"es_ES": "primero"}),
    ("texto plano", {"es_ES": "texto plano"}),
    ({"es_ES": "hola", "en_US": "hello"}, {"es_ES": "hola", "en_US": "hello"}),
    ([], {}),
])
def test_description_is_normalized_to_localized_dict(description, expected):
    payload = MessageContentPayload.model_validate(
        {"description": description, "competence": "messaging", "type_data": "json", "fields": []})

    assert payload.description == expected


def test_fields_description_list_is_normalized():
    content = user_example_content()
    content["fields"][0]["description"] = ["identificador", "identifier"]

    resource = to_resource(build_update_envelope(content))

    assert resource.content.fields[0]["description"] == {"es_ES": "identificador"}


@pytest.mark.parametrize("name", [None, "", "   "])
def test_missing_name_is_rejected(name):
    content = user_example_content()
    content["name"] = name

    with pytest.raises(ValueError, match="name is required"):
        to_resource(build_update_envelope(content))


@pytest.mark.parametrize("bad_name", ["../outside", "/etc/passwd", "folder/../../x", "a*b", "folder//x"])
def test_unsafe_names_are_rejected(bad_name):
    with pytest.raises(ValidationError, match="invalid resource name"):
        to_resource(build_update_envelope(user_example_content(bad_name)))


def test_subfolders_are_allowed_in_name():
    resource = to_resource(build_update_envelope(user_example_content("questions/v2/test_resource")))

    assert resource.name_resource == "questions/v2/test_resource"


@pytest.mark.parametrize("envelope", [
    build_update_envelope(user_example_content(), family_resource="unknown_family"),
    build_update_envelope(user_example_content(), type_resource="unknown_use"),
    {"destination": {}},                                   # message_data missing
    user_example_content(),                                # plain object without envelope
])
def test_invalid_envelopes_are_rejected(envelope):
    with pytest.raises(ValidationError):
        to_resource(envelope)


def test_previous_field_names_are_rejected():
    envelope = {"message_data": {"source": "legacy_producer", "type_data": "datas", "value": "asinc_messages",
                                 "dictionary_content": user_example_content()}}

    with pytest.raises(ValidationError) as error:
        to_resource(envelope)

    missing = {err["loc"][-1] for err in error.value.errors() if err["type"] == "missing"}
    assert missing == {"family_resource", "type_resource", "content"}


def test_invalid_json_is_rejected():
    with pytest.raises(ValidationError):
        KafkaEnvelopeDTO.model_validate_json(b"{not json")
