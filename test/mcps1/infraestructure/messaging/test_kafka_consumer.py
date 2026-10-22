from contextlib import asynccontextmanager
import os
import shutil
import time

from fast_depends import inject
from kafka import KafkaProducer
import pytest
import pytest_asyncio

from mcps1.application.resources.adapters.resources_adaptator import ResourcePortOutDependency
from mcps1.application.resources.ports.resource_port_out import ResourcePortOut
from mcps1.domain.emun.family_resources import FamilyResource
from mcps1.infraestructure.messaging.adapters.messaging_adaptator import KafkaPortInConsumer
from mcps1.infraestructure.messaging.dtos.message_resource_dto import MessageResourceDTO
from test.kafka_container_tester import producer_tester, rise_kafka_container
from test.utils_resources import create_random_message_resources

@pytest_asyncio.fixture(scope="function")
async def producer_container(global_setup) -> KafkaProducer:
    producer: KafkaProducer = producer_tester(global_setup)
    return producer

@inject
def get_resource_assignation(resource_assignation: ResourcePortOutDependency):
    return resource_assignation

@pytest_asyncio.fixture
async def resource_assingantion():
    return get_resource_assignation()


@inject
@asynccontextmanager
async def inite_consumer(initer_consumer: KafkaPortInConsumer):
    yield initer_consumer

@pytest_asyncio.fixture
async def consumer_container():
    yield inite_consumer()

@pytest_asyncio.fixture(scope="session", autouse=True)
async def global_setup():
    print("Setting up global resources...")
    os.environ["ENVIROMENT_EXECUTION"] = "test"
    
    # yield inite_consumer()
    async with rise_kafka_container() as kafka:
        os.environ["bootstrap_servers"] = kafka.get_bootstrap_server()
        yield kafka.get_bootstrap_server()
        print("was end")

        resources_test: str = "test/resources/"
        for filename in os.listdir(resources_test):
            file_path = os.path.join(resources_test, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.remove(file_path)
            else:
                shutil.rmtree(file_path)

        os.environ.pop("ENVIROMENT_EXECUTION", None)
    

@pytest.mark.asyncio
async def test_simply_kafka_consumer(producer_container: KafkaProducer, resource_assingantion: ResourcePortOut):
    """
    Tests the creation and management of a generic data resource object 
    using the DataResources model, demonstrating domain layer usage within tests.
    """

    message_object: MessageResourceDTO = create_random_message_resources(FamilyResource.DATAS)
    familly: str = message_object.family_resource.value
    type: str = message_object.type_resource.value
    message_dict = message_object.model_dump()
    message_dict['family_resource']=familly
    message_dict['type_resource']=type

    kafka_payload = {
        "source": message_object.name_resource,
        "type_data": message_object.family_resource.value,
        "value": message_object.type_resource.value,
        "dictionary_content": message_dict
    }
    payload = {
        "destination": {"resource_type": message_object.family_resource.value, "resource_value": message_object.type_resource.value},
        "message_data": kafka_payload
    }
    producer_container.send("update", value=payload)
    producer_container.flush()
    # time.sleep(10)
    inite_consumer()

    result: str = await resource_assingantion.get_data_dictionary(message_object.family_resource.value, message_object.name_resource)

    # ASSERTIONS
    assert None != result


@pytest.mark.asyncio
async def test_simply_kafka_consumer_multiple(producer_container: KafkaProducer, resource_assingantion: ResourcePortOut):
    """
    Tests the creation and management of a generic data resource object 
    using the DataResources model, demonstrating domain layer usage within tests.
    """

    message_object: MessageResourceDTO = create_random_message_resources(FamilyResource.DATAS)
    familly: str = message_object.family_resource.value
    type: str = message_object.type_resource.value
    message_dict = message_object.model_dump()
    message_dict['family_resource']=familly
    message_dict['type_resource']=type

    kafka_payload = {
        "source": message_object.name_resource,
        "type_data": message_object.family_resource.value,
        "value": message_object.type_resource.value,
        "dictionary_content": message_dict
    }
    payload = {
        "destination": {"resource_type": message_object.family_resource.value, "resource_value": message_object.type_resource.value},
        "message_data": kafka_payload
    }
    producer_container.send("update", value=payload)
    producer_container.flush()

    message_object_2: MessageResourceDTO = create_random_message_resources(FamilyResource.DATAS)
    familly_2: str = message_object_2.family_resource.value
    type_2: str = message_object_2.type_resource.value
    message_dict = message_object_2.model_dump()
    message_dict['family_resource']=familly_2
    message_dict['type_resource']=type_2

    kafka_payload_2 = {
        "source": message_object_2.name_resource,
        "type_data": message_object_2.family_resource.value,
        "value": message_object_2.type_resource.value,
        "dictionary_content": message_dict
    }
    payload_2 = {
        "destination": {"resource_type": message_object_2.family_resource.value, "resource_value": message_object_2.type_resource.value},
        "message_data": kafka_payload_2
    }
    producer_container.send("update", value=payload_2)
    producer_container.flush()

    message_object_3: MessageResourceDTO = create_random_message_resources(FamilyResource.DATAS)
    familly_3: str = message_object_3.family_resource.value
    type_3: str = message_object_3.type_resource.value
    message_dict = message_object_3.model_dump()
    message_dict['family_resource']=familly_3
    message_dict['type_resource']=type_3

    kafka_payload_3 = {
        "source": message_object_3.name_resource,
        "type_data": message_object_3.family_resource.value,
        "value": message_object_3.type_resource.value,
        "dictionary_content": message_dict
    }
    payload_3 = {
        "destination": {"resource_type": message_object_3.family_resource.value, "resource_value": message_object_3.type_resource.value},
        "message_data": kafka_payload_3
    }
    producer_container.send("update", value=payload_3)
    producer_container.flush()

    # time.sleep(10)
    inite_consumer()

    result: str = await resource_assingantion.get_data_dictionary(message_object.family_resource.value, message_object.name_resource)
    result2: str = await resource_assingantion.get_data_dictionary(message_object_2.family_resource.value, message_object_2.name_resource)
    result3: str = await resource_assingantion.get_data_dictionary(message_object_3.family_resource.value, message_object_3.name_resource)

    # ASSERTIONS
    assert None != result
    assert None != result2
    assert None != result3
