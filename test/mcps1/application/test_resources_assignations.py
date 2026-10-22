import os
import shutil

from fast_depends import inject
import pytest
import pytest_asyncio

from fastmcp.client import Client
from fastmcp.client.transports import StdioTransport

from mcps1.application.resources.adapters.management_adaptator import ManagerPortInDependency
from mcps1.application.resources.adapters.resources_adaptator import ResourcePortOutDependency
from mcps1.application.resources.ports.resource_maintender_port_in import ResourceMaintenderPortIn
from mcps1.application.resources.ports.resource_port_out import ResourcePortOut
from mcps1.domain.emun.family_resources import FamilyResource
from mcps1.infraestructure import mcp
from mcps1.infraestructure.messaging.dtos.message_resource_dto import MessageResourceDTO
from test.kafka_container_tester import rise_kafka_container
from test.utils_resources import create_random_message_resources

@inject
def get_resource_manager(resource_management: ManagerPortInDependency):
    return resource_management

@inject
def get_resource_assignation(resource_assignation: ResourcePortOutDependency):
    return resource_assignation

@pytest_asyncio.fixture
async def resource_manager():
    return get_resource_manager()

@pytest_asyncio.fixture
async def resource_assingantion():
    return get_resource_assignation()

@pytest_asyncio.fixture(scope="session", autouse=True)
async def global_setup():
    print("Setting up global resources...")
    os.environ["ENVIROMENT_EXECUTION"] = "test"
   
    # Yield to allow tests to run

    async with rise_kafka_container() as kafka:
        os.environ["bootstrap_servers"] = kafka.get_bootstrap_server()
        yield kafka.get_bootstrap_server()

        resources_test: str = "test/resources/"
        for filename in os.listdir(resources_test):
            file_path = os.path.join(resources_test, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.remove(file_path)
            else:
                shutil.rmtree(file_path)

        os.environ.pop("ENVIROMENT_EXECUTION", None)


@pytest.mark.asyncio
async def test_get_data_dictionary_success(resource_manager: ResourceMaintenderPortIn, resource_assingantion: ResourcePortOut):
    """Tests successful retrieval of a resource dictionary using DataDictionaries enum."""
    family_resource: FamilyResource = FamilyResource.DATAS

    message_object: MessageResourceDTO = create_random_message_resources(family_resource)
    resource_manager.message_procesate("create", message_object)

    # --- ACT: Perform the actual file read operation ---
    result: str = await resource_assingantion.get_data_dictionary(family_resource.value, message_object.name_resource)

    # ASSERTIONS
    assert None != result


@pytest.mark.asyncio
async def test_get_data_dictionary_bad_type(resource_manager: ResourceMaintenderPortIn, resource_assingantion: ResourcePortOut):
    """Tests successful retrieval of a resource dictionary using DataDictionaries enum."""
    family_resource: FamilyResource = FamilyResource.DATAS

    message_object: MessageResourceDTO = create_random_message_resources(family_resource)
    resource_manager.message_procesate("create", message_object)

    # --- ACT: Perform the actual file read operation ---
    with pytest.raises(Exception) as test_ex:
        await resource_assingantion.get_data_dictionary("foo_type", message_object.name_resource)

    assert "incorrent params to define resource path" == str(test_ex.value)

@pytest.mark.asyncio
async def test_get_data_dictionary_empty_type(resource_manager: ResourceMaintenderPortIn, resource_assingantion: ResourcePortOut):
    """Tests successful retrieval of a resource dictionary using DataDictionaries enum."""
    family_resource: FamilyResource = FamilyResource.DATAS

    message_object: MessageResourceDTO = create_random_message_resources(family_resource)
    resource_manager.message_procesate("create", message_object)

    # --- ACT: Perform the actual file read operation ---
    with pytest.raises(Exception) as test_ex:
        await resource_assingantion.get_data_dictionary(None, message_object.name_resource)

    assert "incorrent params to define resource path" == str(test_ex.value)

@pytest.mark.asyncio
async def test_get_data_dictionary_bad_value(resource_manager: ResourceMaintenderPortIn, resource_assingantion: ResourcePortOut):
    """Tests successful retrieval of a resource dictionary using DataDictionaries enum."""
    family_resource: FamilyResource = FamilyResource.DATAS

    message_object: MessageResourceDTO = create_random_message_resources(family_resource)
    resource_manager.message_procesate("create", message_object)

    # --- ACT: Perform the actual file read operation ---
    with pytest.raises(Exception) as test_ex:
        await resource_assingantion.get_data_dictionary(family_resource.value, None)

    assert "incorrent params to define resource path" == str(test_ex.value)

@pytest.mark.asyncio
async def test_get_data_dictionary_not_found(resource_manager: ResourceMaintenderPortIn, resource_assingantion: ResourcePortOut):
    """Tests successful retrieval of a resource dictionary using DataDictionaries enum."""
    family_resource: FamilyResource = FamilyResource.DATAS

    message_object: MessageResourceDTO = create_random_message_resources(family_resource)
    resource_manager.message_procesate("create", message_object)

    # --- ACT: Perform the actual file read operation ---
    result: str = await resource_assingantion.get_data_dictionary(family_resource.value, "foo_file")

    # ASSERTIONS
    assert "unkwon resource" == result

@pytest.mark.asyncio
async def test_get_data_dictionary_confict_value(resource_manager: ResourceMaintenderPortIn, resource_assingantion: ResourcePortOut):
    """Tests successful retrieval of a resource dictionary using DataDictionaries enum."""
    family_resource: FamilyResource = FamilyResource.DATAS

    message_object: MessageResourceDTO = create_random_message_resources(family_resource)
    resource_manager.message_procesate("create", message_object)

    # --- ACT: Perform the actual file read operation ---
    result: str = await resource_assingantion.get_data_dictionary(family_resource.value, "//nofile//")

    # ASSERTIONS
    assert "unkwon resource" == result
