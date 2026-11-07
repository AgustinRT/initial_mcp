from fast_depends import inject
import pytest
import pytest_asyncio

from mcps1.application.resources.adapters.management_adaptator import ManagerPortInDependency
from mcps1.application.resources.ports.resource_maintender_port_in import ResourceMaintenderPortIn
from mcps1.domain.emun.family_resources import FamilyResource
from mcps1.infraestructure.messaging.dtos.message_resource_dto import MessageResourceDTO
from test.utils_resources import create_random_message_resources


@inject
def get_resource_manager(resource_management: ManagerPortInDependency):
    return resource_management

@pytest_asyncio.fixture
async def resource_manager():
    return get_resource_manager()

@pytest.mark.asyncio
async def test_create_message_resource_happy_path(resource_manager: ResourceMaintenderPortIn):
    """
    TEST HAPPY PATH: Verifica que un DTO de mensaje se transforma correctamente en DataResources 
    y simula la creación exitosa del recurso.
    """

    message_object: MessageResourceDTO = create_random_message_resources(FamilyResource.DATAS)
    resource_created: str = resource_manager.message_procesate("update", message_object)

    assert resource_created is not None
    new_file_name = "".join(resource_created.split("/")[-1:][0].split(".")[:-1])
    assert message_object.name_resource.split("/")[-1:][0] == new_file_name

@pytest.mark.asyncio
async def test_create_message_resource_fail_topic(resource_manager: ResourceMaintenderPortIn):
    """
    TEST HAPPY PATH: Verifica que un DTO de mensaje se transforma correctamente en DataResources 
    y simula la creación exitosa del recurso.
    """

    message_object: MessageResourceDTO = create_random_message_resources(FamilyResource.DATAS)

    with pytest.raises(Exception) as test_ex:
        resource_manager.message_procesate("foo", message_object)

    assert "incorrect topic" == str(test_ex.value)

@pytest.mark.asyncio
async def test_create_message_resource_fail_name_path(resource_manager: ResourceMaintenderPortIn):
    """
    TEST HAPPY PATH: Verifica que un DTO de mensaje se transforma correctamente en DataResources 
    y simula la creación exitosa del recurso.
    """

    message_object: MessageResourceDTO = create_random_message_resources(FamilyResource.DATAS)

    message_object.name_resource = None
    with pytest.raises(Exception) as test_ex:
        resource_manager.message_procesate("update", message_object)

    assert "incorrect filename or path definition" == str(test_ex.value)


@pytest.mark.asyncio
async def test_create_message_resource_fail_no_content(resource_manager: ResourceMaintenderPortIn):
    """
    TEST HAPPY PATH: Verifica que un DTO de mensaje se transforma correctamente en DataResources 
    y simula la creación exitosa del recurso.
    """

    message_object: MessageResourceDTO = create_random_message_resources(FamilyResource.DATAS)

    message_object.content = None
    with pytest.raises(Exception) as test_ex:
        resource_manager.message_procesate("update", message_object)

    assert "empty data content" == str(test_ex.value)


@pytest.mark.asyncio
async def test_update_message_resource_happy_path(resource_manager: ResourceMaintenderPortIn):
    """
    TEST HAPPY PATH: Verifica que un DTO de mensaje se transforma correctamente en DataResources 
    y simula la creación exitosa del recurso.
    """

    message_object: MessageResourceDTO = create_random_message_resources(FamilyResource.DATAS)
    resource_manager.message_procesate("update", message_object)

    message_object_2: MessageResourceDTO = create_random_message_resources(FamilyResource.DATAS)
    message_object_2.name_resource = message_object.name_resource
    resource_created_2: str = resource_manager.message_procesate("update", message_object_2)

    assert resource_created_2 is not None
    new_file_name = "".join(resource_created_2.split("/")[-1:][0].split(".")[:-1])
    assert message_object_2.name_resource.split("/")[-1:][0] == new_file_name


@pytest.mark.asyncio
async def test_delete_message_resource_happy_path(resource_manager: ResourceMaintenderPortIn):
    """
    TEST HAPPY PATH: Verifica que un DTO de mensaje se transforma correctamente en DataResources 
    y simula la creación exitosa del recurso.
    """

    message_object: MessageResourceDTO = create_random_message_resources(FamilyResource.DATAS)
    resource_created: str = resource_manager.message_procesate("update", message_object)

    resource_deleted: str = resource_manager.message_procesate("delete", message_object)

    assert resource_created is not None
    new_file_name = "".join(resource_created.split("/")[-1:][0].split(".")[:-1])
    assert message_object.name_resource.split("/")[-1:][0] == new_file_name
    assert f"{resource_created} was been deleted" == resource_deleted


@pytest.mark.asyncio
async def test_delete_message_resource_fail_name_path(resource_manager: ResourceMaintenderPortIn):
    """
    TEST HAPPY PATH: Verifica que un DTO de mensaje se transforma correctamente en DataResources 
    y simula la creación exitosa del recurso.
    """

    message_object: MessageResourceDTO = create_random_message_resources(FamilyResource.DATAS)

    message_object.name_resource = None
    with pytest.raises(Exception) as test_ex:
        resource_manager.message_procesate("delete", message_object)

    assert "incorrect filename or path definition" == str(test_ex.value)
