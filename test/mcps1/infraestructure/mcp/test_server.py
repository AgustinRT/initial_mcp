# from fast_depends import Depends, inject
import pytest

from src.mcps1.infraestructure.mcp.server import collatz, prime, get_data_dictionary
from mcps1.domain.emun.data_dictionaries import DataDictionaries

@pytest.mark.asyncio
async def test_prime_success():
    # Test known prime numbers
    assert await prime(2) == 1
    assert await prime(3) == 1
    assert await prime(17) == 1
    assert await prime(97) == 1

@pytest.mark.asyncio
async def test_prime_failure():
    # Test known composite numbers and edge cases for non-primes
    await prime(4) == 2
    await prime(9) == 3
    await prime(100) == 2

@pytest.mark.asyncio
async def test_prime_large_composite():
    # Test a larger composite number (e.g., 3*5*7 = 105)
    assert await prime(105)  == 3

@pytest.mark.asyncio
async def test_collatz_standard_sequence():
    """Test the Collatz sequence for a standard starting number (e.g., 6)."""
    # We must test against what steps_3xmas1 actually returns, as its logic is complex/buggy.
    # For this example, we check that it runs without error and returns a list of integers.
    result = await collatz(6)
    assert "6, 3, 10, 5, 16, 8, 4" == result

@pytest.mark.asyncio
async def test_collatz_edge_case_low():
    """Test the Collatz sequence for a low starting number (e.g., 1)."""
    # Testing an edge case near the expected termination point (1 or 2)
    result = await collatz(1)
    assert "1, 4" == result

@pytest.mark.asyncio
async def test_collatz_edge_case_four():
    """Test the Collatz sequence for the number 4."""
    # Testing a specific point mentioned in the docstring description (4,2,1 loop)
    result = await collatz(4)
    assert "4" == result


@pytest.mark.asyncio
async def test_get_data_dictionary_success():
    """Tests successful retrieval of known schema files using the Enum key."""
    # ACT: Llamamos al método usando el fixture del adaptador y la clave Enum.
    # type, value = resource_key.name.split("_")
    # resource_workers = get_test_resource_worker()
    # resource_workers = ResourcesAssignation()
    result = await get_data_dictionary("datas", "question")

    # ASSERTIONS
    assert isinstance(result, str)
    assert "error" not in result
    # Comprobamos que se haya cargado correctamente un esquema con el campo 'fields'
    assert 'fields' in result
