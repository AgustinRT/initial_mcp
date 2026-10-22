import pytest
import asyncio
from src.mcps1.application.tools.mathgames import MathGames

mathgames: MathGames = MathGames()

# @pytest.fixture
# async def mathgames():
#     """Fixture to provide an instance of MathGames."""
#     return MathGames()

@pytest.mark.asyncio
async def test_is_prime_success():
    # Test known prime numbers (small and moderate)
    assert await mathgames.is_prime(1) == 1
    assert await mathgames.is_prime(2) == 1
    assert await mathgames.is_prime(3) == 1
    assert await mathgames.is_prime(5) == 1
    assert await mathgames.is_prime(17) == 1


@pytest.mark.asyncio
async def test_is_prime_failure():
    # Test known composite numbers and edge cases for non-primes
    assert await mathgames.is_prime(4) == 2
    assert await mathgames.is_prime(9) == 3
    assert await mathgames.is_prime(100) == 2

@pytest.mark.asyncio
async def test_is_prime_large_composite():
    # Test a larger composite number (e.g., 3*5*7 = 105)
    assert await mathgames.is_prime(105) == 3

@pytest.mark.asyncio
async def test_is_prime_large_prime():
    # Test a known large prime number (e.g., 97)
    assert await mathgames.is_prime(97) == 1

@pytest.mark.asyncio
async def test_is_prime_large_process():
    divisor: int = await mathgames.is_prime(100000000000000003)
    assert divisor == 1



@pytest.mark.asyncio
async def test_steps_3xmas1_simple():
    """Test the steps_3xmas1 function for simple Collatz sequences."""
    # Testing based on the current structure:
    expected: list[int] = [21, 64, 32, 16, 8, 4]
    response: list[int] = await mathgames.steps_3xmas1(21)
    assert expected == response

@pytest.mark.asyncio
async def test_steps_3xmas1_edge_case():
    """Test edge cases for steps_3xmas1."""
    # Test a number that might immediately trigger exit or loop incorrectly.
    response: list[int] = await mathgames.steps_3xmas1(47)
    assert 103 == len(response)

