

import math
import asyncio

from mcps1.domain.question.question import Question
from mcps1.domain.response.awnser import Awnser

class MathGames:
    
    async def is_prime(self, number: int) -> int:
        upper_candidate: int = int(math.sqrt(number))
        current_candidate: int = 2
        if number < 4:
            return 1

        while current_candidate <= upper_candidate:
            if number % current_candidate == 0:
                return current_candidate
            current_candidate += 1
        return 1


    async def steps_3xmas1(self, number: int) -> list[int]:
        current_num: int = number
        pile_steps: list[int] = [number]
        while current_num != 4:
            current_num = int(current_num / 2) if current_num % 2 == 0 else (current_num * 3 + 1)
            pile_steps.append(current_num)
        return pile_steps
