from functools import reduce
import logging
import os

from fast_depends import inject
from mcp.server.fastmcp import FastMCP
from mcps1.application.resources.adapters.resources_adaptator import ResourcePortOutDependency
from mcps1.application.resources.ports.resource_port_out import ResourcePortOut
from mcps1.application.tools.mathgames import MathGames
from mcps1.infraestructure.mcp.runtime import McpServerRuntime
from mcps1.infraestructure.messaging.adapters.messaging_adaptator import KafkaConsumerUpdateDependency
from mcps1.infraestructure.messaging.ports.kafka_port_in import KafkaPortIn

name = "mcps1"
# StreamHandler writes to stderr: stdout is reserved for the MCP stdio transport
logging.basicConfig(
    level=logging.INFO,
    format='%(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(name)

port = int(os.environ.get('PORT', 8000))

math_worker: MathGames = MathGames()


@inject
def get_resource_reader(resource_worker: ResourcePortOutDependency) -> ResourcePortOut:
    return resource_worker


@inject
def get_update_consumer(update_consumer: KafkaConsumerUpdateDependency) -> KafkaPortIn:
    return update_consumer


# Dependencies are resolved lazily (on first use / when the lifespan starts), not at import time
runtime = McpServerRuntime(resource_reader_factory=get_resource_reader,
                           update_consumer_factory=get_update_consumer)

mcp = FastMCP(name=name, host="0.0.0.0", port=port, json_response=True, lifespan=runtime.lifespan)

@mcp.tool()
def hello(origin: str) -> str:
    """ tool for check connection"""
    return f"Hello, you was ping to the mcp server from {origin}"

@mcp.tool()
async def prime(number: int) -> int:
    """check if a possitive integer number is prime. if return 1 is prime, else, return the minimal divisor for the number"""
    return await math_worker.is_prime(number=number)

@mcp.tool()
async def collatz(number: int) -> str:
    """using the Collatz congeture (aka: 3x+1 problem), evaluate an integer positive number and return the list of all steps before to the infinite loop of 4,2,1"""
    logger.info(f"Processing collatz: {int(number)}")
    pile_steps: list[int] = await math_worker.steps_3xmas1(number=number)
    response: str = reduce(lambda acc, x: acc + ", " + str(x), pile_steps[1:], str(pile_steps[0]))
    return response

@mcp.resource(
        uri="data://{type_data}/{value}",
        name="DataDictionaiesAcces",
        description="Provide a json with the data dictionary about the diferent type os uses for it",
        mime_type="application/json")
async def get_data_dictionary(type_data: str, value: str) -> str:
    """about de type of data ["MESSAGE", "DATABASE", "REST", "MODEL"] and the value of data (ie: question, response, etc), return a dictionary with the indications about the structure, functional use a type of datas that must be used in the project"""
    logger.info(f"Processing data dictionary: {type_data}/{value}")
    return await runtime.resource_reader.get_data_dictionary(type_data, value)
