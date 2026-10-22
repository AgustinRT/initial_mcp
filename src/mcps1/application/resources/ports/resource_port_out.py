from typing import Protocol, runtime_checkable

@runtime_checkable
class ResourcePortOut(Protocol):
    """
    Interface defining incoming ports for the MCP Server.
    Must implement methods for retrieving data dictionaries, 
    contextual values, and assignment communications.
    """
    async def get_data_dictionary(self, data_dict: str) -> dict:
        """Retrieves a data dictionary using a string identifier."""
        ...

    def get_contextual_values(self, context: str) -> any:
        """Extracts contextual values from the provided data stream."""
        ...

    def get_assignation_communications(self, route: str) -> any:
        """Retrieves assignment communications from the provided data stream."""
        ...