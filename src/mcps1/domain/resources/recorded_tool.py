

from pydantic import BaseModel


class RecordedTool(BaseModel):
    tool_id: int
    name_tool: str
    description_tool: str
    input_params: str
    response_type: str