

from typing import Optional

from pydantic import BaseModel

from mcps1.domain.question.question import Question
from mcps1.domain.resources.recorded_tool import RecordedTool


class Awnser(BaseModel):
    awnser_id: int = None
    response: str
    question_id: int
    tool_used: Optional[RecordedTool] = None
    model_used: Optional[str] = None
    cost_tokens: Optional[str] = None