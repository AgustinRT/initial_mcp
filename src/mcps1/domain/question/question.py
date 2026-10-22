
from pydantic import BaseModel


class Question(BaseModel):
    question_id: int | None
    prompt: str