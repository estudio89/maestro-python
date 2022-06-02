from pydantic import BaseModel
import uuid


class TodoItem(BaseModel):
    id: uuid.UUID
    text: str = ""
    done: bool = False
