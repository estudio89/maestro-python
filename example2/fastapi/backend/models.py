from pydantic import BaseModel
import uuid
import datetime as dt


class TodoItem(BaseModel):
    id: uuid.UUID
    text: str = ""
    done: bool = False
    date: dt.datetime
