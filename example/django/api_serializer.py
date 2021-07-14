from example.api_serializer import APISerializer
from maestro.core.utils import parse_datetime
import uuid
from typing import Any, Dict
from django.apps import apps

class DjangoAPISerializer(APISerializer):
    def to_dict(self, item: "Any") -> "Dict":
        return {
            "id": str(item.id),
            "text": item.text,
            "done": item.done,
            "date_created": item.date_created.isoformat(),
        }

    def from_dict(self, data: "Dict") -> "Any":
        Todo = apps.get_model('todos','Todo')
        return Todo(
            id=uuid.UUID(data["id"]),
            text=data["text"],
            done=data["done"],
            date_created=parse_datetime(data["date_created"]),
        )
