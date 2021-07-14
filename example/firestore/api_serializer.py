from maestro.core.utils import parse_datetime
from example.api_serializer import APISerializer
from typing import Any, Dict


class FirestoreAPISerializer(APISerializer):
    def to_dict(self, item: "Any") -> "Dict":
        return {
            "id": item["id"],
            "text": item["text"],
            "done": item["done"],
            "date_created": item["date_created"].isoformat(),
        }

    def from_dict(self, data: "Dict") -> "Any":
        return {
            "id": data["id"],
            "text": data["text"],
            "done": data["done"],
            "date_created": parse_datetime(value=data["date_created"]),
            "collection_name": "todos_todo"
        }
