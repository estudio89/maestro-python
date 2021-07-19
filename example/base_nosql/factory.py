from maestro.core.serializer import BaseItemSerializer
from maestro.core.utils import parse_datetime
from example.base_nosql.collections import TodoRecord
import json

class NoSQLExampleSerializer(BaseItemSerializer):
    def serialize_item(self, item: "TodoRecord") -> "str":

        serialized = {
            "fields": {
                "date_created": item["date_created"].isoformat(),
                "done": item["done"],
                "text": item["text"],
            },
            "pk": item["id"],
            "entity_name": "todos_todo",
        }

        serialized = dict(sorted(serialized.items()))
        return json.dumps(serialized)

    def deserialize_item(self, serialized_item: "str") -> "TodoRecord":
        data = json.loads(serialized_item)
        deserialized = {
            "id": data["pk"],
            "text": data["fields"]["text"],
            "done": data["fields"]["done"],
            "date_created": parse_datetime(value=data["fields"]["date_created"]),
            "collection_name": "todos_todo",
        }

        return deserialized