from maestro.core.serializer import BaseItemSerializer
from maestro.core.metadata import SerializationResult
from maestro.core.utils import parse_datetime
from maestro.backends.base_nosql.utils import entity_name_to_collection
from example.base_nosql.collections import TodoRecord
import json


class NoSQLExampleSerializer(BaseItemSerializer):
    def serialize_item(
        self, item: "TodoRecord", entity_name: "str"
    ) -> "SerializationResult":

        serialized = {
            "date_created": item["date_created"].isoformat(),
            "done": item["done"],
            "text": item["text"],
        }

        serialized_item = json.dumps(dict(sorted(serialized.items())))
        return SerializationResult(
            item_id=item["id"],
            entity_name=entity_name,
            serialized_item=serialized_item,
        )

    def deserialize_item(
        self, serialization_result: "SerializationResult"
    ) -> "TodoRecord":
        data = json.loads(serialization_result.serialized_item)
        collection_name = entity_name_to_collection(serialization_result.entity_name)
        deserialized = {
            "id": serialization_result.item_id,
            "text": data["text"],
            "done": data["done"],
            "date_created": parse_datetime(value=data["date_created"]),
            "collection_name": collection_name,
        }

        return deserialized
