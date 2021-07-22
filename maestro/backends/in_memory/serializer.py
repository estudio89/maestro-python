from maestro.core.serializer import BaseItemSerializer
from maestro.core.metadata import SerializationResult
from typing import Dict
import json


class JSONSerializer(BaseItemSerializer):
    def serialize_item(self, item: "Dict") -> "SerializationResult":
        result = SerializationResult(
            item_id=item.pop("id"),
            entity_name=item.pop("entity_name"),
            serialized_item=json.dumps(item),
        )
        return result

    def deserialize_item(self, serialization_result: "SerializationResult") -> "Dict":
        item = json.loads(serialization_result.serialized_item)
        item["id"] = serialization_result.item_id
        item["entity_name"] = serialization_result.entity_name
        return item
