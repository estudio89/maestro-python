from maestro.core.utils import parse_datetime
from maestro.core.serializer import BaseItemSerializer
from maestro.core.metadata import SerializationResult
import json
from typing import Dict, Any, List
from maestro.backends.base_nosql.utils import (
    entity_name_to_collection,
)
import datetime as dt


class NoSQLItemSerializer(BaseItemSerializer):
    def get_skip_fields(self) -> "List[str]":
        return ["collection_name"]

    def serialize_field_value(
        self, collection_name: "str", item: "Dict[str, Any]", key="str"
    ) -> "Any":
        value = item[key]
        if isinstance(value, dt.datetime) or isinstance(value, dt.date):
            value = value.isoformat()
        return value

    def serialize_item(self, item: "Dict[str, Any]", entity_name: "str") -> "SerializationResult":
        pk = item["id"]
        collection_name = entity_name_to_collection(entity_name)

        fields = {}
        for key in item:
            if key in self.get_skip_fields():
                continue

            value = self.serialize_field_value(
                collection_name=collection_name, item=item, key=key
            )
            fields[key] = value

        sorted_fields = dict(sorted(fields.items()))
        serialized_item = json.dumps(sorted_fields)
        result = SerializationResult(
            item_id=pk, entity_name=entity_name, serialized_item=serialized_item
        )
        return result

    def deserialize_field_value(
        self, collection_name: "str", fields: "Dict[str, Any]", key: "str"
    ) -> "Any":

        value = fields[key]
        if value is None:
            return None

        try:
            date = dt.date.fromisoformat(value)
            return date
        except (ValueError, TypeError):
            pass

        # checking for datetime
        try:
            date = parse_datetime(value)
            return date
        except (ValueError, TypeError):
            pass

        return value

    def deserialize_item(
        self, serialization_result: "SerializationResult"
    ) -> "Dict[str, Any]":
        entity_name = serialization_result.entity_name
        collection_name = entity_name_to_collection(entity_name=entity_name)
        pk = serialization_result.item_id

        fields = json.loads(serialization_result.serialized_item)
        item = {"id": pk, "collection_name": collection_name}

        for key in fields:
            value = self.deserialize_field_value(
                collection_name=collection_name, fields=fields, key=key
            )
            item[key] = value

        return item
