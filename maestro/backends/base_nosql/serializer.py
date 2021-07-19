from maestro.core.utils import parse_datetime
from maestro.core.serializer import BaseItemSerializer
import json
from typing import Dict, Any, List
from maestro.backends.base_nosql.utils import (
    collection_to_entity_name,
    entity_name_to_collection,
)
import datetime as dt


class NoSQLItemSerializer(BaseItemSerializer):

    def get_skip_fields(self) -> "List[str]":
        return ["id", "collection_name"]

    def serialize_field_value(
        self, collection_name: "str", item: "Dict[str, Any]", key="str"
    ) -> "Any":
        value = item[key]
        if isinstance(value, dt.datetime) or isinstance(value, dt.date):
            value = value.isoformat()
        return value

    def serialize_item(self, item: "Dict[str, Any]") -> "str":
        pk = item["id"]
        collection_name = item["collection_name"]

        fields = {}
        for key in item:
            if key in self.get_skip_fields():
                continue

            value = self.serialize_field_value(
                collection_name=collection_name, item=item, key=key
            )
            fields[key] = value

        entity_name = collection_to_entity_name(collection=collection_name)
        sorted_fields = dict(sorted(fields.items()))
        serialized_data = {"entity_name": entity_name, "pk": pk, "fields": sorted_fields}
        serialized_data = dict(sorted(serialized_data.items()))
        serialized_item = json.dumps(serialized_data)
        return serialized_item

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

    def deserialize_item(self, serialized_item: "str") -> "Dict[str, Any]":
        raw_data = json.loads(serialized_item)
        entity_name = raw_data.pop("entity_name")
        collection_name = entity_name_to_collection(entity_name=entity_name)
        pk = raw_data.pop("pk")
        fields = raw_data.pop("fields")
        item = {"id": pk, "collection_name": collection_name}

        for key in fields:
            value = self.deserialize_field_value(
                collection_name=collection_name, fields=fields, key=key
            )
            item[key] = value

        return item
