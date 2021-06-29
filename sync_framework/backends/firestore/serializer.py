from sync_framework.core.utils import parse_datetime
from sync_framework.core.serializer import BaseItemSerializer
import json
from typing import Dict, Any
from sync_framework.backends.firestore.utils import (
    collection_to_table_name,
    table_name_to_collection,
)
import datetime as dt


class FirestoreItemSerializer(BaseItemSerializer):
    def serialize_item(self, item: "Dict[str, Any]") -> "str":
        pk = item["id"]
        collection_name = item["collection_name"]

        fields = {}
        for key in item:
            if key in ["id", "collection_name"]:
                continue

            value = item[key]
            if isinstance(value, dt.datetime) or isinstance(value, dt.date):
                value = value.isoformat()

            fields[key] = value

        table_name = collection_to_table_name(collection=collection_name)
        serialized_data = {"table_name": table_name, "pk": pk, "fields": fields}
        serialized_data = dict(sorted(serialized_data.items()))
        serialized_item = json.dumps(serialized_data)
        return serialized_item

    def deserialize_item(self, serialized_item: "str") -> "Dict[str, Any]":
        raw_data = json.loads(serialized_item)
        table_name = raw_data.pop("table_name")
        collection_name = table_name_to_collection(table_name=table_name)
        pk = raw_data.pop("pk")
        fields = raw_data.pop("fields")
        item = {"id": pk, "collection_name": collection_name}

        for key in fields:
            item[key] = fields[key]
            # checking for date
            try:
                date = dt.date.fromisoformat(item[key])
                item[key] = date
            except (ValueError, TypeError):
                pass

            # checking for datetime
            try:
                date = parse_datetime(item[key])
                item[key] = date
            except (ValueError, TypeError):
                pass
        return item
