from sync_framework.core.serializer import BaseItemSerializer
from django.db import models
from django.core import serializers
import json
from sync_framework.backends.django.utils import (
    app_model_to_table_name,
    table_name_to_app_model,
)


class DjangoItemSerializer(BaseItemSerializer):
    def serialize_item(self, item: "models.Model") -> "str":
        serialized_item = serializers.serialize("json", [item])
        serialized_item = json.loads(serialized_item)
        serialized_item = serialized_item[0]
        serialized_item["table_name"] = serialized_item.pop("model")
        serialized_item["table_name"] = app_model_to_table_name(
            serialized_item["table_name"]
        )
        serialized_item = dict(sorted(serialized_item.items()))
        serialized_item_str = json.dumps(serialized_item)
        return serialized_item_str

    def deserialize_item(self, serialized_item: "str") -> "models.Model":
        raw_data = json.loads(serialized_item)
        raw_data["model"] = raw_data.pop("table_name")
        raw_data["model"] = table_name_to_app_model(table_name=raw_data["model"])
        raw_data = [raw_data]
        serialized_data = json.dumps(raw_data)
        result_list = list(serializers.deserialize("json", serialized_data))
        result = result_list[0]
        return result.object
