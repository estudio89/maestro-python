from maestro.core.serializer import BaseItemSerializer
from django.db import models
from django.core import serializers
import json
from maestro.backends.django.utils import (
    app_model_to_entity_name,
    entity_name_to_app_model,
)


class DjangoItemSerializer(BaseItemSerializer):
    def serialize_item(self, item: "models.Model") -> "str":
        serialized_item = serializers.serialize("json", [item])
        serialized_item = json.loads(serialized_item)
        serialized_item = serialized_item[0]
        serialized_item["entity_name"] = serialized_item.pop("model")
        serialized_item["entity_name"] = app_model_to_entity_name(
            serialized_item["entity_name"]
        )
        serialized_item = dict(sorted(serialized_item.items()))
        serialized_item_str = json.dumps(serialized_item)
        return serialized_item_str

    def deserialize_item(self, serialized_item: "str") -> "models.Model":
        raw_data = json.loads(serialized_item)
        raw_data["model"] = raw_data.pop("entity_name")
        raw_data["model"] = entity_name_to_app_model(entity_name=raw_data["model"])
        raw_data = [raw_data]
        serialized_data = json.dumps(raw_data)
        result_list = list(
            serializers.deserialize("json", serialized_data, ignorenonexistent=True)
        )
        result = result_list[0]
        res_object = result.object
        res_object.m2m_data = result.m2m_data
        return res_object
