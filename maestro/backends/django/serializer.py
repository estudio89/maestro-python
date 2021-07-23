from maestro.core.serializer import BaseItemSerializer
from maestro.core.metadata import SerializationResult
from django.db import models
from django.core import serializers
import json
from maestro.backends.django.utils import (
    app_model_to_entity_name,
    entity_name_to_app_model,
)


class DjangoItemSerializer(BaseItemSerializer):
    def serialize_item(self, item: "models.Model") -> "SerializationResult":
        serialized_item = serializers.serialize("json", [item])
        serialized_item = json.loads(serialized_item)
        serialized_item = serialized_item[0]
        app_model = serialized_item.pop("model")
        entity_name = app_model_to_entity_name(app_model)
        fields = serialized_item.pop("fields")
        pk = serialized_item.pop("pk")
        fields["id"] = pk
        serialized_item = dict(sorted(fields.items()))
        serialized_item_str = json.dumps(serialized_item)
        result = SerializationResult(
            item_id=pk, entity_name=entity_name, serialized_item=serialized_item_str
        )
        return result

    def deserialize_item(
        self, serialization_result: "SerializationResult"
    ) -> "models.Model":
        raw_data = {
            "model": entity_name_to_app_model(
                entity_name=serialization_result.entity_name
            ),
            "fields": json.loads(serialization_result.serialized_item),
            "pk": serialization_result.item_id,
        }
        raw_data_list = [raw_data]
        serialized_data = json.dumps(raw_data_list)
        result_list = list(
            serializers.deserialize("json", serialized_data, ignorenonexistent=True)
        )
        result = result_list[0]
        res_object = result.object
        res_object.m2m_data = result.m2m_data
        return res_object
