from sync_framework.core.serializer import BaseItemSerializer
from typing import Dict
import json

class JSONSerializer(BaseItemSerializer):

    def serialize_item(self, item: "Dict") -> "str":
        return json.dumps(item)

    def deserialize_item(self, serialized_item: "str") -> "Dict":
        return json.loads(serialized_item)
