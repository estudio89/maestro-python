from example.api_serializer import APISerializer
from typing import Any, Dict

class InMemoryAPISerializer(APISerializer):
	def to_dict(self, item: "Any") -> "Dict":
		return item

	def from_dict(self, data: "Dict") -> "Any":
		data["entity_name"] = "todos_todo"
		return data
