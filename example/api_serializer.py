from abc import ABC, abstractmethod
from typing import Any, Dict

class APISerializer(ABC):

	@abstractmethod
	def to_dict(self, item: "Any") -> "Dict":
		raise NotImplementedError()

	@abstractmethod
	def from_dict(self, data: "Dict") -> "Any":
		raise NotImplementedError()