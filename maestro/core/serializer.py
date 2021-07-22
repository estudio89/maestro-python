from typing import Any, Dict, TYPE_CHECKING
from abc import ABC, abstractmethod
from collections.abc import Iterable

import json
import datetime as dt
import copy
import uuid
import enum

if TYPE_CHECKING:
    from maestro.core.store import BaseDataStore
    from maestro.core.metadata import SerializationResult

class BaseItemSerializer(ABC):

    """Abstract class that serializes items to string and back."""

    @abstractmethod
    def serialize_item(self, item: "Any") -> "SerializationResult":
        """Serializes the item, converting it to a string.

        Args:
            item (Any): Item to be serialized.
        """

    @abstractmethod
    def deserialize_item(self, serialization_result: "SerializationResult") -> "Any":
        """Converts a serialized string to an item.

        Args:
            serialization_result (SerializationResult): The result of the serialization
        """


class BaseMetadataSerializer(ABC):

    """Abstract class that serializes metadata objects to dictionaries of primitive types and back.
    """

    @abstractmethod
    def serialize(self, metadata_object: "Any") -> "Dict":
        """Serializes a metadata object to a dictionary of primitive types.

        Args:
            metadata_object (Any): The object to be serialized.
        """


class MetadataSerializer(BaseMetadataSerializer):

    """Concrete implementation of a BaseMetadataSerializer.
    """

    def _serialize_field(self, value: "Any") -> "Any":
        """Converts a field to a primitive type.

        Args:
            value (Any): The value to be serialized.

        Returns:
            Any: Primitive type.
        """
        serialized: "Any"

        if isinstance(value, dict):
            serialized = {}
            for attr in value:
                result = self._serialize_field(value[attr])
                serialized[attr] = result
            return serialized
        elif value is None:
            return None
        elif isinstance(value, bool):
            return value
        elif isinstance(value, dt.datetime):
            return value.isoformat()
        elif isinstance(value, str):
            return value
        elif isinstance(value, float) or isinstance(value, int):
            return value
        elif isinstance(value, uuid.UUID):
            return str(value)
        elif isinstance(value, Iterable):
            serialized = []
            for sub_value in value:
                result = self._serialize_field(sub_value)
                serialized.append(result)
            return serialized
        elif isinstance(value, enum.Enum):
            return str(value)
        else:
            if hasattr(value, "__dict__"):
                serialized = self._serialize_field(value.__dict__)
                return serialized
            else:
                return str(value)

    def serialize(self, metadata_object: "Any") -> "Dict":
        """Converts the metadata object to a dictionary of primitive types.

        Args:
            metadata_object (Any): Metadata object to be serialized.

        Returns:
            Dict: A dictionary of primitive types.
        """
        return self._serialize_field(metadata_object)


class RawDataStoreJSONSerializer:

    """Serializes the contents of a data store to JSON string. This class is only ever used in tests,
    it shouldn't be used in production as it would read ALL the data from the data store.

    Attributes:
        metadata_serializer (BaseMetadataSerializer): The serializer that should be used for converting the metadata objects in the data store to primitive dictionaries.
        indent (int): Indentation to be used when the JSON string is generated.
    """

    metadata_serializer: "BaseMetadataSerializer"

    def __init__(self, metadata_serializer: "BaseMetadataSerializer", indent: "int"):
        """
        Args:
            metadata_serializer (BaseMetadataSerializer): The serializer that should be used for converting the metadata objects in the data store to primitive dictionaries.
            indent (int): Indentation to be used when the JSON string is generated.
        """
        self.metadata_serializer = metadata_serializer
        self.indent = indent

    def serialize(self, data_store: "BaseDataStore") -> "str":
        """Converts the contents of the data store to a JSON string. This is used only for testing, DO NOT use in production.

        Args:
            data_store (BaseDataStore): The data store being serialized.

        Returns:
            str: JSON string.
        """
        raw_db = copy.deepcopy(data_store._get_raw_db())
        items = raw_db.pop("items")

        serialized_db = self.metadata_serializer.serialize(metadata_object=raw_db)
        serialized_items = [
            data_store.item_serializer.serialize_item(item=item) for item in items
        ]
        serialized_db["items"] = serialized_items
        return json.dumps(serialized_db, indent=self.indent)
