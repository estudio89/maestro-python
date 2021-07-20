from .collections import CollectionType
import json


def entity_name_to_collection(entity_name: "str") -> "str":
    return entity_name


def collection_to_entity_name(collection: "str") -> "str":
    return collection


def get_collection_name(serialized_item: "str") -> "str":
    data = json.loads(serialized_item)
    entity_name = data["entity_name"]
    collection = entity_name_to_collection(entity_name)
    return collection


def type_to_collection(key: "CollectionType"):
    return f"maestro__{key.value}"
