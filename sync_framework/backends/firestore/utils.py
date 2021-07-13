from .collections import CollectionType
import json


def table_name_to_collection(table_name: "str") -> "str":
    return table_name


def collection_to_table_name(collection: "str") -> "str":
    return collection


def get_collection_name(serialized_item: "str") -> "str":
    data = json.loads(serialized_item)
    table_name = data["table_name"]
    collection = table_name_to_collection(table_name)
    return collection


def type_to_collection(key: "CollectionType"):
    return f"sync_framework__{key.value}"
