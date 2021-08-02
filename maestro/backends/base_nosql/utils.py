from .collections import CollectionType


def entity_name_to_collection(entity_name: "str") -> "str":
    return entity_name


def collection_to_entity_name(collection: "str") -> "str":
    return collection

def type_to_collection(key: "CollectionType"):
    return f"maestro__{key.value}"
