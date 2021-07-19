from .collections import CollectionType
from typing import Callable, Dict, Union, List
from maestro.core.query import Filter, Comparison, Comparator, Connector
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

def _check_filter(child: "Union[Filter, Comparison]", item: "Dict") -> "bool":
    if isinstance(child, Comparison):
        if child.comparator == Comparator.EQUALS:
            return item[child.field_name] == child.value
        elif child.comparator == Comparator.NOT_EQUALS:
            return item[child.field_name] != child.value
        elif child.comparator == Comparator.LESS_THAN:
            return item[child.field_name] < child.value
        elif child.comparator == Comparator.LESS_THAN_OR_EQUALS:
            return item[child.field_name] <= child.value
        elif child.comparator == Comparator.GREATER_THAN:
            return item[child.field_name] > child.value
        elif child.comparator == Comparator.GREATER_THAN_OR_EQUALS:
            return item[child.field_name] >= child.value
        elif child.comparator == Comparator.IN:
            return item[child.field_name] in child.value
        else:
            raise ValueError("Unexpected comparator: %s" % (child.comparator))
    else:
        check_func = all if child.connector == Connector.AND else any
        results: "List[bool]" = []
        for child in child.children:
            res = _check_filter(child=child, item=item)
            results.append(res)
        return check_func(results)


def query_filter_to_lambda(filter: "Filter") -> "Callable[[Dict], bool]":
    return lambda item: _check_filter(child=filter, item=item)
