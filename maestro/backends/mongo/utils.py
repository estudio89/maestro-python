from maestro.core.query.metadata import (
    Filter,
    Comparison,
    Comparator,
    Connector,
    SortOrder,
)
import pymongo
from typing import Dict, cast, List


def _comparison_to_mongo_expression(
    comparison: "Comparison", field_prefix: "str"
) -> "Dict":
    if comparison.comparator == Comparator.EQUALS:
        mongo_operator = "$eq"
    elif comparison.comparator == Comparator.NOT_EQUALS:
        mongo_operator = "$ne"
    elif comparison.comparator == Comparator.LESS_THAN:
        mongo_operator = "$lt"
    elif comparison.comparator == Comparator.LESS_THAN_OR_EQUALS:
        mongo_operator = "$lte"
    elif comparison.comparator == Comparator.GREATER_THAN:
        mongo_operator = "$gt"
    elif comparison.comparator == Comparator.GREATER_THAN_OR_EQUALS:
        mongo_operator = "$gte"
    elif comparison.comparator == Comparator.IN:
        mongo_operator = "$in"
    else:
        raise ValueError(f"Unknown comparator: {comparison.comparator}")

    return {field_prefix + comparison.field_name: {mongo_operator: comparison.value}}


def convert_to_mongo_filter(filter: "Filter", field_prefix: "str") -> "Dict":
    mongo_filter = {}
    if len(filter.children) == 1 and isinstance(filter.children[0], Comparison):
        comparison: "Comparison" = cast("Comparison", filter.children[0])
        child_mongo_filter = _comparison_to_mongo_expression(
            comparison=comparison, field_prefix=field_prefix
        )
        mongo_filter.update(child_mongo_filter)
    else:
        child_expressions: "List[Dict]" = []
        for child in filter.children:
            if isinstance(child, Filter):
                child_expression = convert_to_mongo_filter(
                    filter=child, field_prefix=field_prefix
                )
            else:
                child_expression = _comparison_to_mongo_expression(
                    comparison=child, field_prefix=field_prefix
                )

            child_expressions.append(child_expression)

        if filter.connector == Connector.AND:
            mongo_operator = "$and"
        else:
            mongo_operator = "$or"

        mongo_filter[mongo_operator] = child_expressions

    return mongo_filter


def convert_to_mongo_sort(ordering: "List[SortOrder]", field_prefix: "str") -> "Dict[str,int]":
    mongo_sort: "Dict[str,int]" = {}
    for sort_order in ordering:
        mongo_sort[field_prefix + sort_order.field_name] = (
            pymongo.DESCENDING if sort_order.descending else pymongo.ASCENDING
        )

    return mongo_sort
