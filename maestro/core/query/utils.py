from maestro.core.query.metadata import Filter, Comparison, Comparator, Connector
from typing import Union, Dict, Callable, List, Any


def _check_filter(
    child: "Union[Filter, Comparison]",
    item: "Dict",
    item_field_getter: "Callable[[Any, str], Any]",
) -> "bool":
    if isinstance(child, Comparison):
        if child.comparator == Comparator.EQUALS:
            return item_field_getter(item, child.field_name) == child.value
        elif child.comparator == Comparator.NOT_EQUALS:
            return item_field_getter(item, child.field_name) != child.value
        elif child.comparator == Comparator.LESS_THAN:
            return item_field_getter(item, child.field_name) < child.value
        elif child.comparator == Comparator.LESS_THAN_OR_EQUALS:
            return item_field_getter(item, child.field_name) <= child.value
        elif child.comparator == Comparator.GREATER_THAN:
            return item_field_getter(item, child.field_name) > child.value
        elif child.comparator == Comparator.GREATER_THAN_OR_EQUALS:
            return item_field_getter(item, child.field_name) >= child.value
        elif child.comparator == Comparator.IN:
            return item_field_getter(item, child.field_name) in child.value
        else:
            raise ValueError("Unexpected comparator: %s" % (child.comparator))
    else:
        check_func = all if child.connector == Connector.AND else any
        results: "List[bool]" = []
        for child in child.children:
            res = _check_filter(
                child=child, item=item, item_field_getter=item_field_getter
            )
            results.append(res)
        return check_func(results)


def query_filter_to_lambda(
    filter: "Filter", item_field_getter: "Callable[[Any, str], Any]"
) -> "Callable[[Any], bool]":
    """Converts a filter to a lambda function."""

    return lambda item: _check_filter(
        child=filter, item=item, item_field_getter=item_field_getter
    )
