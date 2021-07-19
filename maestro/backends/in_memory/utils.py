from typing import ContextManager, Callable, Dict, Union, List
from maestro.core.utils import BaseSyncLock
from maestro.core.query import Filter, Comparison, Comparator, Connector


class InMemoryContextManager:
    lock: "InMemorySyncLock"

    def __init__(self, lock: "InMemorySyncLock"):
        self.lock = lock

    def __enter__(self):
        self.lock._running = True

    def __exit__(self, type, value, traceback):
        self.lock._running = False


class InMemorySyncLock(BaseSyncLock):
    def __init__(self):
        self._running = False

    def is_running(self) -> "bool":
        return self._running

    def lock(self) -> "ContextManager":
        return InMemoryContextManager(lock=self)


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
