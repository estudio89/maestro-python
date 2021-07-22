from typing import Any, Union
from maestro.core.utils import BaseSyncLock
from maestro.core.events import EventsManager
from maestro.core.provider import BaseSyncProvider
from maestro.core.store import BaseDataStore
from maestro.core.execution import ChangesExecutor
from maestro.core.metadata import ItemChange, ItemVersion, ConflictLog
import json

class TestDataStoreMixin:
    __test__ = False

    def _create_item(
        self, id: "str", name: "str", version: "str"
    ) -> "Any":  # pragma: no cover
        raise NotImplementedError()

    def _get_id(self, item: "Any") -> "str":  # pragma: no cover
        raise NotImplementedError()

    def _add_item(self, item: "Any"):  # pragma: no cover
        raise NotImplementedError()

    def _add_item_change(self, item_change: "ItemChange"):  # pragma: no cover
        raise NotImplementedError()

    def _add_item_version(self, item_version: "ItemVersion"):  # pragma: no cover
        raise NotImplementedError()

    def _add_conflict_log(self, conflict_log: "ConflictLog"):  # pragma: no cover
        raise NotImplementedError()

class BackendTestMixin:
    def to_dict(self, value: "Any"):
        if isinstance(value, dict):
            return value
        return json.loads(value)

    def _create_data_store(
        self, local_provider_id: "str",
    ) -> "BaseDataStore":  # pragma: no cover
        raise NotImplementedError()

    def _create_provider(
        self,
        provider_id: "str",
        data_store: "BaseDataStore",
        events_manager: "EventsManager",
        changes_executor: "ChangesExecutor",
        max_num: "int",
    ) -> "Union[BaseSyncProvider, TestDataStoreMixin]":  # pragma: no cover
        raise NotImplementedError()

    def _serialize_item(
        self, id: "str", name: "str", version: "str"
    ) -> "str":
        return '{"name": "%s", "version": "%s"}' % (name, version)

    def _create_sync_lock(self) -> "BaseSyncLock":  # pragma: no cover
        raise NotImplementedError()
