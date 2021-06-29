from typing import Any
from sync_framework.core.utils import BaseSyncLock
from sync_framework.core.events import EventsManager
from sync_framework.core.provider import BaseSyncProvider
from sync_framework.core.store import BaseDataStore
from sync_framework.core.execution import ChangesExecutor
from sync_framework.core.metadata import ItemChange, ItemVersion, ConflictLog


class BackendTestMixin:

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
    ) -> "BaseSyncProvider":  # pragma: no cover
        raise NotImplementedError()

    def _create_item(
        self, id: "str", name: "str", version: "str"
    ) -> "Any":  # pragma: no cover
        raise NotImplementedError()

    def _serialize_item(
        self, id: "str", name: "str", version: "str"
    ) -> "str":  # pragma: no cover
        raise NotImplementedError()

    def _deserialize_item(
        self, id: "str", name: "str", version: "str"
    ) -> "Any":  # pragma: no cover
        raise NotImplementedError()

    def _get_id(self, item: "Any") -> "str":  # pragma: no cover
        raise NotImplementedError()

    def _create_sync_lock(self) -> "BaseSyncLock":  # pragma: no cover
        raise NotImplementedError()

    def _add_item_change(self, item_change: "ItemChange"):  # pragma: no cover
        raise NotImplementedError()

    def _add_item_version(self, item_version: "ItemVersion"):  # pragma: no cover
        raise NotImplementedError()

    def _add_conflict_log(self, conflict_log: "ConflictLog"):  # pragma: no cover
        raise NotImplementedError()

    def _add_item(self, item: "Any"):  # pragma: no cover
        raise NotImplementedError()
