import tests.base
from maestro.core.store import BaseDataStore
from maestro.core.utils import BaseSyncLock
from maestro.core.events import EventsManager
from maestro.core.provider import BaseSyncProvider
from maestro.core.execution import ChangesExecutor
from maestro.core.metadata import (
    SyncSession,
    ItemChange,
    ItemVersion,
    ConflictLog,
    VectorClock,
)
from maestro.backends.in_memory import (
    InMemoryDataStore,
    InMemorySyncProvider,
    InMemorySyncLock,
    JSONSerializer,
    NullConverter,
)
from typing import Any

class TestInMemoryDataStore(InMemoryDataStore, tests.base.TestDataStoreMixin):
    def _create_item(
        self, id: "str", name: "str", version: "str"
    ) -> "Any":  # pragma: no cover
        return {
            "id": str(id),
            "name": name,
            "version": version,
            "entity_name": "my_app_item",
        }

    def _get_id(self, item: "Any") -> "str":  # pragma: no cover
        return item["id"]

    def _add_item(self, item: "Any"):
        if "my_app_item" not in self._db["items"]:
            self._db["items"]["my_app_item"] = []

        self._db["items"]["my_app_item"].append(item)

    def _add_item_change(self, item_change: "ItemChange"):
        self._db["item_changes"].append(item_change.__dict__)

    def _add_item_version(self, item_version: "ItemVersion"):
        self._db["item_versions"].append(item_version.__dict__)

    def _add_conflict_log(self, conflict_log: "ConflictLog"):
        self._db["conflict_logs"].append(conflict_log.__dict__)

class InMemoryBackendTestMixin(tests.base.BackendTestMixin):
    data_store: "TestInMemoryDataStore"

    def _create_data_store(self, local_provider_id: "str",) -> "BaseDataStore":
        return TestInMemoryDataStore(
            local_provider_id=local_provider_id,
            sync_session_metadata_converter=NullConverter(metadata_class=SyncSession),
            item_version_metadata_converter=NullConverter(metadata_class=ItemVersion),
            item_change_metadata_converter=NullConverter(metadata_class=ItemChange),
            conflict_log_metadata_converter=NullConverter(metadata_class=ConflictLog),
            vector_clock_metadata_converter=NullConverter(metadata_class=VectorClock),
            item_serializer=JSONSerializer(),
        )

    def _create_provider(
        self,
        provider_id: "str",
        data_store: "BaseDataStore",
        events_manager: "EventsManager",
        changes_executor: "ChangesExecutor",
        max_num: "int",
    ) -> "BaseSyncProvider":
        return InMemorySyncProvider(
            provider_id=provider_id,
            data_store=data_store,
            events_manager=events_manager,
            changes_executor=changes_executor,
            max_num=max_num,
        )

    def _create_sync_lock(self) -> "BaseSyncLock":
        return InMemorySyncLock()

