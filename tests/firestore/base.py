from firebase_admin import firestore
from typing import Any, cast
from sync_framework.core.utils import BaseSyncLock
from sync_framework.core.events import EventsManager
from sync_framework.core.provider import BaseSyncProvider
from sync_framework.core.store import BaseDataStore
from sync_framework.core.execution import ChangesExecutor
from sync_framework.backends.firestore.collections import CollectionType
from sync_framework.backends.firestore.utils import type_to_collection
import unittest
import copy

from sync_framework.core.metadata import (
    SyncSession,
    ItemVersion,
    ItemChange,
    ConflictLog,
    VectorClock,
)
from sync_framework.core.exceptions import ItemNotFoundException
from sync_framework.backends.in_memory import (
    InMemoryDataStore,
    InMemorySyncProvider,
    InMemorySyncLock,
    NullConverter,
    JSONSerializer,
)

from sync_framework.backends.firestore import (
    FirestoreDataStore,
    SyncSessionMetadataConverter,
    ItemVersionMetadataConverter,
    ItemChangeMetadataConverter,
    ConflictLogMetadataConverter,
    VectorClockMetadataConverter,
    FirestoreItemSerializer,
    FirestoreSyncProvider,
)
import uuid
import tests.base


def _delete_data():
    db = firestore.client()
    for collection_type in CollectionType:
        collection = type_to_collection(collection_type)
        docs = db.collection(collection).get()
        for doc in docs:
            doc.reference.delete()

    docs = db.collection("test_items").get()
    for doc in docs:
        doc.reference.delete()


class FirestoreTestCase(unittest.TestCase):
    def tearDown(self):
        _delete_data()

    @classmethod
    def tearDownClass(cls):
        _delete_data()


class TestInMemoryDataStore(InMemoryDataStore):
    __test__ = False

    def _get_hashable_item(self, item: "Any"):
        """Retorna uma versão hashable de um item.

        Args:
            item (Any): Description

        Returns:
            TYPE: Description
        """
        return (item["id"], item["name"], item["version"])

    def update_item(self, item: "Any", serialized_item: "str") -> "Any":
        # print("update_item - serialized_item:", serialized_item)
        serializer = FirestoreItemSerializer()
        deserialized = serializer.deserialize_item(serialized_item=serialized_item)
        # print("update_item - deserialized:", deserialized)
        if item:
            item.update(deserialized)
            return item
        else:
            return deserialized

    def serialize_item(self, item):
        return (
            '{"pk": "%s", "fields": {"name": "%s", "version": "%s"}, "table_name": "test_items"}'
            % (str(item["id"]), item["name"], item["version"])
        )


class TestFirestoreDataStore(FirestoreDataStore):
    __test__ = False

    def _get_hashable_item(self, item: "Any"):
        """Retorna uma versão hashable de um item.

        Args:
            item (Any): Description

        Returns:
            TYPE: Description
        """
        return (item["id"], item["name"], item["version"])

    def get_items(self):
        docs = self.db.collection("test_items").get()
        items = []
        for doc in docs:
            record = self._document_to_raw_instance(doc, ignore_read=True)
            record["collection_name"] = "test_items"
            items.append(record)
        return items

    def get_item_by_id(self, id):
        doc = self.db.collection("test_items").document(str(id)).get()
        if not doc.exists:
            raise ItemNotFoundException(item_type="Item", id=id)

        record = self._document_to_raw_instance(doc, ignore_read=True)
        record["collection_name"] = "test_items"
        return record


class FirestoreBackendTestMixin(tests.base.BackendTestMixin):
    @property
    def db(self):
        if not hasattr(self, "_db"):
            self._db = firestore.client()
        return self._db

    def _get_id(self, item: "Any") -> "str":  # pragma: no cover
        return item["id"]

    def _create_sync_lock(self) -> "BaseSyncLock":  # pragma: no cover
        return InMemorySyncLock()

    def _create_item(self, id: "str", name: "str", version: "str"):
        if isinstance(id, uuid.UUID):
            id = str(id)
        return {
            "id": id,
            "name": name,
            "version": version,
            "collection_name": "test_items",
        }

    def _serialize_item(self, id: "str", name: "str", version: "str") -> "str":
        return (
            '{"fields": {"name": "%s", "version": "%s"}, "pk": "%s", "table_name": "test_items"}'
            % (name, version, str(id))
        )

    def _deserialize_item(self, id: "str", name: "str", version: "str") -> "Any":
        return self._create_item(id=id, name=name, version=version)

    def _create_data_store(self, local_provider_id: "str") -> "BaseDataStore":
        if local_provider_id == "other_provider":
            return TestInMemoryDataStore(
                local_provider_id=local_provider_id,
                sync_session_metadata_converter=NullConverter(
                    metadata_class=SyncSession
                ),
                item_version_metadata_converter=NullConverter(
                    metadata_class=ItemVersion
                ),
                item_change_metadata_converter=NullConverter(metadata_class=ItemChange),
                conflict_log_metadata_converter=NullConverter(
                    metadata_class=ConflictLog
                ),
                vector_clock_metadata_converter=NullConverter(
                    metadata_class=VectorClock
                ),
                item_serializer=JSONSerializer(),
            )
        else:
            sync_session_metadata_converter = SyncSessionMetadataConverter()
            item_version_metadata_converter = ItemVersionMetadataConverter()
            item_change_metadata_converter = ItemChangeMetadataConverter()
            conflict_log_metadata_converter = ConflictLogMetadataConverter()
            vector_clock_metadata_converter = VectorClockMetadataConverter()

            data_store = TestFirestoreDataStore(
                local_provider_id=local_provider_id,
                sync_session_metadata_converter=sync_session_metadata_converter,
                item_version_metadata_converter=item_version_metadata_converter,
                item_change_metadata_converter=item_change_metadata_converter,
                conflict_log_metadata_converter=conflict_log_metadata_converter,
                vector_clock_metadata_converter=vector_clock_metadata_converter,
                item_serializer=FirestoreItemSerializer(),
                db=self.db,
            )

            sync_session_metadata_converter.data_store = data_store
            item_version_metadata_converter.data_store = data_store
            conflict_log_metadata_converter.data_store = data_store

            return data_store

    def _create_provider(
        self,
        provider_id: "str",
        data_store: "BaseDataStore",
        events_manager: "EventsManager",
        changes_executor: "ChangesExecutor",
        max_num: "int",
    ) -> "BaseSyncProvider":
        if provider_id == "other_provider":
            return InMemorySyncProvider(
                provider_id=provider_id,
                data_store=data_store,
                events_manager=events_manager,
                changes_executor=changes_executor,
                max_num=max_num,
            )
        else:
            return FirestoreSyncProvider(
                provider_id=provider_id,
                data_store=data_store,
                events_manager=events_manager,
                changes_executor=changes_executor,
                max_num=max_num,
            )

    def _add_item_change(self, item_change: "ItemChange"):  # pragma: no cover
        vector_clock = []
        for vector_clock_item in item_change.vector_clock:
            vector_clock.append(
                {
                    "provider_id": vector_clock_item.provider_id,
                    "timestamp": vector_clock_item.timestamp,
                }
            )
        self.db.collection("sync_framework__item_changes").document(
            str(item_change.id)
        ).set(
            {
                "date_created": item_change.date_created,
                "operation": item_change.operation.value,
                "item_id": str(item_change.item_id),
                "collection_name": "my_app_item",
                "provider_timestamp": item_change.provider_timestamp,
                "provider_id": item_change.provider_id,
                "insert_provider_timestamp": item_change.insert_provider_timestamp,
                "insert_provider_id": item_change.insert_provider_id,
                "serialized_item": item_change.serialized_item,
                "should_ignore": item_change.should_ignore,
                "is_applied": item_change.is_applied,
                "vector_clock": vector_clock,
            }
        )

        self.db.collection("sync_framework__provider_ids").document(
            item_change.provider_id
        ).set({"timestamp": item_change.provider_timestamp})

    def _add_item_version(self, item_version: "ItemVersion"):  # pragma: no cover
        vector_clock = []
        for vector_clock_item in item_version.vector_clock:
            vector_clock.append(
                {
                    "provider_id": vector_clock_item.provider_id,
                    "timestamp": vector_clock_item.timestamp,
                }
            )

        current_item_change = cast("ItemChange", item_version.current_item_change)
        self.db.collection("sync_framework__item_versions").document(
            str(item_version.item_id)
        ).set(
            {
                "date_created": item_version.date_created,
                "current_item_change_id": str(current_item_change.id),
                "vector_clock": vector_clock,
                "collection_name": "my_app_item",
            }
        )

    def _add_conflict_log(self, conflict_log: "ConflictLog"):  # pragma: no cover
        self.db.collection("sync_framework__conflict_logs").document(
            str(conflict_log.id)
        ).set(
            {
                "created_at": conflict_log.created_at,
                "resolved_at": conflict_log.resolved_at,
                "item_change_loser_id": str(conflict_log.item_change_loser.id),
                "item_change_winner_id": str(conflict_log.item_change_winner.id)
                if conflict_log.item_change_winner is not None
                else None,
                "status": conflict_log.status.value,
                "conflict_type": conflict_log.conflict_type.value,
                "description": conflict_log.description,
            }
        )

    def _add_item(self, item: "Any"):  # pragma: no cover
        item_to_save = copy.deepcopy(item)
        id = item_to_save.pop("id")
        item_to_save.pop("collection_name")
        self.db.collection("my_app_item").document(str(id)).set(item_to_save)
