from pymongo import MongoClient
from pymongo.database import Database
from typing import Any, cast
from maestro.core.utils import BaseSyncLock
from maestro.core.events import EventsManager
from maestro.core.provider import BaseSyncProvider
from maestro.core.store import BaseDataStore
from maestro.core.execution import ChangesExecutor
import unittest
import copy

from maestro.core.metadata import (
    SyncSession,
    ItemVersion,
    ItemChange,
    ConflictLog,
    VectorClock,
)
from maestro.core.exceptions import ItemNotFoundException
from maestro.backends.in_memory import (
    InMemoryDataStore,
    InMemorySyncProvider,
    InMemorySyncLock,
    NullConverter,
    JSONSerializer,
)

from maestro.backends.mongo import (
    MongoDataStore,
    SyncSessionMetadataConverter,
    ItemVersionMetadataConverter,
    ItemChangeMetadataConverter,
    ConflictLogMetadataConverter,
    VectorClockMetadataConverter,
    MongoItemSerializer,
    MongoSyncProvider,
    DateConverter,
)

from maestro.backends.base_nosql.converters import TrackedQueryMetadataConverter
import uuid
import tests.base
import datetime as dt


class MongoTestCase(unittest.TestCase):
    connect_uri = "mongodb://maestro:maestro@localhost:27000"

    @classmethod
    def setUpClass(cls):
        cls.client = MongoClient(cls.connect_uri, tz_aware=True, tzinfo=dt.timezone.utc)
        cls.db = cls.client.test_db

    def tearDown(self):
        self.client.drop_database("test_db")

    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database("test_db")


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
        serializer = MongoItemSerializer()
        deserialized = serializer.deserialize_item(serialized_item=serialized_item)
        # print("update_item - deserialized:", deserialized)
        if item:
            item.update(deserialized)
            return item
        else:
            return deserialized

    def serialize_item(self, item):
        return (
            '{"entity_name": "test_items", "fields": {"name": "%s", "version": "%s"}, "pk": "%s"}'
            % (item["name"], item["version"], str(item["id"]))
        )


class TestMongoDataStore(MongoDataStore):
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
        docs = self.db["test_items"].find(filter={})
        items = []
        for doc in docs:
            record = self._document_to_raw_instance(doc)
            record["collection_name"] = "test_items"
            items.append(record)
        return items

    def get_item_by_id(self, id):
        doc = self.db["test_items"].find_one(filter={"_id": str(id)})
        if not doc:
            raise ItemNotFoundException(item_type="Item", id=id)

        record = self._document_to_raw_instance(doc)
        record["collection_name"] = "test_items"
        return record


class MongoBackendTestMixin(tests.base.BackendTestMixin):
    db: "Database"
    client: "MongoClient"

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
            '{"entity_name": "test_items", "fields": {"name": "%s", "version": "%s"}, "pk": "%s"}'
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
            self.item_serializer = MongoItemSerializer()
            self.date_converter = DateConverter()
            sync_session_metadata_converter = SyncSessionMetadataConverter()
            conflict_log_metadata_converter = ConflictLogMetadataConverter()
            vector_clock_metadata_converter = VectorClockMetadataConverter()
            item_version_metadata_converter = ItemVersionMetadataConverter(
                vector_clock_converter=vector_clock_metadata_converter
            )
            item_change_metadata_converter = ItemChangeMetadataConverter(
                item_serializer=self.item_serializer,
                vector_clock_converter=vector_clock_metadata_converter,
            )

            tracked_query_metadata_converter = TrackedQueryMetadataConverter(
                vector_clock_converter=vector_clock_metadata_converter
            )

            data_store = TestMongoDataStore(
                local_provider_id=local_provider_id,
                sync_session_metadata_converter=sync_session_metadata_converter,
                item_version_metadata_converter=item_version_metadata_converter,
                item_change_metadata_converter=item_change_metadata_converter,
                conflict_log_metadata_converter=conflict_log_metadata_converter,
                vector_clock_metadata_converter=vector_clock_metadata_converter,
                item_serializer=self.item_serializer,
                tracked_query_metadata_converter=tracked_query_metadata_converter,
                db=self.db,
                client=self.client,
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
            return MongoSyncProvider(
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
                    "timestamp": self.date_converter.serialize_date(
                        vector_clock_item.timestamp
                    ),
                }
            )
        self.db["maestro__item_changes"].update_one(
            filter={"_id": str(item_change.id)},
            update={
                "$set": {
                    "date_created": self.date_converter.serialize_date(
                        item_change.date_created
                    ),
                    "operation": item_change.operation.value,
                    "item_id": str(item_change.item_id),
                    "collection_name": "my_app_item",
                    "provider_timestamp": self.date_converter.serialize_date(
                        item_change.provider_timestamp
                    ),
                    "provider_id": item_change.provider_id,
                    "insert_provider_timestamp": self.date_converter.serialize_date(
                        item_change.insert_provider_timestamp
                    ),
                    "insert_provider_id": item_change.insert_provider_id,
                    "serialized_item": self.item_serializer.deserialize_item(
                        item_change.serialized_item
                    ),
                    "should_ignore": item_change.should_ignore,
                    "is_applied": item_change.is_applied,
                    "vector_clock": vector_clock,
                }
            },
            upsert=True,
        )

        self.db["maestro__provider_ids"].update_one(
            filter={"_id": item_change.provider_id},
            update={
                "$set": {
                    "timestamp": self.date_converter.serialize_date(
                        item_change.provider_timestamp
                    )
                }
            },
            upsert=True,
        )

    def _add_item_version(self, item_version: "ItemVersion"):  # pragma: no cover
        vector_clock = []
        for vector_clock_item in item_version.vector_clock:
            vector_clock.append(
                {
                    "provider_id": vector_clock_item.provider_id,
                    "timestamp": self.date_converter.serialize_date(
                        vector_clock_item.timestamp
                    ),
                }
            )

        current_item_change = cast("ItemChange", item_version.current_item_change)
        self.db["maestro__item_versions"].update_one(
            filter={"_id": str(item_version.item_id)},
            update={
                "$set": {
                    "date_created": self.date_converter.serialize_date(
                        item_version.date_created
                    ),
                    "current_item_change_id": str(current_item_change.id),
                    "vector_clock": vector_clock,
                    "collection_name": "my_app_item",
                }
            },
            upsert=True,
        )

    def _add_conflict_log(self, conflict_log: "ConflictLog"):  # pragma: no cover
        self.db["maestro__conflict_logs"].update_one(
            filter={"_id": str(conflict_log.id)},
            update={
                "$set": {
                    "created_at": self.date_converter.serialize_date(
                        conflict_log.created_at
                    ),
                    "resolved_at": self.date_converter.serialize_date(
                        conflict_log.resolved_at
                    ),
                    "item_change_loser_id": str(conflict_log.item_change_loser.id),
                    "item_change_winner_id": str(conflict_log.item_change_winner.id)
                    if conflict_log.item_change_winner is not None
                    else None,
                    "status": conflict_log.status.value,
                    "conflict_type": conflict_log.conflict_type.value,
                    "description": conflict_log.description,
                }
            },
            upsert=True,
        )

    def _add_item(self, item: "Any"):  # pragma: no cover
        item_to_save = copy.deepcopy(item)
        id = item_to_save.pop("id")
        item_to_save.pop("collection_name")
        self.db["my_app_item"].update_one(
            filter={"_id": str(id)}, update={"$set": item_to_save}, upsert=True,
        )
