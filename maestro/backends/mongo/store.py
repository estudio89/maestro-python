from maestro.backends.base_nosql.utils import get_collection_name, type_to_collection
from maestro.backends.base_nosql.store import NoSQLDataStore
from maestro.backends.base_nosql.converters import TrackedQueryMetadataConverter
from maestro.backends.mongo.converters import DateConverter
from maestro.core.exceptions import ItemNotFoundException
from maestro.core.query import Query
from maestro.core.metadata import (
    VectorClock,
    ItemVersion,
    ItemChange,
    ItemChangeBatch,
    ConflictLog,
    ConflictStatus,
    SyncSession,
    TrackedQuery,
)
from maestro.backends.base_nosql.collections import (
    CollectionType,
    ItemChangeRecord,
    ConflictLogRecord,
)
from typing import Dict, Optional, List, Callable, cast
import uuid
import copy
import pymongo


class MongoDataStore(NoSQLDataStore):
    tracked_query_metadata_converter: "TrackedQueryMetadataConverter"

    def __init__(self, *args, **kwargs):
        self.db = kwargs.pop("db")
        self.client = kwargs.pop("client")
        self.tracked_query_metadata_converter = kwargs.pop(
            "tracked_query_metadata_converter"
        )
        super().__init__(*args, **kwargs)
        self.session = None
        self.date_converter = DateConverter()

    def _get_collection_query(self, key: "CollectionType"):
        collection_name = type_to_collection(key=key)
        return self.db[collection_name]

    def _document_to_raw_instance(self, document):
        document["id"] = document.pop("_id")
        document.pop("_lock", None)

        return document

    def _delete(self, instance: "Dict", collection: "str"):
        pk = instance.pop("id")
        self.db[collection].delete_one({"_id": str(pk)}, session=self.session)

    def _get_provider_ids(self) -> "List[str]":
        collection_name = type_to_collection(key=CollectionType.PROVIDER_IDS)
        docs = self.db[collection_name].find()
        provider_ids = [doc["_id"] for doc in docs]
        return provider_ids

    def _get_tracked_query(self, query: "Query") -> "Optional[TrackedQuery]":
        doc = self._get_collection_query(CollectionType.TRACKED_QUERIES).find_one(
            {"_id": query.get_id()}
        )
        if not doc:
            return None

        instance = self._document_to_raw_instance(document=doc)
        tracked_query = self.tracked_query_metadata_converter.to_metadata(record=instance)
        return tracked_query

    def _update_query_vector_clock(
        self, query: "Query", item_change_record: "ItemChangeRecord"
    ):
        tracked_query = self._get_tracked_query(query=query)

        if tracked_query:
            vector_clock = copy.deepcopy(tracked_query.vector_clock)
        else:
            vector_clock = VectorClock.create_empty(
                provider_ids=self._get_provider_ids()
            )
            tracked_query = TrackedQuery(query=query, vector_clock=vector_clock)

        vector_clock.update_vector_clock_item(
            provider_id=item_change_record["provider_id"],
            timestamp=self.date_converter.deserialize_date(
                item_change_record["provider_timestamp"]
            ),
        )
        updated_tracked_query = TrackedQuery(query=query, vector_clock=vector_clock)
        instance = self.tracked_query_metadata_converter.to_record(
            metadata_object=updated_tracked_query
        )
        collection_name = type_to_collection(key=CollectionType.TRACKED_QUERIES)
        self._save(instance=cast("Dict", instance), collection=collection_name)

    def _save(self, instance: "Dict", collection: "str"):
        pk = instance.pop("id")
        self.db[collection].update_one(
            filter={"_id": str(pk)},
            update={"$set": instance},
            upsert=True,
            session=self.session,
        )

    def get_local_vector_clock(self, query: "Optional[Query]" = None) -> "VectorClock":
        if query is not None:
            tracked_query = self._get_tracked_query(query=query)
            if tracked_query:
                return tracked_query.vector_clock
            else:
                return VectorClock.create_empty(provider_ids=[self.local_provider_id])

        vector_clock = VectorClock.create_empty(provider_ids=[self.local_provider_id])
        docs = self._get_collection_query(CollectionType.PROVIDER_IDS).find()
        for doc in docs:
            instance = self._document_to_raw_instance(doc)
            timestamp = self.date_converter.deserialize_date(
                value=instance["timestamp"]
            )
            vector_clock.update_vector_clock_item(
                provider_id=instance["id"], timestamp=timestamp
            )
        return vector_clock

    def get_item_version(self, item_id: "str") -> "Optional[ItemVersion]":
        doc = self._get_collection_query(CollectionType.ITEM_VERSIONS).find_one(
            {"_id": str(item_id)}
        )
        if doc:
            instance = self._document_to_raw_instance(doc)
            item_version = self.item_version_metadata_converter.to_metadata(
                record=instance
            )
            return item_version
        else:
            return None

    def get_item_change_by_id(self, id: "uuid.UUID") -> "ItemChange":
        doc = self._get_collection_query(CollectionType.ITEM_CHANGES).find_one(
            filter={"_id": str(id)}
        )
        if doc:
            instance = self._document_to_raw_instance(doc)
            item_change = self.item_change_metadata_converter.to_metadata(
                record=instance
            )
            return item_change

        raise ItemNotFoundException(item_type="ItemChangeRecord", id=str(id))

    def select_changes(
        self,
        vector_clock: "VectorClock",
        max_num: "int",
        query: "Optional[Query]" = None,
    ) -> "ItemChangeBatch":

        if query is not None:
            raise ValueError("This backend doesn't support queries!")

        item_change_records: "List[ItemChangeRecord]" = []
        provider_ids = self._get_provider_ids()

        for provider_id in provider_ids:
            vector_clock_item = vector_clock.get_vector_clock_item(
                provider_id=provider_id
            )
            docs = self._get_collection_query(CollectionType.ITEM_CHANGES).find(
                filter={
                    "provider_id": {"$eq": vector_clock_item.provider_id},
                    "provider_timestamp": {
                        "$gt": self.date_converter.serialize_date(
                            vector_clock_item.timestamp
                        )
                    },
                },
                limit=max_num,
                sort=[["provider_timestamp", pymongo.ASCENDING]],
            )

            if docs:
                query_instances = [self._document_to_raw_instance(doc) for doc in docs]
                item_change_records += query_instances

        current_count = len(item_change_records)
        is_last_batch = current_count == 0

        item_change_records.sort(key=lambda record: record["date_created"])

        item_changes = []
        for item_change_record in item_change_records:
            item_change = self.item_change_metadata_converter.to_metadata(
                record=item_change_record
            )
            item_changes.append(item_change)

        item_change_batch = ItemChangeBatch(
            item_changes=item_changes, is_last_batch=is_last_batch
        )
        return item_change_batch

    def select_deferred_changes(
        self,
        vector_clock: "VectorClock",
        max_num: "int",
        query: "Optional[Query]" = None,
    ) -> "ItemChangeBatch":

        if query is not None:
            raise ValueError("This backend doesn't support queries!")

        docs = self._get_collection_query(CollectionType.CONFLICT_LOGS).find(
            filter={"status": {"$eq": ConflictStatus.DEFERRED.value}},
            sort=[["created_at", pymongo.ASCENDING]],
            limit=max_num,
        )
        if not docs:
            return ItemChangeBatch(item_changes=[], is_last_batch=True)

        item_change_ids = []
        for doc in docs:
            instance: "ConflictLogRecord" = self._document_to_raw_instance(doc)
            item_change_ids.append(instance["item_change_loser_id"])

        item_changes = self.find_item_changes(ids=item_change_ids)
        item_changes.sort(key=lambda item_change: item_change.date_created)

        selected_item_changes = []
        for item_change in item_changes:
            vector_clock_item = vector_clock.get_vector_clock_item(
                provider_id=item_change.provider_id
            )
            if item_change.provider_timestamp > vector_clock_item.timestamp:
                selected_item_changes.append(item_change)

        current_count = len(selected_item_changes)
        is_last_batch = current_count == 0

        item_change_batch = ItemChangeBatch(
            item_changes=selected_item_changes, is_last_batch=is_last_batch
        )

        return item_change_batch

    def run_in_transaction(self, item_change: "ItemChange", callback: "Callable"):
        collection_name = get_collection_name(
            serialized_item=item_change.serialized_item
        )

        with self.client.start_session() as self.session:
            with self.session.start_transaction():
                try:
                    self.db[collection_name].find_one_and_update(
                        filter={"_id": str(item_change.item_id)},
                        update={"$set": {"_lock": uuid.uuid4()}},
                        session=self.session,
                    )
                    callback()
                finally:
                    self.session = None

    def get_deferred_conflict_logs(
        self, item_change_loser: "ItemChange"
    ) -> "List[ConflictLog]":
        docs = self._get_collection_query(key=CollectionType.CONFLICT_LOGS).find(
            filter={
                "item_change_loser_id": {"$eq": str(item_change_loser.id)},
                "status": {"$eq": ConflictStatus.DEFERRED.value,},
            },
            sort=[["created_at", pymongo.ASCENDING]],
        )
        metadata_objects = []

        for doc in docs:
            record = self._document_to_raw_instance(doc)
            metadata_object = self.conflict_log_metadata_converter.to_metadata(record)
            metadata_objects.append(metadata_object)

        return metadata_objects

    def get_item_changes(self) -> "List[ItemChange]":
        docs = self._get_collection_query(key=CollectionType.ITEM_CHANGES).find(
            filter={}, sort=[["date_created", pymongo.ASCENDING]]
        )
        metadata_objects = []

        for doc in docs:
            record = self._document_to_raw_instance(doc)
            metadata_object = self.item_change_metadata_converter.to_metadata(record)
            metadata_objects.append(metadata_object)
        return metadata_objects

    def find_item_changes(self, ids: "List[str]") -> "List[ItemChange]":
        if not ids:
            return []

        instances = []

        docs = self._get_collection_query(key=CollectionType.ITEM_CHANGES).find(
            {"_id": {"$in": ids}},
        )
        for doc in docs:
            instance = self._document_to_raw_instance(doc)
            instances.append(instance)

        metadata_objects = []
        instances_map = {}
        for instance in instances:
            metadata_object = self.item_change_metadata_converter.to_metadata(
                record=instance
            )
            metadata_objects.append(metadata_object)
            instances_map[str(metadata_object.id)] = metadata_object

        metadata_objects = [instances_map[id] for id in ids if id in instances_map]

        return metadata_objects

    def get_item_versions(self) -> "List[ItemVersion]":
        docs = self._get_collection_query(key=CollectionType.ITEM_VERSIONS).find(
            filter={}, sort=[["date_created", pymongo.ASCENDING]]
        )
        metadata_objects = []

        for doc in docs:
            record = self._document_to_raw_instance(doc)
            metadata_object = self.item_version_metadata_converter.to_metadata(record)
            metadata_objects.append(metadata_object)
        return metadata_objects

    def get_sync_sessions(self) -> "List[SyncSession]":
        docs = self._get_collection_query(key=CollectionType.SYNC_SESSIONS).find(
            filter={}, sort=[["started_at", pymongo.ASCENDING]]
        )
        metadata_objects = []

        for doc in docs:
            record = self._document_to_raw_instance(doc)
            metadata_object = self.sync_session_metadata_converter.to_metadata(record)
            metadata_objects.append(metadata_object)
        return metadata_objects

    def get_conflict_logs(self) -> "List[ConflictLog]":
        docs = self._get_collection_query(key=CollectionType.CONFLICT_LOGS).find(
            filter={}, sort=[["created_at", pymongo.ASCENDING]]
        )
        metadata_objects = []

        for doc in docs:
            record = self._document_to_raw_instance(doc)
            metadata_object = self.conflict_log_metadata_converter.to_metadata(record)
            metadata_objects.append(metadata_object)
        return metadata_objects
