from maestro.backends.base_nosql.utils import (
    get_collection_name,
    type_to_collection,
    entity_name_to_collection,
)
from maestro.backends.base_nosql.store import NoSQLDataStore
from maestro.backends.mongo.converters import DateConverter
from maestro.backends.mongo.utils import convert_to_mongo_filter, convert_to_mongo_sort
from maestro.core.exceptions import ItemNotFoundException
from maestro.core.query.metadata import Query, TrackedQuery, Filter
from maestro.core.query.store import TrackQueriesStoreMixin
from maestro.core.metadata import (
    VectorClock,
    ItemVersion,
    ItemChange,
    ItemChangeBatch,
    ConflictLog,
    ConflictStatus,
    SyncSession,
)
from maestro.backends.base_nosql.collections import (
    CollectionType,
    ItemChangeRecord,
    ConflictLogRecord,
)
from typing import Dict, Optional, List, Callable, Any, Set
import uuid
import pymongo


class MongoDataStore(TrackQueriesStoreMixin, NoSQLDataStore):
    def __init__(self, *args, **kwargs):
        self.db = kwargs.pop("db")
        self.client = kwargs.pop("client")
        self.tracked_query_metadata_converter = kwargs.pop(
            "tracked_query_metadata_converter"
        )
        self.item_field_getter: "Callable[[Any, str], Any]" = lambda item, field_name: item[
            field_name
        ]
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

    def get_tracked_query(self, query: "Query") -> "Optional[TrackedQuery]":
        doc = self._get_collection_query(CollectionType.TRACKED_QUERIES).find_one(
            {"_id": query.get_id()}
        )
        if not doc:
            return None

        instance = self._document_to_raw_instance(document=doc)
        tracked_query = self.tracked_query_metadata_converter.to_metadata(
            record=instance
        )
        return tracked_query

    def get_tracked_queries(self) -> "List[TrackedQuery]":
        docs = self._get_collection_query(CollectionType.TRACKED_QUERIES).find(
            filter={}
        )

        tracked_queries: "List[TrackedQuery]" = []
        for doc in docs:
            instance = self._document_to_raw_instance(doc)
            tracked_query = self.tracked_query_metadata_converter.to_metadata(
                record=instance
            )
            tracked_queries.append(tracked_query)

        return tracked_queries

    def query_items(
        self, query: "Query", vector_clock: "Optional[VectorClock]"
    ) -> "List[Any]":

        collection_name = entity_name_to_collection(query.entity_name)
        mongo_filter: "Dict" = {
            "is_applied": {"$eq": True},
            "should_ignore": {"$eq": False,},
            "collection_name": {"$eq": collection_name},
        }
        item_mongo_filter = convert_to_mongo_filter(
            filter=query.filter, field_prefix="serialized_item."
        )
        mongo_filter.update(item_mongo_filter)

        if vector_clock:
            or_expressions: "List[Dict]" = []
            provider_ids = []
            for vector_clock_item in vector_clock:
                provider_ids.append(vector_clock_item.provider_id)
                or_expressions.append(
                    {
                        "$and": [
                            {
                                "provider_id": vector_clock_item.provider_id,
                                "provider_timestamp": {
                                    "$lte": self.date_converter.serialize_date(
                                        vector_clock_item.timestamp
                                    )
                                },
                            }
                        ]
                    }
                )
            or_expressions.append({"provider_id": {"$nin": provider_ids}})

            mongo_filter["$or"] = or_expressions

        pipeline = [
            {"$match": mongo_filter,},
            {
                "$setWindowFields": {
                    "partitionBy": "$item_id",
                    "sortBy": {"date_created": pymongo.DESCENDING},
                    "output": {"serialized_item": {"$first": "$serialized_item"}},
                },
            },
            {
                "$group": {
                    "_id": "$item_id",
                    "item": {"$first": "$$ROOT.serialized_item"},
                },
            },
            {"$replaceRoot": {"newRoot": "$item"},},
        ]

        if query.ordering:
            mongo_sort = convert_to_mongo_sort(ordering=query.ordering)
            pipeline.append({"$sort": mongo_sort})

        if query.offset:
            pipeline.append({"$skip": query.offset})

        if query.limit:
            pipeline.append({"$limit": query.limit})

        docs = self._get_collection_query(CollectionType.ITEM_CHANGES).aggregate(
            pipeline
        )

        items = [doc for doc in docs]

        return items

    def save_tracked_query(self, tracked_query: "TrackedQuery"):
        instance = self.tracked_query_metadata_converter.to_record(
            metadata_object=tracked_query
        )
        self._save(
            instance=instance,
            collection=type_to_collection(key=CollectionType.TRACKED_QUERIES),
        )

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
            tracked_query = self.get_tracked_query(query=query)
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

        filtered_item_ids: "Optional[Set[str]]" = None
        if query:
            filtered_item_ids = self.get_item_ids_for_query(
                query=query, vector_clock=vector_clock
            )

        item_change_records: "List[ItemChangeRecord]" = []
        provider_ids = self._get_provider_ids()

        for provider_id in provider_ids:
            vector_clock_item = vector_clock.get_vector_clock_item(
                provider_id=provider_id
            )
            mongo_filter = {
                "provider_id": {"$eq": vector_clock_item.provider_id},
                "provider_timestamp": {
                    "$gt": self.date_converter.serialize_date(
                        vector_clock_item.timestamp
                    )
                },
            }

            if filtered_item_ids:
                mongo_filter["item_id"] = {"$in": list(filtered_item_ids)}

            docs = self._get_collection_query(CollectionType.ITEM_CHANGES).find(
                filter=mongo_filter,
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

        filtered_item_ids: "Optional[Set[str]]" = None
        if query:
            filtered_item_ids = self.get_item_ids_for_query(
                query=query, vector_clock=vector_clock
            )

        selected_item_changes = []
        for item_change in item_changes:
            vector_clock_item = vector_clock.get_vector_clock_item(
                provider_id=item_change.provider_id
            )
            if item_change.provider_timestamp > vector_clock_item.timestamp:
                if (
                    filtered_item_ids is not None
                    and item_change.item_id not in filtered_item_ids
                ):
                    continue
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

    def save_item_change(
        self, item_change: "ItemChange", is_creating: "bool" = False
    ) -> "ItemChange":

        super().save_item_change(item_change=item_change, is_creating=is_creating)
        if is_creating:
            self.check_tracked_query_vector_clocks(new_item_change=item_change)
        return item_change

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
