from maestro.backends.base_nosql.store import NoSQLDataStore
from maestro.core.exceptions import ItemNotFoundException
from maestro.core.query import Query
from maestro.core.metadata import (
    VectorClock,
    ItemVersion,
    ItemChange,
    ItemChangeBatch,
    Operation,
    ConflictLog,
    ConflictStatus,
    SyncSession,
)
from maestro.backends.base_nosql.collections import (
    CollectionType,
    ItemChangeRecord,
    ConflictLogRecord,
)
from firebase_admin import firestore
from maestro.backends.base_nosql.utils import get_collection_name, type_to_collection
import copy
from typing import Dict, Optional, List, Any, Callable
import uuid
import datetime as dt
import logging

logger = logging.getLogger(__name__)


class FirestoreUsage:
    """
        - writes:
            INSERT
            >>> 4 + 5/item

            DELETE
            >>> 4 + 4/item

            UPDATE
            >>> 4 + 5/item


    """

    debug_reads = False
    debug_writes = False
    enabled = False

    def __init__(self):
        self.reset(enable=False)

    def _log(self, message, collection_name, document_id):  # pragma: no cover
        import inspect

        previous_frame = inspect.currentframe().f_back.f_back.f_back.f_back
        (filename, line_number, function_name, lines, index) = inspect.getframeinfo(
            previous_frame
        )

        log_message = " ".join(
            [
                message,
                filename,
                line_number,
                function_name,
                [v.strip() for v in lines],
                collection_name,
                document_id,
            ]
        )
        logger.info(log_message)

    def reset(self, enable=True):
        if enable:
            self.enabled = True
        self.num_writes = 0
        self.num_reads = 0
        self.num_deletes = 0

    def register_write(self, collection_name, document_id):
        if self.enabled:
            self.num_writes += 1

            if self.debug_writes:  # pragma: no cover
                self._log(
                    message="wrote document",
                    collection_name=collection_name,
                    document_id=document_id,
                )

    def register_read(self, collection_name, document_id):
        if self.enabled:
            self.num_reads += 1

            if self.debug_reads:  # pragma: no cover
                self._log(
                    message="read document",
                    collection_name=collection_name,
                    document_id=document_id,
                )

    def register_delete(self):
        if self.enabled:
            self.num_deletes += 1

    def show(self, should_print=False):

        usage = {
            "reads": self.num_reads,
            "writes": self.num_writes,
            "deletes": self.num_deletes,
        }
        if should_print:  # pragma: no cover
            from pprint import pprint

            pprint(usage)
        else:
            logger.info("Firestore usage: %s", str(usage))


class FirestoreCache:
    def __init__(self):
        self._cache = {}

    def save(self, collection_name: "str", document_id: "str", value: "Any"):
        collection_data = self._cache.get(collection_name, {})
        collection_data[str(document_id)] = copy.deepcopy(value)
        self._cache[collection_name] = collection_data

    def get(self, collection_name: "str", document_id: "str"):
        collection_data = self._cache.get(collection_name, {})
        value = collection_data.get(str(document_id))
        return copy.deepcopy(value)


class FirestoreDataStore(NoSQLDataStore):
    """ Reference: https://googleapis.dev/python/firestore/latest/collection.html"""

    def __init__(self, *args, **kwargs):
        self.db = kwargs.pop("db")
        super().__init__(*args, **kwargs)
        self.current_transaction = None
        self._usage = FirestoreUsage()
        self._cache = FirestoreCache()

    def _get_collection_query(self, key: "CollectionType"):
        collection_name = type_to_collection(key=key)
        return self.db.collection(collection_name)

    def _document_to_raw_instance(self, document, ignore_read=False):
        instance = document.to_dict()
        instance["id"] = document.id

        if not ignore_read:
            self._usage.register_read(
                collection_name=document.reference.parent.id, document_id=document.id
            )
        return instance

    def _delete(self, instance: "Dict", collection: "str"):
        pk = instance.pop("id")
        doc_ref = self.db.collection(collection).document(str(pk))
        if self.current_transaction is None:
            doc_ref.delete()
        else:
            self.current_transaction.delete(doc_ref)
        self._usage.register_delete()

    def _get_provider_ids(self) -> "List[str]":
        collection_name = type_to_collection(key=CollectionType.PROVIDER_IDS)
        docs = self.db.collection(collection_name).get()
        provider_ids = [doc.id for doc in docs]
        if not provider_ids:
            self._usage.register_read(collection_name=collection_name, document_id="")
        for provider_id in provider_ids:
            self._usage.register_read(
                collection_name=collection_name, document_id=provider_id
            )
        return provider_ids

    def _save(self, instance: "Dict", collection: "str"):
        self._cache.save(
            collection_name=collection, document_id=instance["id"], value=instance
        )
        pk = instance.pop("id")
        doc_ref = self.db.collection(collection).document(str(pk))

        if self.current_transaction is None:
            doc_ref.set(instance)
        else:
            self.current_transaction.set(doc_ref, instance)

        self._usage.register_write(collection_name=collection, document_id=pk)

    def _check_tracked_query_vector_clocks(self, item_change_record: "Any"):
        pass

    def get_local_vector_clock(self, query: "Optional[Query]" = None) -> "VectorClock":
        if query is not None:
            raise ValueError("This backend doesn't support queries!")
        vector_clock = VectorClock.create_empty(provider_ids=[self.local_provider_id])
        docs = self._get_collection_query(CollectionType.PROVIDER_IDS).get()
        if not docs:
            collection_name = type_to_collection(key=CollectionType.PROVIDER_IDS)
            self._usage.register_read(collection_name=collection_name, document_id="")
        for doc in docs:
            instance = self._document_to_raw_instance(doc)
            vector_clock.update_vector_clock_item(
                provider_id=instance["id"], timestamp=instance["timestamp"]
            )
        return vector_clock

    def get_item_version(self, item_id: "str") -> "Optional[ItemVersion]":
        collection_name = type_to_collection(key=CollectionType.ITEM_VERSIONS)
        instance = self._cache.get(collection_name=collection_name, document_id=item_id)
        if not instance:
            doc = (
                self._get_collection_query(CollectionType.ITEM_VERSIONS)
                .document(str(item_id))
                .get()
            )
            if doc.exists:
                instance = self._document_to_raw_instance(doc)
                self._cache.save(
                    collection_name=collection_name, document_id=item_id, value=instance
                )
            else:
                self._usage.register_read(
                    collection_name=collection_name, document_id=""
                )

        if instance:
            item_version = self.item_version_metadata_converter.to_metadata(
                record=instance
            )
            return item_version
        else:
            return None

    def get_item_change_by_id(self, id: "uuid.UUID") -> "ItemChange":
        doc = (
            self._get_collection_query(CollectionType.ITEM_CHANGES)
            .document(str(id))
            .get()
        )
        if doc.exists:
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
            docs = (
                self._get_collection_query(CollectionType.ITEM_CHANGES)
                .where("provider_id", "==", vector_clock_item.provider_id)
                .where("provider_timestamp", ">", vector_clock_item.timestamp)
                .order_by("provider_timestamp")
                .limit(max_num)
                .get()
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

        docs = (
            self._get_collection_query(CollectionType.CONFLICT_LOGS)
            .where("status", "==", ConflictStatus.DEFERRED.value)
            .order_by("created_at")
            .limit(max_num)
            .get()
        )
        collection_name = type_to_collection(key=CollectionType.CONFLICT_LOGS)
        if not docs:
            self._usage.register_read(collection_name="", document_id=collection_name)
            return ItemChangeBatch(item_changes=[], is_last_batch=True)

        item_change_ids = []
        if not docs:
            self._usage.register_read(collection_name="", document_id=collection_name)

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
        self.current_transaction = self.db.transaction()
        collection_name = get_collection_name(
            serialized_item=item_change.serialized_item
        )

        @firestore.transactional
        def in_transaction(transaction):
            try:
                self.db.collection(collection_name).document(
                    str(item_change.item_id)
                ).get(transaction=self.current_transaction)
                callback()
            finally:
                self.current_transaction = None

        in_transaction(self.current_transaction)

    def get_deferred_conflict_logs(
        self, item_change_loser: "ItemChange"
    ) -> "List[ConflictLog]":
        docs = (
            self._get_collection_query(key=CollectionType.CONFLICT_LOGS)
            .order_by("created_at")
            .where("item_change_loser_id", "==", str(item_change_loser.id))
            .where("status", "==", ConflictStatus.DEFERRED.value)
            .get()
        )
        metadata_objects = []
        if not docs:
            collection_name = type_to_collection(key=CollectionType.CONFLICT_LOGS)
            self._usage.register_read(collection_name=collection_name, document_id="")
        for doc in docs:
            record = self._document_to_raw_instance(doc)
            metadata_object = self.conflict_log_metadata_converter.to_metadata(record)
            metadata_objects.append(metadata_object)
        return metadata_objects

    def get_item_changes(self) -> "List[ItemChange]":
        docs = (
            self._get_collection_query(key=CollectionType.ITEM_CHANGES)
            .order_by("date_created")
            .get()
        )
        metadata_objects = []
        if not docs:
            collection_name = type_to_collection(key=CollectionType.ITEM_CHANGES)
            self._usage.register_read(collection_name=collection_name, document_id="")

        for doc in docs:
            record = self._document_to_raw_instance(doc, ignore_read=True)
            metadata_object = self.item_change_metadata_converter.to_metadata(record)
            metadata_objects.append(metadata_object)
        return metadata_objects

    def find_item_changes(
        self, ids: "List[str]", sort: "bool" = True
    ) -> "List[ItemChange]":
        if not ids:
            return []

        collection_name = type_to_collection(key=CollectionType.ITEM_CHANGES)
        instances = []
        ids_not_in_cache = []
        for id in ids:
            value = self._cache.get(collection_name=collection_name, document_id=id)
            if value:
                instances.append(value)
            else:
                ids_not_in_cache.append(id)

        if ids_not_in_cache:
            refs = [
                self._get_collection_query(key=CollectionType.ITEM_CHANGES).document(id)
                for id in ids_not_in_cache
            ]
            docs = self.db.get_all(refs)
            if not docs:
                self._usage.register_read(
                    collection_name=collection_name, document_id=""
                )
            for doc in docs:
                if doc.exists:
                    instance = self._document_to_raw_instance(doc)
                    self._cache.save(
                        collection_name=collection_name,
                        document_id=instance["id"],
                        value=instance,
                    )
                    instances.append(instance)

        metadata_objects = []
        for instance in instances:
            metadata_object = self.item_change_metadata_converter.to_metadata(
                record=instance
            )
            metadata_objects.append(metadata_object)

        return metadata_objects

    def get_item_versions(self) -> "List[ItemVersion]":
        docs = (
            self._get_collection_query(key=CollectionType.ITEM_VERSIONS)
            .order_by("date_created")
            .get()
        )
        metadata_objects = []
        if not docs:
            collection_name = type_to_collection(key=CollectionType.ITEM_VERSIONS)
            self._usage.register_read(collection_name=collection_name, document_id="")
        for doc in docs:
            record = self._document_to_raw_instance(doc, ignore_read=True)
            metadata_object = self.item_version_metadata_converter.to_metadata(record)
            metadata_objects.append(metadata_object)
        return metadata_objects

    def get_sync_sessions(self) -> "List[SyncSession]":
        docs = (
            self._get_collection_query(key=CollectionType.SYNC_SESSIONS)
            .order_by("started_at")
            .get()
        )
        metadata_objects = []
        if not docs:
            collection_name = type_to_collection(key=CollectionType.SYNC_SESSIONS)
            self._usage.register_read(collection_name=collection_name, document_id="")
        for doc in docs:
            record = self._document_to_raw_instance(doc, ignore_read=True)
            metadata_object = self.sync_session_metadata_converter.to_metadata(record)
            metadata_objects.append(metadata_object)
        return metadata_objects

    def get_conflict_logs(self) -> "List[ConflictLog]":
        docs = (
            self._get_collection_query(key=CollectionType.CONFLICT_LOGS)
            .order_by("created_at")
            .get()
        )
        metadata_objects = []
        if not docs:
            collection_name = type_to_collection(key=CollectionType.CONFLICT_LOGS)
            self._usage.register_read(collection_name=collection_name, document_id="")
        for doc in docs:
            record = self._document_to_raw_instance(doc, ignore_read=True)
            metadata_object = self.conflict_log_metadata_converter.to_metadata(record)
            metadata_objects.append(metadata_object)
        return metadata_objects
