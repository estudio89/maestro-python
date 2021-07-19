from maestro.core.store import BaseDataStore
from maestro.core.query import Query
from maestro.core.metadata import (
    ItemChange,
    ConflictLog,
    ItemVersion,
    Operation,
    SyncSession,
    VectorClock,
)
from maestro.backends.base_nosql.collections import CollectionType, ItemChangeRecord
from maestro.backends.base_nosql.utils import type_to_collection, query_filter_to_lambda
from maestro.core.utils import cast_away_optional
from typing import List, Dict, Any, Optional
from abc import abstractmethod
import copy


class NoSQLDataStore(BaseDataStore):
    def _update_provider_global_vector_clock(
        self, item_change_record: "ItemChangeRecord"
    ):
        self._save(
            instance={
                "timestamp": item_change_record["provider_timestamp"],
                "id": item_change_record["provider_id"],
            },
            collection=type_to_collection(key=CollectionType.PROVIDER_IDS),
        )

    def _update_query_vector_clock(
        self, query: "Query", item_change_record: "ItemChangeRecord"
    ):
        """Updates the vector clock for the given query.

        Args:
            query (Query): The query
            item_change_record (ItemChangeRecord): The record that caused the update
        """
        raise NotImplementedError("This backend doesn't support queries!")

    def _check_tracked_query_vector_clocks(self, item_change_record: "Any"):
        item = item_change_record["serialized_item"]
        local_version = self.get_item_version(item_id=item_change_record["item_id"])
        old_item: "Optional[Dict]"
        if local_version:
            old_item = self.deserialize_item(
                cast_away_optional(local_version.current_item_change).serialized_item
            )

        else:
            old_item = None

        for tracked_query in self.get_tracked_queries():
            if self._check_impacts_query(
                item=item, query=tracked_query.query, vector_clock=None
            ):
                self._update_query_vector_clock(
                    query=tracked_query.query, item_change_record=item_change_record
                )
            elif old_item and self._check_impacts_query(
                item=old_item,
                query=tracked_query.query,
                vector_clock=tracked_query.vector_clock,
            ):
                self._update_query_vector_clock(
                    query=tracked_query.query, item_change_record=item_change_record
                )

    def _update_vector_clocks(self, item_change_record: "Any"):
        """
        Updates the cached VectorClocks with the new change.

        Args:
            item_change (ItemChange): ItemChange that was saved to the data store

        """
        self._update_provider_global_vector_clock(item_change_record=item_change_record)
        self._check_tracked_query_vector_clocks(item_change_record=item_change_record)

    def _check_impacts_query(
        self, item: "Dict", query: "Query", vector_clock: "Optional[VectorClock]"
    ) -> "bool":
        filter_check = query_filter_to_lambda(query.filter)
        if filter_check(item):
            items = self.query_items(query=query, vector_clock=vector_clock)
            item_ids = {item["id"] for item in items}
            if item["item_id"] in item_ids:
                return True

        return False

    @abstractmethod
    def find_item_changes(self, ids: "List[str]") -> "List[ItemChange]":
        """
        Finds multiple ItemChanges by their IDs. The elements must be returned in
        the same order as the list of IDs.

        Args:
            ids (List[str]): List of ItemChange IDs
        """

    @abstractmethod
    def _save(self, instance: "Dict", collection: "str"):
        """
        Saves an item to the data store.

        Args:
            instance (Dict): data being saved
            collection (str): collection name
        """

    @abstractmethod
    def _delete(self, instance: "Dict", collection: "str"):
        """
        Deletes an item from the data store.

        Args:
            instance (Dict): item being deleted
            collection (str): collection name
        """

    def _get_hashable_item(self, item: "Any"):
        return tuple(item[attr] for attr in item)

    def save_item(self, item: "Any"):
        copied = copy.deepcopy(item)
        self._save(instance=copied, collection=copied.pop("collection_name"))

    def delete_item(self, item: "Any"):
        copied = copy.deepcopy(item)
        self._delete(instance=copied, collection=copied.pop("collection_name"))

    def save_item_change(
        self, item_change: "ItemChange", is_creating: "bool" = False
    ) -> "ItemChange":
        item_change_record = self.item_change_metadata_converter.to_record(
            metadata_object=item_change
        )
        self._save(
            instance=item_change_record,
            collection=type_to_collection(key=CollectionType.ITEM_CHANGES),
        )
        if is_creating:
            self._update_vector_clocks(item_change_record=item_change_record)

        return item_change

    def save_conflict_log(self, conflict_log: "ConflictLog"):
        conflict_log_record = self.conflict_log_metadata_converter.to_record(
            metadata_object=conflict_log
        )
        self._save(
            instance=conflict_log_record,
            collection=type_to_collection(key=CollectionType.CONFLICT_LOGS),
        )

    def execute_item_change(self, item_change: "ItemChange"):
        item = self.deserialize_item(serialized_item=item_change.serialized_item)

        if item_change.operation == Operation.DELETE:
            self.delete_item(item=item)
        else:
            self.save_item(item=item)

    def save_item_version(self, item_version: "ItemVersion"):
        item_version_record = self.item_version_metadata_converter.to_record(
            metadata_object=item_version
        )
        self._save(
            instance=item_version_record,
            collection=type_to_collection(key=CollectionType.ITEM_VERSIONS),
        )

    def save_sync_session(self, sync_session: "SyncSession"):
        sync_session_record = self.sync_session_metadata_converter.to_record(
            metadata_object=sync_session
        )
        self._save(
            instance=sync_session_record,
            collection=type_to_collection(key=CollectionType.SYNC_SESSIONS),
        )
