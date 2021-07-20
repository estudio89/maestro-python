from maestro.core.store import BaseDataStore
from maestro.core.metadata import (
    ItemChange,
    ConflictLog,
    ItemVersion,
    Operation,
    SyncSession,
)
from maestro.backends.base_nosql.collections import CollectionType
from maestro.backends.base_nosql.utils import type_to_collection
from typing import List, Dict, Any
from abc import abstractmethod
import copy


class NoSQLDataStore(BaseDataStore):

    def update_vector_clocks(self, item_change: "ItemChange"):
        """
        Updates the cached VectorClocks with the new change.

        Args:
            item_change (ItemChange): ItemChange that was saved to the data store

        """
        item_change_record = self.item_change_metadata_converter.to_record(
            metadata_object=item_change
        )
        self._save(
            instance={
                "timestamp": item_change_record["provider_timestamp"],
                "id": item_change_record["provider_id"],
            },
            collection=type_to_collection(key=CollectionType.PROVIDER_IDS),
        )

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
            self.update_vector_clocks(item_change=item_change)

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
