from typing import List, Any, Optional, Callable, Dict
from abc import ABC, abstractmethod
from .metadata import (
    VectorClock,
    ItemChange,
    ItemVersion,
    ItemChangeBatch,
    ConflictLog,
    Operation,
    SyncSession,
)
from .utils import BaseMetadataConverter, get_now_utc
from .serializer import BaseItemSerializer
from .exceptions import ItemNotFoundException
import copy
import uuid
from pprint import pprint


class BaseDataStore(ABC):
    """Abstract class that encapsulates the access to the storage system.

    Attributes:
        conflict_log_metadata_converter (BaseMetadataConverter): Instance used to convert ConflictLog objects to data store native records and back.
        item_change_metadata_converter (BaseMetadataConverter): Instance used to convert ItemChange objects to data store native records and back.
        item_serializer (BaseItemSerializer): Instance used to convert serialize data store items to strings.
        item_version_metadata_converter (BaseMetadataConverter): Instance used to convert ItemVersion objects to data store native records and back.
        local_provider_id (str): Unique identifier of the provider that controls this data store.
        sync_session_metadata_converter (BaseMetadataConverter): Instance used to convert SyncSession objects to data store native records and back.
        vector_clock_metadata_converter (BaseMetadataConverter): Instance used to convert VectorClock objects to data store native records and back.
    """

    local_provider_id: "str"
    sync_session_metadata_converter: "BaseMetadataConverter"
    item_version_metadata_converter: "BaseMetadataConverter"
    item_change_metadata_converter: "BaseMetadataConverter"
    conflict_log_metadata_converter: "BaseMetadataConverter"
    vector_clock_metadata_converter: "BaseMetadataConverter"
    item_serializer: "BaseItemSerializer"

    def __init__(
        self,
        local_provider_id: "str",
        sync_session_metadata_converter: "BaseMetadataConverter",
        item_version_metadata_converter: "BaseMetadataConverter",
        item_change_metadata_converter: "BaseMetadataConverter",
        conflict_log_metadata_converter: "BaseMetadataConverter",
        vector_clock_metadata_converter: "BaseMetadataConverter",
        item_serializer: "BaseItemSerializer",
    ):
        self.local_provider_id = local_provider_id
        self.sync_session_metadata_converter = sync_session_metadata_converter
        self.item_version_metadata_converter = item_version_metadata_converter
        self.item_change_metadata_converter = item_change_metadata_converter
        self.conflict_log_metadata_converter = conflict_log_metadata_converter
        self.vector_clock_metadata_converter = vector_clock_metadata_converter
        self.item_serializer = item_serializer

    def __repr__(self):  # pragma: no cover
        return (
            f"{self.__class__.__name__}(local_provider_id='{self.local_provider_id}')"
        )

    def get_local_version(self, item_id: "str",) -> "ItemVersion":
        """Retrieves the current version of the item with the given id.

        Args:
            item_id (str): Primary key of the item whose version we're looking for.
        """
        try:
            local_version = self.get_item_version(item_id=item_id)
        except ItemNotFoundException:
            local_version = None

        if local_version is None:
            vector_clock = VectorClock.create_empty(
                provider_ids=[self.local_provider_id]
            )
            now_utc = get_now_utc()
            local_version = ItemVersion(
                current_item_change=None,
                item_id=item_id,
                vector_clock=vector_clock,
                date_created=now_utc,
            )

        return copy.deepcopy(local_version)

    def get_or_create_item_change(self, item_change: "ItemChange") -> "ItemChange":
        """Looks for the ItemChange in the data store and if it's not found, saves it to the data store.

        Args:
            item_change (ItemChange): The ItemChange saved to the data store.

        """
        try:
            local_item_change = self.get_item_change_by_id(id=item_change.id)
            return copy.deepcopy(local_item_change)
        except ItemNotFoundException:
            if not item_change.date_created:
                now_utc = get_now_utc()
                item_change.date_created = now_utc
            self.save_item_change(item_change=item_change, is_creating=True)
            return copy.deepcopy(item_change)

    def commit_item_change(
        self, operation: "Operation", item_id: "str", item: "Any",
    ) -> "ItemChange":
        """This method will never be called directly by the sync framework but by the application consuming the framework.
           It will perform the operation given as well as record all the metadata necessary for synchronization.

        Args:
            operation (Operation): The operation being performed.
            item_id (str): The item's primary key.
            item (Any): The item that is being changed.
        """

        old_version = self.get_local_version(item_id=item_id)

        local_vector_clock = copy.deepcopy(old_version.vector_clock)
        now_utc = get_now_utc()
        local_vector_clock.update_vector_clock_item(
            provider_id=self.local_provider_id, timestamp=now_utc
        )

        item_change = ItemChange(
            id=uuid.uuid4(),
            operation=operation,
            item_id=item_id,
            provider_timestamp=now_utc,
            provider_id=self.local_provider_id,
            insert_provider_timestamp=old_version.current_item_change.insert_provider_timestamp
            if old_version.current_item_change
            else now_utc,
            insert_provider_id=old_version.current_item_change.insert_provider_id
            if old_version.current_item_change
            else self.local_provider_id,
            serialized_item=self.serialize_item(item),
            should_ignore=False,
            is_applied=True,
            vector_clock=local_vector_clock,
            date_created=now_utc,
        )

        if operation != Operation.DELETE:
            self.save_item(item=item)
        else:
            self.delete_item(item=item)

        self.save_item_change(item_change=item_change, is_creating=True)
        new_version = ItemVersion(
            item_id=item_id,
            current_item_change=item_change,
            date_created=old_version.date_created,
        )
        self.save_item_version(item_version=new_version)
        return item_change

    def serialize_item(self, item: "Any") -> "str":
        """Serializes the given item to a string.

        Args:
            item (Any): Item to be serialized.
        """
        return self.item_serializer.serialize_item(item=item)

    def deserialize_item(self, serialized_item: "str") -> "Any":
        """Deserializes a string back to an item.

        Args:
            serialized_item (str): Serialized item string.
        """
        return self.item_serializer.deserialize_item(serialized_item=serialized_item)

    @abstractmethod
    def get_local_vector_clock(self) -> "VectorClock":  # pragma: no cover
        """Returns the VectorClock calculated from the changes currently in the data store.
        """

    @abstractmethod
    def get_item_version(
        self, item_id: "str"
    ) -> "Optional[ItemVersion]":  # pragma: no cover
        """Returns the current version for the item with the given id or None if it's not found.

        Args:
            item_id (str): Primary key of the item whose current version we're looking for.
        """

    @abstractmethod
    def get_item_change_by_id(self, id: "uuid.UUID") -> "ItemChange":
        """Returns the ItemChange whose ID was given or raises an ItemNotFoundException if it's not found.

        Args:
            id (uuid.UUID): ItemChange's primary key.
        """

    @abstractmethod
    def select_changes(
        self, vector_clock: "VectorClock", max_num: "int"
    ) -> "ItemChangeBatch":  # pragma: no cover
        """Selects all changes commited after the VectorClock.
        The ItemChanges are returned in the same order as they were saved to the data store.

        Args:
            vector_clock (VectorClock): VectorClock that represents the state of the last sync pass.
            max_num (int): Maximum number of changes to be added to the ItemChangeBatch.
        """

    @abstractmethod
    def select_deferred_changes(
        self, vector_clock: "VectorClock", max_num: "int"
    ) -> "ItemChangeBatch":  # pragma: no cover
        """Selects all the changes that were not applied in the last sync session due to an exception having occurred.
        The ItemChanges are returned in the same order as they were saved to the data store.


        Args:
            vector_clock (VectorClock): VectorClock that represents the state of the last sync pass.
            max_num (int): Maximum number of changes to be added to the ItemChangeBatch.
        """

    @abstractmethod
    def save_item_change(
        self, item_change: "ItemChange", is_creating: "bool" = False
    ) -> "ItemChange":  # pragma: no cover
        """Saves the ItemChange to the data store.

        Args:
            item_change (ItemChange): Change to be saved.
        """

    @abstractmethod
    def save_item(self, item: "Any"):
        """Saves an item to the data store.

        Args:
            item (Any): Item being saved.
        """

    @abstractmethod
    def delete_item(self, item: "Any"):
        """Deletes an item from the data store.

        Args:
            item (Any): Item to be deleted.
        """

    @abstractmethod
    def run_in_transaction(
        self, item_change: "ItemChange", callback: "Callable"
    ):  # pragma: no cover
        """Runs the given callback inside a transaction.

        Args:
            item_change (ItemChange): The change being processed inside the callback. The goal of this parameter is to enable locking the item referenced by the change.
            callback (Callable): Callback to be run inside the transaction.
        """

    @abstractmethod
    def save_conflict_log(self, conflict_log: "ConflictLog"):  # pragma: no cover
        """Saves the ConflitLog to the data store.

        Args:
            conflict_log (ConflictLog): ConflictLog to be saved.

        """

    @abstractmethod
    def execute_item_change(self, item_change: "ItemChange"):  # pragma: no cover
        """Executes the change by performing its operation on the item it references.

        Args:
            item_change (ItemChange): Change to be executed.
        """

    @abstractmethod
    def save_item_version(self, item_version: "ItemVersion"):  # pragma: no cover
        """Saves a version to the data store.

        Args:
            item_version (ItemVersion): Version to be saved.

        """

    @abstractmethod
    def get_deferred_conflict_logs(
        self, item_change_loser: "ItemChange"
    ) -> "List[ConflictLog]":  # pragma: no cover
        """Searches for all the ConflictLogs that reference the given change and whose status is equal to ConflictStatus.DEFERRED.

        Args:
            item_change_loser (ItemChange): ItemChange being searched.
        """

    @abstractmethod
    def save_sync_session(self, sync_session: "SyncSession"):  # pragma: no cover
        """Saves a sync session to the data store.

        Args:
            sync_session (SyncSession): Sync session being saved.
        """

    # The methods below are only used in tests
    def get_items(self) -> "List[Any]":  # pragma: no cover
        """Returns a list with all the items in the data store.
        DO NOT use in production, used only in tests."""
        raise NotImplementedError()

    def get_item_by_id(self, id: "str") -> "Any":  # pragma: no cover
        """Returns the item with the given primary key. Used only in tests.

        Args:
            id (str): Item's primary key.

        Returns:
            Any: The matching item.

        Raises:
            ItemNotFoundException: If the item is not found.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_item_changes(self) -> "List[ItemChange]":
        """Returns all the changes in the data store in the order that they were saved. DO NOT use in production, used only in tests.

        Returns:
            List[ItemChange]: Changes in the data store.
        """

    @abstractmethod
    def get_item_versions(self) -> "List[ItemVersion]":
        """Returns all the versions saved to the data store in the order that they were saved. DO NOT use in production, used only in tests.

        Returns:
            List[ItemVersion]: Versions in the data store.
        """

    @abstractmethod
    def get_sync_sessions(self) -> "List[SyncSession]":
        """Returns all the sync sessions in the data store ordered by their start date. DO NOT use in production, used only in tests.

        Returns:
            List[SyncSession]: Sync sessions in the data store.
        """

    @abstractmethod
    def get_conflict_logs(self) -> "List[ConflictLog]":
        """Returns all conflict logs in the data store ordered by their date of creation. DO NOT use in production, used only in tests.

        Returns:
            List[ConflictLog]: Conflict logs in the data store.
        """

    @abstractmethod
    def _get_hashable_item(self, item: "Any") -> "Any":
        """Returns a hashable representation of an item.

        Args:
            item (Any): The item that needs hashing.

        Returns:
            Any: Hashable object.
        """

    def _get_raw_db(self) -> "Dict":
        """Returns a dictionary containing all the data in the data store. DO NOT use in production, used only in tests.

        Returns:
            Dict: Dictionary with all the data in the store.
        """
        conflict_logs = self.get_conflict_logs()
        item_changes = self.get_item_changes()
        item_versions = self.get_item_versions()
        items = self.get_items()
        sync_sessions = self.get_sync_sessions()

        db = {
            "conflict_logs": conflict_logs,
            "item_changes": item_changes,
            "item_versions": item_versions,
            "items": items,
            "sync_sessions": sync_sessions,
        }
        return db

    def show(self, items_only=False):  # pragma: no cover
        """Prints all the data in the store. DO NOT use in production, used only in tests.

        Args:
            items_only (bool, optional): Whether only the items should be printed.
        """
        raw_db = self._get_raw_db()
        if items_only:
            pprint(raw_db["items"])
        else:
            pprint(raw_db)

    def __eq__(self, other: "object"):
        assert isinstance(other, BaseDataStore)

        self_db = self._get_raw_db()
        other_db = other._get_raw_db()

        for key in self_db:
            if key in ["sync_sessions", "conflict_logs"]:
                continue
            if key == "items":
                vals = set(self._get_hashable_item(item) for item in self_db[key])
                other_vals = set(
                    other._get_hashable_item(item) for item in other_db[key]
                )
            elif key == "item_changes":

                attrs = [
                    "id",
                    "operation",
                    "item_id",
                    "provider_timestamp",
                    "provider_id",
                    "serialized_item",
                    "vector_clock",
                ]
                vals = set()
                for item in self_db[key]:
                    attr_vals = [getattr(item, attr) for attr in attrs]
                    attr_vals_immutable = tuple(attr_vals)
                    vals.add(attr_vals_immutable)

                other_vals = set()
                for item in other_db[key]:
                    attr_vals = [getattr(item, attr) for attr in attrs]
                    attr_vals_immutable = tuple(attr_vals)
                    other_vals.add(attr_vals_immutable)

            else:
                vals = set(self_db[key])
                other_vals = set(other_db[key])

            if vals != other_vals:
                return False

        return True
