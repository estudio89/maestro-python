import datetime as dt
import copy
from enum import Enum
import uuid
from typing import List, Optional, Dict, cast

class VectorClockItem:
    """Stores the timestamp from a specific provider"""

    provider_id: "str"
    timestamp: "dt.datetime"

    def __init__(self, provider_id: "str", timestamp: "dt.datetime"):
        self.provider_id = provider_id
        self.timestamp = timestamp

    def __repr__(self):  # pragma: no cover
        return f"VectorClockItem(provider_id='{self.provider_id}', timestamp={self.timestamp})"

    def __lt__(self, other: "VectorClockItem"):
        assert isinstance(other, VectorClockItem)

        assert (
            other.provider_id == self.provider_id
        ), f"Can't compare clocks from different providers ({self.provider_id}, {other.provider_id})"

        return self.timestamp < other.timestamp

    def __gt__(self, other: "VectorClockItem"):
        assert isinstance(other, VectorClockItem)

        assert (
            other.provider_id == self.provider_id
        ), f"Can't compare clocks from different providers ({self.provider_id}, {other.provider_id})"

        return self.timestamp > other.timestamp

    def __eq__(self, other: "object"):
        assert isinstance(other, VectorClockItem)

        assert (
            other.provider_id == self.provider_id
        ), f"Can't compare clocks from different providers ({self.provider_id}, {other.provider_id})"

        return self.timestamp == other.timestamp

    def __hash__(self):
        return hash((self.provider_id, self.timestamp))

    def is_empty(self):
        return self.timestamp == dt.datetime.min.replace(tzinfo=dt.timezone.utc)


class VectorClock:
    """Groups multiple VectorClockItems from different providers. This class represents the
    state of synchronization of a given provider at a specific instant."""

    _vector_clock_items_by_id: "Dict[str, VectorClockItem]"

    def __init__(self, *vector_clock_items: "VectorClockItem"):
        self._vector_clock_items_by_id = {}

        for vector_item in vector_clock_items:
            if vector_item.provider_id in self._vector_clock_items_by_id:
                raise ValueError(f"Duplicate provider ids! {vector_item.provider_id}")
            self._vector_clock_items_by_id[vector_item.provider_id] = vector_item

    def __repr__(self):  # pragma: no cover
        return (
            "VectorClock("
            + ", ".join(list(repr(v) for v in self._vector_clock_items_by_id.values()))
            + ")"
        )

    def __iter__(self):
        """Iterates the VectorClockItems"""

        return iter(self._vector_clock_items_by_id.values())

    def __eq__(self, other: "object"):
        """Compares two VectorClocks. For them to be equal, their VectorClockItems must be equal."""

        assert isinstance(other, VectorClock)

        for vector_item in self:
            other_vector_item = other.get_vector_clock_item(vector_item.provider_id)

            if vector_item.timestamp != other_vector_item.timestamp:
                return False

        for other_vector_item in other:
            vector_item = self.get_vector_clock_item(other_vector_item.provider_id)

            if vector_item.timestamp != other_vector_item.timestamp:
                return False

        return True

    def __hash__(self):
        return hash(
            tuple(
                [
                    vector_clock_item
                    for vector_clock_item in self._vector_clock_items_by_id.values()
                    if not vector_clock_item.is_empty()
                ]
            )
        )

    @classmethod
    def create_empty(cls, provider_ids: "List[str]") -> "VectorClock":
        """Initializes a VectorClock with the minimum timestamp for the given provider identifiers.

        Args:
            provider_ids (List[str]): List of provider identifiers.
        """

        vector_clock = VectorClock()
        for provider_id in provider_ids:
            vector_clock.get_vector_clock_item(provider_id=provider_id)

        return vector_clock

    def get_vector_clock_item(self, provider_id: "str") -> "VectorClockItem":

        """Returns a VectorClockItem matching the given provider identifier. If none is found,
        a new one is created with timestamp equal to dt.datetime.min.

        Args:
            provider_id (str): Provider identifier.

        Returns:
            VectorClockItem: The matching item.
        """

        vector_clock_item = self._vector_clock_items_by_id.get(provider_id)
        if vector_clock_item is None:
            vector_clock_item = VectorClockItem(
                provider_id=provider_id,
                timestamp=dt.datetime.min.replace(tzinfo=dt.timezone.utc),
            )
            self._vector_clock_items_by_id[provider_id] = vector_clock_item

        return vector_clock_item

    def update_vector_clock_item(
        self, provider_id: "str", timestamp: "dt.datetime"
    ) -> "VectorClockItem":
        """Updates the given provider's timestamp with the new timestamp only if the given timestamp is greater than the current one.

        Args:
            provider_id (str): Provider identifier.
            timestamp (dt.datetime): New timestamp.
        """

        vector_clock_item = self.get_vector_clock_item(provider_id)
        if vector_clock_item.timestamp < timestamp:
            vector_clock_item.timestamp = timestamp

        return vector_clock_item


class Operation(Enum):

    """Represents an operation that can be performed to an item."""

    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class ItemChange:
    """Represents a change performed to an item.

    Attributes:
        id (uuid.UUID): The change's primary key
        date_created (dt.datetime): The date that the change was saved to the data store.
        insert_provider_id (str): The identifier of the provider that first created the item being referenced by this change.
        insert_provider_timestamp (dt.datetime): The timestamp when the item that is referenced by this change was created.
        is_applied (bool): Indicates whether this change has already been applied to the item.
        item_id (str): The primary key of the item referenced by this change.
        operation (Operation): The operation that was performed with this change.
        provider_id (str): The identifier of the provider that created the change.
        provider_timestamp (dt.datetime): The timestamp of the provider that created the change.
        serialized_item (str): The serialized item.
        should_ignore (bool): Indicates whether this change should be ignored (will only be true if this change lost a conflict dispute).
        vector_clock (VectorClock): The synchronization clock at the time this change was created.
    """

    id: "uuid.UUID"
    date_created: "dt.datetime"
    operation: "Operation"
    item_id: "str"
    provider_timestamp: "dt.datetime"
    provider_id: "str"
    insert_provider_id: "str"
    insert_provider_timestamp: "dt.datetime"
    serialized_item: "str"
    should_ignore: "bool"
    is_applied: "bool"
    vector_clock: "VectorClock"

    def __init__(
        self,
        id: "uuid.UUID",
        date_created: "dt.datetime",
        operation: "Operation",
        item_id: "str",
        provider_timestamp: "dt.datetime",
        provider_id: "str",
        insert_provider_timestamp: "dt.datetime",
        insert_provider_id: "str",
        serialized_item: "str",
        should_ignore: "bool",
        is_applied: "bool",
        vector_clock: "VectorClock",
    ):
        """
        Args:
            id (uuid.UUID): The change's primary key
            date_created (dt.datetime): The date that the change was saved to the data store.
            insert_provider_id (str): The identifier of the provider that first created the item being referenced by this change.
            insert_provider_timestamp (dt.datetime): The timestamp when the item that is referenced by this change was created.
            is_applied (bool): Indicates whether this change has already been applied to the item.
            item_id (str): The primary key of the item referenced by this change.
            operation (Operation): The operation that was performed with this change.
            provider_id (str): The identifier of the provider that created the change.
            provider_timestamp (dt.datetime): The timestamp of the provider that created the change.
            serialized_item (str): The serialized item.
            should_ignore (bool): Indicates whether this change should be ignored (will only be true if this change lost a conflict dispute).
            vector_clock (VectorClock): The synchronization clock at the time this change was created.
        """
        self.id = id
        self.date_created = date_created
        self.operation = operation
        self.item_id = item_id
        self.provider_timestamp = provider_timestamp
        self.provider_id = provider_id
        self.insert_provider_timestamp = insert_provider_timestamp
        self.insert_provider_id = insert_provider_id
        self.insert_provider_id = provider_id
        self.insert_provider_id = provider_id
        self.serialized_item = serialized_item
        self.should_ignore = should_ignore
        self.is_applied = is_applied
        self.vector_clock = vector_clock

    def __repr__(self):  # pragma: no cover
        return f"ItemChange(id='{self.id}', operation={self.operation}, item_id={self.item_id}, provider_id={self.provider_id}, should_ignore={self.should_ignore}, is_applied={self.is_applied})"

    def reset_status(self):
        """Resets all the fields that only make sense locally to a provider that applied the change.
        This method is called before the change is sent to a remote provider."""
        self.is_applied = False
        self.should_ignore = False
        self.date_created = None

    def __eq__(self, other: "object"):
        assert isinstance(other, ItemChange)

        for param in [
            "id",
            "operation",
            "item_id",
            "provider_timestamp",
            "provider_id",
            "insert_provider_timestamp",
            "insert_provider_id",
            "serialized_item",
            "should_ignore",
            "is_applied",
            "vector_clock",
        ]:
            self_param = getattr(self, param)
            other_param = getattr(other, param)
            if param == "item_id":
                self_param = str(self_param)
                other_param = str(other_param)

            if not self_param == other_param:
                return False

        return True

    def __hash__(self):
        return hash(
            (
                getattr(self, param)
                for param in [
                    "id",
                    "operation",
                    "item_id",
                    "provider_timestamp",
                    "provider_id",
                    "serialized_item",
                    "should_ignore",
                    "is_applied",
                    "vector_clock",
                ]
            )
        )


class ItemChangeBatch:
    """Represents a page of changes. This class' goal is to group the changes being synced into smaller groups
    so that a synchronization session containing many changes is not performed all at once but in chunks.

    Attributes:
        item_changes (List[ItemChange]): List of changes contained in the batch.
        is_last_batch (bool): Indicates whether this is the last batch of items to be processed.
    """

    item_changes: "List[ItemChange]"
    is_last_batch: "bool"

    def __init__(self, item_changes: "List[ItemChange]", is_last_batch: "bool"):
        """
        Args:
            item_changes (List[ItemChange]): List of changes contained in the batch.
            is_last_batch (bool): Indicates whether this is the last batch of items to be processed.
        """
        self.item_changes = item_changes
        self.is_last_batch = is_last_batch

    def __repr__(self):  # pragma: no cover
        return f"ItemChangeBatch(item_changes=[...{len(self.item_changes)} changes], is_last_batch={self.is_last_batch})"

    def get_vector_clock_after_done(
        self, initial_vector_clock: "VectorClock"
    ) -> "VectorClock":
        """Retrieves the new VectorClock that the provider will have after applying all the changes contained in the batch.

        Args:
            initial_vector_clock (VectorClock): The VectorClock before the changes are applied.
        """
        final_vector_clock = copy.deepcopy(initial_vector_clock)
        for item_change in self.item_changes:
            final_vector_clock.update_vector_clock_item(
                provider_id=item_change.provider_id,
                timestamp=item_change.provider_timestamp,
            )

        return final_vector_clock

    def reset_status(self):
        """Resets all the local information contained in the changes in this batch before it is sent to a remote provider."""

        for item_change in self.item_changes:
            item_change.reset_status()

    def __eq__(self, other: "object"):
        assert isinstance(other, ItemChangeBatch)
        return (
            self.item_changes == other.item_changes
            and self.is_last_batch == other.is_last_batch
        )


class ItemVersion:
    """Represents the current version of an item: it links an item in the data store to the last change that was applied to it.

    Attributes:
        current_item_change (Optional[ItemChange]): The last change that was applied to the item. It will only be null if the item does not exist in the data store yet.
        item_id (str): The primary key of the item being referenced by this version.
        date_created (dt.datetime): The date this item was first added to this data store.
        vector_clock (VectorClock): The VectorClock of the last change applied to this item (equals the "vector_clock" attribute of "current_item_change").
    """

    current_item_change: "Optional[ItemChange]"
    item_id: "str"
    date_created: "dt.datetime"
    vector_clock: "VectorClock"

    def __init__(
        self,
        current_item_change: "Optional[ItemChange]",
        item_id: "str",
        date_created: "dt.datetime",
        vector_clock: "Optional[VectorClock]" = None,
    ):
        """
        Args:
            current_item_change (Optional[ItemChange]): The last change that was applied to the item. It will only be null if the item does not exist in the data store yet.
            item_id (str): The primary key of the item being referenced by this version.
            date_created (dt.datetime): The date this item was first added to this data store.
            vector_clock (VectorClock): The VectorClock of the last change applied to this item (equals the "vector_clock" attribute of "current_item_change").
        """
        assert current_item_change is not None or vector_clock is not None

        if current_item_change is not None and vector_clock is not None:
            assert vector_clock == current_item_change.vector_clock

        self.current_item_change = current_item_change
        self.item_id = item_id
        if not vector_clock:
            self.vector_clock = cast("ItemChange", current_item_change).vector_clock
        else:
            self.vector_clock = vector_clock
        self.date_created = date_created

    def __repr__(self):  # pragma: no cover
        return f"ItemVersion(item_id='{self.item_id}', current_item_change_id='{self.current_item_change.id if self.current_item_change else None}')"

    def __eq__(self, other: "object"):
        assert isinstance(other, ItemVersion)

        return self.current_item_change == other.current_item_change and str(
            self.item_id
        ) == str(other.item_id)

    def __hash__(self):
        return hash((self.current_item_change, self.item_id))


class ConflictStatus(Enum):
    """Represents the status of a conflict."""

    DEFERRED = "DEFERRED"
    RESOLVED = "RESOLVED"


class ConflictType(Enum):
    """Represents the type of conflict that was detected."""

    LOCAL_UPDATE_REMOTE_UPDATE = "LOCAL_UPDATE_REMOTE_UPDATE"
    LOCAL_UPDATE_REMOTE_DELETE = "LOCAL_UPDATE_REMOTE_DELETE"
    LOCAL_UPDATE_REMOTE_INSERT = "LOCAL_UPDATE_REMOTE_INSERT"
    LOCAL_INSERT_REMOTE_UPDATE = "LOCAL_INSERT_REMOTE_UPDATE"
    LOCAL_DELETE_REMOTE_UPDATE = "LOCAL_DELETE_REMOTE_UPDATE"
    LOCAL_DELETE_REMOTE_DELETE = "LOCAL_DELETE_REMOTE_DELETE"
    EXCEPTION_OCCURRED = "EXCEPTION_OCCURRED"


class ConflictLog:
    """Represents the occurrence of a conflict during a synchronization session.

    Attributes:
        id (uuid.UUID): This instance's primary key.
        created_at (dt.datetime): The date that this conflict was detected.
        resolved_at (Optional[dt.datetime]): The date when the conflict was resolved or None if the conflict's status is ConflictStatus.DEFERRED.
        item_change_loser (ItemChange): The change that lost the conflict or, in the case of a conflict of type ConflictType.EXCEPTION_OCCURRED, the change that caused the exception to be raised.
        item_change_winner (Optional[ItemChange]): The change that won the conflict or None, in the case of a conflict of type ConflictType.EXCEPTION_OCCURRED.
        status (ConflictStatus): The status of the conflict.
        conflict_type (ConflictType): The type of conflict.
        description (Optional[str]): This field will contain the stack trace in cause the type of conflict is ConflictType.EXCEPTION_OCURRED, otherwise it will be null.
    """

    id: "uuid.UUID"
    created_at: "dt.datetime"
    resolved_at: "Optional[dt.datetime]"
    item_change_loser: "ItemChange"
    item_change_winner: "Optional[ItemChange]"
    status: "ConflictStatus"
    conflict_type: "ConflictType"
    description: "Optional[str]"

    def __init__(
        self,
        id: "uuid.UUID",
        created_at: "dt.datetime",
        resolved_at: "Optional[dt.datetime]",
        item_change_loser: "ItemChange",
        item_change_winner: "Optional[ItemChange]",
        status: "ConflictStatus",
        conflict_type: "ConflictType",
        description: "Optional[str]",
    ):
        """
        Args:
            id (uuid.UUID): This instance's primary key.
            created_at (dt.datetime): The date that this conflict was detected.
            resolved_at (Optional[dt.datetime]): The date when the conflict was resolved or None if the conflict's status is ConflictStatus.DEFERRED.
            item_change_loser (ItemChange): The change that lost the conflict or, in the case of a conflict of type ConflictType.EXCEPTION_OCCURRED, the change that caused the exception to be raised.
            item_change_winner (Optional[ItemChange]): The change that won the conflict or None, in the case of a conflict of type ConflictType.EXCEPTION_OCCURRED.
            status (ConflictStatus): The status of the conflict.
            conflict_type (ConflictType): The type of conflict.
            description (Optional[str]): This field will contain the stack trace in cause the type of conflict is ConflictType.EXCEPTION_OCURRED, otherwise it will be null.
        """
        self.id = id
        self.created_at = created_at
        self.resolved_at = resolved_at
        self.item_change_loser = item_change_loser
        self.item_change_winner = item_change_winner
        self.status = status
        self.conflict_type = conflict_type
        self.description = description

    def __repr__(self):  # pragma: no cover
        return f"ConflictLog(id='{self.id}', item_change_loser_id='{self.item_change_loser.id}', item_change_winner_id='{self.item_change_winner.id if self.item_change_winner else None}', conflict_type={self.conflict_type}, status={self.status})"

    def __eq__(self, other: "object"):
        assert isinstance(other, ConflictLog)

        for param in [
            "id",
            "created_at",
            "resolved_at",
            "item_change_loser",
            "item_change_winner",
            "status",
            "conflict_type",
            "description",
        ]:
            if not getattr(self, param) == getattr(other, param):
                return False

        return True


class SyncSessionStatus(Enum):
    IN_PROGRESS = "IN_PROGRESS"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class SyncSession:
    """Represents a synchronization session. A session consists of the following stages:
    - Processing deferred changes
    - Retrieving data from the source provider
    - Sending data to the target provider.

    Attributes:
        id (uuid.UUID): This instance's primary key.
        started_at (str): The date when this session started.
        ended_at (Optional[dt.datetime]): The date when this session ended. It will only be None right after the session is created.
        status (SyncSessionStatus): The status of this session.
        source_provider_id (str): The source provider's identifier.
        target_provider_id (str): The target provider's identifier.
        item_changes (List[ItemChange]): The list of changes that were exchanged in this session (either sent or received).

    """

    id: "uuid.UUID"
    started_at: "dt.datetime"
    ended_at: "Optional[dt.datetime]"
    status: "SyncSessionStatus"
    source_provider_id: "str"
    target_provider_id: "str"
    item_changes: "List[ItemChange]"

    def __init__(
        self,
        id: "uuid.UUID",
        started_at: "dt.datetime",
        ended_at: "Optional[dt.datetime]",
        status: "SyncSessionStatus",
        source_provider_id: "str",
        target_provider_id: "str",
        item_changes: "List[ItemChange]",
    ):
        """
        Args:
            id (uuid.UUID): This instance's primary key.
            started_at (str): The date when this session started.
            ended_at (Optional[dt.datetime]): The date when this session ended. It will only be None right after the session is created.
            status (SyncSessionStatus): The status of this session.
            source_provider_id (str): The source provider's identifier.
            target_provider_id (str): The target provider's identifier.
            item_changes (List[ItemChange]): The list of changes that were exchanged in this session (either sent or received).
        """
        self.id = id
        self.started_at = started_at
        self.ended_at = ended_at
        self.status = status
        self.source_provider_id = source_provider_id
        self.target_provider_id = target_provider_id
        self.item_changes = item_changes

    def __repr__(self):  # pragma: no cover
        return f"SyncSession(id='{self.id}', started_at='{self.started_at}', ended_at='{self.ended_at}', status={self.status}, source_provider_id={self.source_provider_id}, target_provider_id={self.target_provider_id}, item_changes=[...{len(self.item_changes)} changes]"

    def __eq__(self, other: "object"):
        assert isinstance(other, SyncSession)

        for param in [
            "id",
            "started_at",
            "ended_at",
            "status",
            "source_provider_id",
            "target_provider_id",
            "item_changes",
        ]:
            if not getattr(self, param) == getattr(other, param):
                return False

        return True
