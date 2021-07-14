from maestro.core.utils import BaseMetadataConverter
from maestro.core.metadata import (
    SyncSession,
    ItemVersion,
    ItemChange,
    ConflictLog,
    ConflictStatus,
    ConflictType,
    VectorClock,
    VectorClockItem,
    SyncSessionStatus,
    Operation,
)
import datetime as dt
import uuid
from .utils import get_collection_name
from typing import List, TYPE_CHECKING, cast, Optional
from .collections import (
    SyncSessionRecord,
    ItemChangeRecord,
    ItemVersionRecord,
    ConflictLogRecord,
    VectorClockItemRecord,
)

if TYPE_CHECKING:
    from .store import FirestoreDataStore


class FirestoreConverter(BaseMetadataConverter):
    data_store: "FirestoreDataStore"


class SyncSessionMetadataConverter(FirestoreConverter):
    def to_metadata(self, record: "SyncSessionRecord") -> "SyncSession":
        item_changes = self.data_store.find_item_changes(ids=record["item_change_ids"])

        return SyncSession(
            id=uuid.UUID(record["id"]),
            started_at=record["started_at"],
            ended_at=record["ended_at"],
            status=SyncSessionStatus[record["status"]],
            source_provider_id=record["source_provider_id"],
            target_provider_id=record["target_provider_id"],
            item_changes=item_changes,
        )

    def to_record(self, metadata_object: "SyncSession") -> "SyncSessionRecord":
        item_change_ids = [
            str(item_change.id) for item_change in metadata_object.item_changes
        ]
        sync_session_record = SyncSessionRecord(
            id=str(metadata_object.id),
            started_at=metadata_object.started_at,
            ended_at=metadata_object.ended_at,
            status=metadata_object.status.value,
            source_provider_id=metadata_object.source_provider_id,
            target_provider_id=metadata_object.target_provider_id,
            item_change_ids=item_change_ids,
        )
        return sync_session_record


class ItemVersionMetadataConverter(FirestoreConverter):
    def to_metadata(self, record: "ItemVersionRecord") -> "ItemVersion":
        item_changes = self.data_store.find_item_changes(
            ids=[record["current_item_change_id"]]
        )
        item_change = item_changes[0]
        vector_clock_converter = VectorClockMetadataConverter()
        vector_clock = vector_clock_converter.to_metadata(record=record["vector_clock"])
        return ItemVersion(
            date_created=record["date_created"],
            current_item_change=item_change,
            item_id=record["id"],
            vector_clock=vector_clock,
        )

    def to_record(self, metadata_object: "ItemVersion") -> "ItemVersionRecord":
        vector_clock_converter = VectorClockMetadataConverter()
        vector_clock = vector_clock_converter.to_record(
            metadata_object=metadata_object.vector_clock
        )
        current_item_change = cast("ItemChange", metadata_object.current_item_change)
        collection_name = get_collection_name(current_item_change.serialized_item)
        return ItemVersionRecord(
            id=str(metadata_object.item_id),
            date_created=metadata_object.date_created,
            current_item_change_id=str(current_item_change.id),
            vector_clock=vector_clock,
            collection_name=collection_name,
        )


class ItemChangeMetadataConverter(BaseMetadataConverter):
    def to_metadata(self, record: "ItemChangeRecord") -> "ItemChange":
        vector_clock_converter = VectorClockMetadataConverter()
        vector_clock = vector_clock_converter.to_metadata(record=record["vector_clock"])
        metadata_object = ItemChange(
            id=uuid.UUID(record["id"]),
            date_created=record["date_created"],
            operation=Operation[record["operation"]],
            item_id=record["item_id"],
            provider_timestamp=record["provider_timestamp"],
            provider_id=record["provider_id"],
            insert_provider_timestamp=record["insert_provider_timestamp"],
            insert_provider_id=record["insert_provider_id"],
            serialized_item=record["serialized_item"],
            should_ignore=record["should_ignore"],
            is_applied=record["is_applied"],
            vector_clock=vector_clock,
        )
        return metadata_object

    def to_record(self, metadata_object: "ItemChange") -> "ItemChangeRecord":
        vector_clock_converter = VectorClockMetadataConverter()
        vector_clock = vector_clock_converter.to_record(
            metadata_object=metadata_object.vector_clock
        )
        collection_name = get_collection_name(metadata_object.serialized_item)
        return ItemChangeRecord(
            id=str(metadata_object.id),
            date_created=metadata_object.date_created,
            operation=metadata_object.operation.value,
            item_id=str(metadata_object.item_id),
            collection_name=collection_name,
            provider_timestamp=metadata_object.provider_timestamp,
            provider_id=metadata_object.provider_id,
            insert_provider_timestamp=metadata_object.insert_provider_timestamp,
            insert_provider_id=metadata_object.insert_provider_id,
            serialized_item=metadata_object.serialized_item,
            should_ignore=metadata_object.should_ignore,
            is_applied=metadata_object.is_applied,
            vector_clock=vector_clock,
        )


class ConflictLogMetadataConverter(FirestoreConverter):
    def to_metadata(self, record: "ConflictLogRecord") -> "ConflictLog":

        item_change_winner: "Optional[ItemChange]"

        if record.get("item_change_winner_id"):
            item_change_loser, item_change_winner = self.data_store.find_item_changes(
                ids=[
                    record["item_change_loser_id"],
                    cast("str", record["item_change_winner_id"]),
                ]
            )
        else:
            item_change_loser = self.data_store.find_item_changes(
                ids=[record["item_change_loser_id"]]
            )[0]
            item_change_winner = None

        return ConflictLog(
            id=uuid.UUID(record["id"]),
            created_at=record["created_at"],
            resolved_at=record["resolved_at"],
            item_change_loser=item_change_loser,
            item_change_winner=item_change_winner,
            status=ConflictStatus[record["status"]],
            conflict_type=ConflictType[record["conflict_type"]],
            description=record["description"],
        )

    def to_record(self, metadata_object: "ConflictLog") -> "ConflictLogRecord":
        return ConflictLogRecord(
            id=str(metadata_object.id),
            created_at=metadata_object.created_at,
            resolved_at=metadata_object.resolved_at,
            item_change_loser_id=str(metadata_object.item_change_loser.id),
            item_change_winner_id=str(metadata_object.item_change_winner.id)
            if metadata_object.item_change_winner is not None
            else None,
            status=metadata_object.status.value,
            conflict_type=metadata_object.conflict_type.value,
            description=metadata_object.description,
        )


class VectorClockMetadataConverter(BaseMetadataConverter):
    def to_metadata(self, record: "List[VectorClockItemRecord]") -> "VectorClock":
        vector_clock_items: "List[VectorClockItem]" = []
        for item in record:
            timestamp = item["timestamp"]
            timestamp = timestamp.replace(tzinfo=dt.timezone.utc)
            vector_clock_item = VectorClockItem(
                provider_id=item["provider_id"], timestamp=timestamp
            )
            vector_clock_items.append(vector_clock_item)

        vector_clock = VectorClock(*vector_clock_items)
        return vector_clock

    def to_record(
        self, metadata_object: "VectorClock"
    ) -> "List[VectorClockItemRecord]":
        items: "List[VectorClockItemRecord]" = []
        for vector_clock_item in metadata_object:
            items.append(
                {
                    "provider_id": vector_clock_item.provider_id,
                    "timestamp": vector_clock_item.timestamp,
                }
            )
        return items
