from django.apps import apps
from maestro.core.utils import parse_datetime
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
    SerializationResult,
)
import datetime as dt
from .utils import entity_name_to_content_type, content_type_to_entity_name
from typing import List, Dict, TYPE_CHECKING, cast, Optional

if TYPE_CHECKING:  # pragma: no cover
    from .models import (
        SyncSessionRecord,
        ItemVersionRecord,
        ItemChangeRecord,
        ConflictLogRecord,
    )


class SyncSessionMetadataConverter(BaseMetadataConverter):
    def to_metadata(self, record: "SyncSessionRecord") -> "SyncSession":
        item_change_records = record.item_changes.all()
        item_changes: "List[ItemChange]" = []
        converter = ItemChangeMetadataConverter()

        for item_change_record in item_change_records:
            item_change = converter.to_metadata(record=item_change_record)
            item_changes.append(item_change)

        return SyncSession(
            id=record.id,
            started_at=record.started_at,
            ended_at=record.ended_at,
            status=SyncSessionStatus[record.status],
            source_provider_id=record.source_provider_id,
            target_provider_id=record.target_provider_id,
            item_changes=item_changes,
        )

    def to_record(self, metadata_object: "SyncSession") -> "SyncSessionRecord":
        SyncSessionRecord = apps.get_model("maestro", "SyncSessionRecord")
        sync_session_record = SyncSessionRecord(
            id=metadata_object.id,
            started_at=metadata_object.started_at,
            ended_at=metadata_object.ended_at,
            status=metadata_object.status.value,
            source_provider_id=metadata_object.source_provider_id,
            target_provider_id=metadata_object.target_provider_id,
        )
        item_change_records: "List[ItemChangeRecord]" = []
        converter = ItemChangeMetadataConverter()
        for item_change in metadata_object.item_changes:
            item_change_record = converter.to_record(metadata_object=item_change)
            item_change_records.append(item_change_record)

        sync_session_record._item_changes = item_change_records
        return sync_session_record


class ItemVersionMetadataConverter(BaseMetadataConverter):
    def to_metadata(self, record: "ItemVersionRecord") -> "ItemVersion":
        item_change_converter = ItemChangeMetadataConverter()
        item_change = item_change_converter.to_metadata(
            record=record.current_item_change
        )
        vector_clock_converter = VectorClockMetadataConverter()
        vector_clock = vector_clock_converter.to_metadata(record=record.vector_clock)
        return ItemVersion(
            date_created=record.date_created,
            current_item_change=item_change,
            item_id=str(record.id),
            vector_clock=vector_clock,
        )

    def to_record(self, metadata_object: "ItemVersion") -> "ItemVersionRecord":
        vector_clock_converter = VectorClockMetadataConverter()
        vector_clock = vector_clock_converter.to_record(
            metadata_object=metadata_object.vector_clock
        )
        current_item_change = cast("ItemChange", metadata_object.current_item_change)
        content_type = entity_name_to_content_type(
            current_item_change.serialization_result.entity_name
        )
        ItemVersionRecord = apps.get_model("maestro", "ItemVersionRecord")
        return ItemVersionRecord(
            id=metadata_object.item_id,
            current_item_change_id=current_item_change.id,
            vector_clock=vector_clock,
            content_type=content_type,
            date_created=metadata_object.date_created,
        )


class ItemChangeMetadataConverter(BaseMetadataConverter):
    def to_metadata(self, record: "ItemChangeRecord") -> "ItemChange":
        vector_clock_converter = VectorClockMetadataConverter()
        vector_clock = vector_clock_converter.to_metadata(record=record.vector_clock)
        change_vector_clock_item = VectorClockItem(
            provider_id=record.provider_id, timestamp=record.provider_timestamp
        )
        insert_vector_clock_item = VectorClockItem(
            provider_id=record.insert_provider_id,
            timestamp=record.insert_provider_timestamp,
        )
        entity_name = content_type_to_entity_name(record.content_type)
        serialization_result = SerializationResult(
            item_id=str(record.item_id),
            entity_name=entity_name,
            serialized_item=record.serialized_item,
        )
        metadata_object = ItemChange(
            id=record.id,
            date_created=record.date_created,
            operation=Operation[record.operation],
            change_vector_clock_item=change_vector_clock_item,
            insert_vector_clock_item=insert_vector_clock_item,
            serialization_result=serialization_result,
            should_ignore=record.should_ignore,
            is_applied=record.is_applied,
            vector_clock=vector_clock,
        )
        return metadata_object

    def to_record(self, metadata_object: "ItemChange") -> "ItemChangeRecord":
        ItemChangeRecord = apps.get_model("maestro", "ItemChangeRecord")
        vector_clock_converter = VectorClockMetadataConverter()
        vector_clock = vector_clock_converter.to_record(
            metadata_object=metadata_object.vector_clock
        )
        content_type = entity_name_to_content_type(
            metadata_object.serialization_result.entity_name
        )
        return ItemChangeRecord(
            id=metadata_object.id,
            date_created=metadata_object.date_created,
            operation=metadata_object.operation.value,
            item_id=metadata_object.serialization_result.item_id,
            content_type=content_type,
            provider_timestamp=metadata_object.change_vector_clock_item.timestamp,
            provider_id=metadata_object.change_vector_clock_item.provider_id,
            insert_provider_timestamp=metadata_object.insert_vector_clock_item.timestamp,
            insert_provider_id=metadata_object.insert_vector_clock_item.provider_id,
            serialized_item=metadata_object.serialization_result.serialized_item,
            should_ignore=metadata_object.should_ignore,
            is_applied=metadata_object.is_applied,
            vector_clock=vector_clock,
        )


class ConflictLogMetadataConverter(BaseMetadataConverter):
    def to_metadata(self, record: "ConflictLogRecord") -> "ConflictLog":
        item_change_converter = ItemChangeMetadataConverter()
        item_change_loser = item_change_converter.to_metadata(
            record=record.item_change_loser
        )
        item_change_winner: "Optional[ItemChange]"
        if record.item_change_winner_id:
            item_change_winner_record = cast(
                "ItemChangeRecord", record.item_change_winner
            )
            item_change_winner = item_change_converter.to_metadata(
                record=item_change_winner_record
            )
        else:
            item_change_winner = None
        return ConflictLog(
            id=record.id,
            created_at=record.created_at,
            resolved_at=record.resolved_at,
            item_change_loser=item_change_loser,
            item_change_winner=item_change_winner,
            status=ConflictStatus[record.status],
            conflict_type=ConflictType[record.conflict_type],
            description=record.description,
        )

    def to_record(self, metadata_object: "ConflictLog") -> "ConflictLogRecord":
        ConflictLogRecord = apps.get_model("maestro", "ConflictLogRecord")
        return ConflictLogRecord(
            id=metadata_object.id,
            created_at=metadata_object.created_at,
            resolved_at=metadata_object.resolved_at,
            item_change_loser_id=metadata_object.item_change_loser.id,
            item_change_winner_id=metadata_object.item_change_winner.id
            if metadata_object.item_change_winner is not None
            else None,
            status=metadata_object.status.value,
            conflict_type=metadata_object.conflict_type.value,
            description=metadata_object.description,
        )


class VectorClockMetadataConverter(BaseMetadataConverter):
    def to_metadata(self, record: "List[Dict]") -> "VectorClock":
        vector_clock_items: "List[VectorClockItem]" = []
        for item in record:
            timestamp = parse_datetime(value=item["timestamp"])
            timestamp = timestamp.replace(tzinfo=dt.timezone.utc)
            vector_clock_item = VectorClockItem(
                provider_id=item["provider_id"], timestamp=timestamp
            )
            vector_clock_items.append(vector_clock_item)

        vector_clock = VectorClock(*vector_clock_items)
        return vector_clock

    def to_record(self, metadata_object: "VectorClock") -> "List[Dict]":
        items = []
        for vector_clock_item in metadata_object:
            items.append(
                {
                    "provider_id": vector_clock_item.provider_id,
                    "timestamp": vector_clock_item.timestamp.isoformat(),
                }
            )
        return items
