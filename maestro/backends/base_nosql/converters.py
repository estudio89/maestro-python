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
    TrackedQuery,
)
from maestro.backends.base_nosql.utils import (
    collection_to_entity_name,
    entity_name_to_collection,
)
from maestro.core.query import (
    Query,
    SortOrder,
    Filter,
    Comparison,
    Comparator,
    Connector,
)
from maestro.core.utils import cast_away_optional

import datetime as dt
import uuid
from .utils import get_collection_name
from .serializer import NoSQLItemSerializer
from typing import List, TYPE_CHECKING, Optional, Any, Type, Union, cast
from .collections import (
    SyncSessionRecord,
    ItemChangeRecord,
    ItemVersionRecord,
    ConflictLogRecord,
    VectorClockItemRecord,
    TrackedQueryRecord,
    QueryRecord,
    FilterRecord,
    SortOrderRecord,
    ComparisonRecord,
)

if TYPE_CHECKING:
    from .store import NoSQLDataStore


class DateConverter:
    def serialize_date(self, value: "Optional[dt.datetime]") -> "Optional[Any]":
        return value

    def deserialize_date(self, value: "Optional[Any]") -> "Optional[dt.datetime]":
        return value


class NoSQLConverter(BaseMetadataConverter):
    date_converter: "DateConverter"
    date_converter_class: "Type[DateConverter]" = DateConverter

    def __init__(self):
        self.date_converter = self.date_converter_class()


class DataStoreAccessConverter(NoSQLConverter):
    data_store: "NoSQLDataStore"


class SyncSessionMetadataConverter(DataStoreAccessConverter):
    def to_metadata(self, record: "SyncSessionRecord") -> "SyncSession":
        item_changes = self.data_store.find_item_changes(ids=record["item_change_ids"])

        return SyncSession(
            id=uuid.UUID(record["id"]),
            started_at=cast_away_optional(
                self.date_converter.deserialize_date(record["started_at"]),
            ),
            ended_at=self.date_converter.deserialize_date(record["ended_at"]),
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
            started_at=cast_away_optional(
                self.date_converter.serialize_date(metadata_object.started_at)
            ),
            ended_at=self.date_converter.serialize_date(metadata_object.ended_at),
            status=metadata_object.status.value,
            source_provider_id=metadata_object.source_provider_id,
            target_provider_id=metadata_object.target_provider_id,
            item_change_ids=item_change_ids,
        )
        return sync_session_record


class ConflictLogMetadataConverter(DataStoreAccessConverter):
    def to_metadata(self, record: "ConflictLogRecord") -> "ConflictLog":

        item_change_winner: "Optional[ItemChange]"

        if record.get("item_change_winner_id"):
            item_change_loser, item_change_winner = self.data_store.find_item_changes(
                ids=[
                    record["item_change_loser_id"],
                    cast_away_optional(record["item_change_winner_id"]),
                ],
            )
        else:
            item_change_loser = self.data_store.find_item_changes(
                ids=[record["item_change_loser_id"]]
            )[0]
            item_change_winner = None

        return ConflictLog(
            id=uuid.UUID(record["id"]),
            created_at=cast_away_optional(
                self.date_converter.deserialize_date(record["created_at"]),
            ),
            resolved_at=self.date_converter.deserialize_date(record["resolved_at"]),
            item_change_loser=item_change_loser,
            item_change_winner=item_change_winner,
            status=ConflictStatus[record["status"]],
            conflict_type=ConflictType[record["conflict_type"]],
            description=record["description"],
        )

    def to_record(self, metadata_object: "ConflictLog") -> "ConflictLogRecord":
        return ConflictLogRecord(
            id=str(metadata_object.id),
            created_at=cast_away_optional(
                self.date_converter.serialize_date(metadata_object.created_at)
            ),
            resolved_at=self.date_converter.serialize_date(metadata_object.resolved_at),
            item_change_loser_id=str(metadata_object.item_change_loser.id),
            item_change_winner_id=str(metadata_object.item_change_winner.id)
            if metadata_object.item_change_winner is not None
            else None,
            status=metadata_object.status.value,
            conflict_type=metadata_object.conflict_type.value,
            description=metadata_object.description,
        )


class VectorClockMetadataConverter(NoSQLConverter):
    def to_metadata(self, record: "List[VectorClockItemRecord]") -> "VectorClock":
        vector_clock_items: "List[VectorClockItem]" = []
        for item in record:
            timestamp = cast_away_optional(
                self.date_converter.deserialize_date(item["timestamp"])
            )
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
                    "timestamp": cast_away_optional(
                        self.date_converter.serialize_date(vector_clock_item.timestamp)
                    ),
                }
            )
        return items


class ItemVersionMetadataConverter(DataStoreAccessConverter):
    vector_clock_converter: "VectorClockMetadataConverter"

    def __init__(
        self,
        vector_clock_converter: "VectorClockMetadataConverter" = VectorClockMetadataConverter(),
    ):
        self.vector_clock_converter = vector_clock_converter
        super().__init__()

    def to_metadata(self, record: "ItemVersionRecord") -> "ItemVersion":
        item_changes = self.data_store.find_item_changes(
            ids=[record["current_item_change_id"]]
        )
        item_change = item_changes[0]
        vector_clock = self.vector_clock_converter.to_metadata(
            record=record["vector_clock"]
        )
        return ItemVersion(
            date_created=cast_away_optional(
                self.date_converter.deserialize_date(record["date_created"])
            ),
            current_item_change=item_change,
            item_id=record["id"],
            vector_clock=vector_clock,
        )

    def to_record(self, metadata_object: "ItemVersion") -> "ItemVersionRecord":

        vector_clock = self.vector_clock_converter.to_record(
            metadata_object=metadata_object.vector_clock
        )
        current_item_change = cast_away_optional(metadata_object.current_item_change)
        collection_name = get_collection_name(current_item_change.serialized_item)
        return ItemVersionRecord(
            id=str(metadata_object.item_id),
            date_created=cast_away_optional(
                self.date_converter.serialize_date(metadata_object.date_created)
            ),
            current_item_change_id=str(current_item_change.id),
            vector_clock=vector_clock,
            collection_name=collection_name,
        )


class ItemChangeMetadataConverter(DataStoreAccessConverter):
    item_serializer: "NoSQLItemSerializer"

    def __init__(
        self,
        item_serializer: "NoSQLItemSerializer",
        vector_clock_converter: "VectorClockMetadataConverter" = VectorClockMetadataConverter(),
    ):
        self.item_serializer = item_serializer
        self.vector_clock_converter = vector_clock_converter
        super().__init__()

    def to_metadata(self, record: "ItemChangeRecord") -> "ItemChange":
        vector_clock = self.vector_clock_converter.to_metadata(
            record=record["vector_clock"]
        )
        serialized_item = self.item_serializer.serialize_item(
            item=record["serialized_item"]
        )
        metadata_object = ItemChange(
            id=uuid.UUID(record["id"]),
            date_created=cast_away_optional(
                self.date_converter.deserialize_date(record["date_created"])
            ),
            operation=Operation[record["operation"]],
            item_id=record["item_id"],
            provider_timestamp=cast_away_optional(
                self.date_converter.deserialize_date(record["provider_timestamp"])
            ),
            provider_id=record["provider_id"],
            insert_provider_timestamp=cast_away_optional(
                self.date_converter.deserialize_date(
                    record["insert_provider_timestamp"]
                )
            ),
            insert_provider_id=record["insert_provider_id"],
            serialized_item=serialized_item,
            should_ignore=record["should_ignore"],
            is_applied=record["is_applied"],
            vector_clock=vector_clock,
        )
        return metadata_object

    def to_record(self, metadata_object: "ItemChange") -> "ItemChangeRecord":
        vector_clock = self.vector_clock_converter.to_record(
            metadata_object=metadata_object.vector_clock
        )
        collection_name = get_collection_name(metadata_object.serialized_item)
        deserialized_item = self.item_serializer.deserialize_item(
            serialized_item=metadata_object.serialized_item
        )
        return ItemChangeRecord(
            id=str(metadata_object.id),
            date_created=cast_away_optional(
                self.date_converter.serialize_date(metadata_object.date_created)
            ),
            operation=metadata_object.operation.value,
            item_id=str(metadata_object.item_id),
            collection_name=collection_name,
            provider_timestamp=cast_away_optional(
                self.date_converter.serialize_date(metadata_object.provider_timestamp)
            ),
            provider_id=metadata_object.provider_id,
            insert_provider_timestamp=cast_away_optional(
                self.date_converter.serialize_date(
                    metadata_object.insert_provider_timestamp
                )
            ),
            insert_provider_id=metadata_object.insert_provider_id,
            serialized_item=deserialized_item,
            should_ignore=metadata_object.should_ignore,
            is_applied=metadata_object.is_applied,
            vector_clock=vector_clock,
        )


class SortOrderConverter(NoSQLConverter):
    def to_metadata(self, record: "SortOrderRecord") -> "SortOrder":
        return SortOrder(
            field_name=record["field_name"], descending=record["descending"]
        )

    def to_record(self, metadata_object: "SortOrder") -> "SortOrderRecord":
        return SortOrderRecord(
            field_name=metadata_object.field_name, descending=metadata_object.descending
        )


class ComparisonMetadataConverter(NoSQLConverter):
    def to_metadata(self, record: "ComparisonRecord") -> "Comparison":
        return Comparison(
            field_name=record["field_name"],
            comparator=Comparator[record["comparator"]],
            value=record["value"],
        )

    def to_record(self, metadata_object: "Comparison") -> "ComparisonRecord":
        return ComparisonRecord(
            type="comparison",
            field_name=metadata_object.field_name,
            comparator=metadata_object.comparator.value,
            value=metadata_object.value,
        )


class FilterMetadataConverter(NoSQLConverter):
    def __init__(
        self,
        comparison_converter: "ComparisonMetadataConverter" = ComparisonMetadataConverter(),
    ):
        self.comparison_converter = comparison_converter

    def to_metadata(self, record: "FilterRecord") -> "Filter":
        children: "List[Union[Filter, Comparison]]" = []

        for child in record["children"]:
            if record["type"] == "filter":
                metadata_child = self.to_metadata(record=cast("FilterRecord", child))
            else:
                metadata_child = self.comparison_converter.to_metadata(record=child)

            children.append(metadata_child)

        return Filter(connector=Connector[record["connector"]], children=children)

    def to_record(self, metadata_object: "Filter") -> "FilterRecord":
        children: "List[Union[FilterRecord, ComparisonRecord]]" = []

        for child in metadata_object.children:
            record_child: "Union[FilterRecord, ComparisonRecord]"
            if isinstance(child, Filter):
                record_child = self.to_record(metadata_object=child)
            else:
                record_child = self.comparison_converter.to_record(
                    metadata_object=child
                )
            children.append(record_child)

        return FilterRecord(
            type="filter", connector=metadata_object.connector.value, children=children
        )


class QueryMetadataConverter(NoSQLConverter):
    def __init__(
        self,
        filter_converter: "FilterMetadataConverter" = FilterMetadataConverter(),
        sort_order_converter: "SortOrderConverter" = SortOrderConverter(),
    ):
        self.filter_converter = filter_converter
        self.sort_order_converter = sort_order_converter

    def to_metadata(self, record: "QueryRecord") -> "Query":
        entity_name = collection_to_entity_name(record["collection_name"])
        filter = self.filter_converter.to_metadata(record=record["filter"])

        ordering: "List[SortOrder]" = []
        for sort_order_record in record["ordering"]:
            ordering.append(
                self.sort_order_converter.to_metadata(record=sort_order_record)
            )

        query = Query(
            entity_name=entity_name,
            filter=filter,
            ordering=ordering,
            limit=record["limit"],
            offset=record["offset"],
        )
        return query

    def to_record(self, metadata_object: "Query") -> "QueryRecord":
        collection_name = entity_name_to_collection(metadata_object.entity_name)
        filter = self.filter_converter.to_record(metadata_object=metadata_object.filter)

        ordering: "List[SortOrderRecord]" = []
        for sort_order in metadata_object.ordering:
            ordering.append(
                self.sort_order_converter.to_record(metadata_object=sort_order)
            )

        return QueryRecord(
            filter=filter,
            ordering=ordering,
            collection_name=collection_name,
            limit=metadata_object.limit,
            offset=metadata_object.offset,
        )


class TrackedQueryMetadataConverter(NoSQLConverter):
    def __init__(
        self,
        vector_clock_converter: "VectorClockMetadataConverter" = VectorClockMetadataConverter(),
        query_converter: "QueryMetadataConverter" = QueryMetadataConverter(),
    ):
        self.vector_clock_converter = vector_clock_converter
        self.query_converter = query_converter

    def to_metadata(self, record: "TrackedQueryRecord") -> "TrackedQuery":
        vector_clock = self.vector_clock_converter.to_metadata(
            record=record["vector_clock"]
        )
        query = self.query_converter.to_metadata(record=record["query"])
        tracked_query = TrackedQuery(query=query, vector_clock=vector_clock)
        return tracked_query

    def to_record(self, metadata_object: "TrackedQuery") -> "TrackedQueryRecord":
        vector_clock = self.vector_clock_converter.to_record(
            metadata_object=metadata_object.vector_clock
        )
        query = self.query_converter.to_record(metadata_object=metadata_object.query)
        return TrackedQueryRecord(
            id=metadata_object.query.get_id(), vector_clock=vector_clock, query=query
        )
