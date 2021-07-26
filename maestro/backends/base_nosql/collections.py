from typing import TypedDict, Dict, List, Optional, Union, Literal, Any
import datetime as dt
import enum


class CollectionType(enum.Enum):
    ITEM_CHANGES = "item_changes"
    ITEM_VERSIONS = "item_versions"
    CONFLICT_LOGS = "conflict_logs"
    SYNC_SESSIONS = "sync_sessions"
    PROVIDER_IDS = "provider_ids"
    TRACKED_QUERIES = "tracked_queries"


class VectorClockItemRecord(TypedDict):
    provider_id: "str"
    timestamp: "Union[dt.datetime, float]"


class ItemChangeRecord(TypedDict):
    id: "str"
    operation: "str"
    collection_name: "str"
    item_id: "str"
    date_created: "Union[dt.datetime, float]"
    change_vector_clock_item: "VectorClockItemRecord"
    insert_vector_clock_item: "VectorClockItemRecord"
    serialized_item: "Dict"
    should_ignore: "bool"
    is_applied: "bool"
    vector_clock: "List[VectorClockItemRecord]"


class ItemVersionRecord(TypedDict):
    id: "str"
    date_created: "Union[dt.datetime, float]"
    current_item_change_id: "str"
    collection_name: "str"
    vector_clock: "List[VectorClockItemRecord]"


class ConflictLogRecord(TypedDict):
    id: "str"
    created_at: "Union[dt.datetime, float]"
    resolved_at: "Union[Optional[dt.datetime], float]"
    item_change_loser_id: "str"
    item_change_winner_id: "Optional[str]"
    status: "str"
    conflict_type: "str"
    description: "Optional[str]"


class SyncSessionRecord(TypedDict):
    id: "str"
    started_at: "Union[dt.datetime, float]"
    ended_at: "Union[Optional[dt.datetime], float]"
    status: "str"
    target_provider_id: "str"
    source_provider_id: "str"
    item_change_ids: "List[str]"
    query_id: "Optional[str]"

class ComparisonRecord(TypedDict):
    type: "Literal['comparison']"
    field_name: "str"
    comparator: "str"
    value: "Any"

class FilterRecord(TypedDict):
    type: "Literal['filter']"
    connector: "str"
    children: "List[Union[FilterRecord, ComparisonRecord]]" # type: ignore

class SortOrderRecord(TypedDict):
    field_name: "str"
    descending: "bool"

class QueryRecord(TypedDict):
    filter: "FilterRecord"
    ordering: "List[SortOrderRecord]"
    collection_name: "str"
    limit: "Optional[int]"
    offset: "Optional[int]"

class TrackedQueryRecord(TypedDict):
    id: "str"
    vector_clock: "List[VectorClockItemRecord]"
    query: "QueryRecord"

