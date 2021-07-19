from typing import TypedDict, Dict, List, Optional, Union
import datetime as dt
import enum


class CollectionType(enum.Enum):
    ITEM_CHANGES = "item_changes"
    ITEM_VERSIONS = "item_versions"
    CONFLICT_LOGS = "conflict_logs"
    SYNC_SESSIONS = "sync_sessions"
    PROVIDER_IDS = "provider_ids"


class VectorClockItemRecord(TypedDict):
    provider_id: "str"
    timestamp: "Union[dt.datetime, float]"


class ItemChangeRecord(TypedDict):
    id: "str"
    operation: "str"
    collection_name: "str"
    item_id: "str"
    date_created: "Union[dt.datetime, float]"
    provider_timestamp: "Union[dt.datetime, float]"
    provider_id: "str"
    insert_provider_timestamp: "Union[dt.datetime, float]"
    insert_provider_id: "str"
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
