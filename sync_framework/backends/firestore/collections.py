from typing import TypedDict, List, Optional
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
    timestamp: "dt.datetime"

class ItemChangeRecord(TypedDict):
    id: "str"
    operation: "str"
    collection_name: "str"
    item_id: "str"
    date_created: "dt.datetime"
    provider_timestamp: "dt.datetime"
    provider_id: "str"
    insert_provider_timestamp: "dt.datetime"
    insert_provider_id: "str"
    serialized_item: "str"
    should_ignore: "bool"
    is_applied: "bool"
    vector_clock: "List[VectorClockItemRecord]"

class ItemVersionRecord(TypedDict):
    id: "str"
    date_created: "dt.datetime"
    current_item_change_id: "str"
    collection_name: "str"
    vector_clock: "List[VectorClockItemRecord]"

class ConflictLogRecord(TypedDict):
    id: "str"
    created_at: "dt.datetime"
    resolved_at: "Optional[dt.datetime]"
    item_change_loser_id: "str"
    item_change_winner_id: "Optional[str]"
    status: "str"
    conflict_type: "str"
    description: "Optional[str]"

class SyncSessionRecord(TypedDict):
    id: "str"
    started_at: "dt.datetime"
    ended_at: "Optional[dt.datetime]"
    status: "str"
    target_provider_id: "str"
    source_provider_id: "str"
    item_change_ids: "List[str]"
