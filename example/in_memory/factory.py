from maestro.core.metadata import (
    SyncSession,
    ItemVersion,
    ItemChange,
    ConflictLog,
    VectorClock,
)
from maestro.core.serializer import BaseItemSerializer
from maestro.core.execution import ChangesExecutor, ConflictResolver
from example.events import DebugEventsManager
from maestro.backends.in_memory import (
    InMemoryDataStore,
    InMemorySyncProvider,
    NullConverter,
)
from .types import TodoType
from .api_serializer import InMemoryAPISerializer
from typing import List
import json


class InMemoryExampleSerializer(BaseItemSerializer):
    def serialize_item(self, item: "TodoType") -> "str":

        serialized = {
            "fields": {
                "date_created": item["date_created"],
                "done": item["done"],
                "text": item["text"],
            },
            "pk": item["id"],
            "entity_name": "todos_todo",
        }

        serialized = dict(sorted(serialized.items()))
        return json.dumps(serialized)

    def deserialize_item(self, serialized_item: "str") -> "TodoType":
        data = json.loads(serialized_item)
        deserialized = {
            "id": data["pk"],
            "text": data["fields"]["text"],
            "done": data["fields"]["done"],
            "date_created": data["fields"]["date_created"],
        }

        return deserialized


def create_provider(local_provider_id: "str"):
    # Provider 1
    data_store = InMemoryDataStore(
        local_provider_id=local_provider_id,
        sync_session_metadata_converter=NullConverter(metadata_class=SyncSession),
        item_version_metadata_converter=NullConverter(metadata_class=ItemVersion),
        item_change_metadata_converter=NullConverter(metadata_class=ItemChange),
        conflict_log_metadata_converter=NullConverter(metadata_class=ConflictLog),
        vector_clock_metadata_converter=NullConverter(metadata_class=VectorClock),
        item_serializer=InMemoryExampleSerializer(),
    )
    events_manager = DebugEventsManager(data_store=data_store)
    changes_executor = ChangesExecutor(
        data_store=data_store,
        events_manager=events_manager,
        conflict_resolver=ConflictResolver(),
    )
    provider = InMemorySyncProvider(
        provider_id=local_provider_id,
        data_store=data_store,
        events_manager=events_manager,
        changes_executor=changes_executor,
        max_num=10,
    )
    return provider, InMemoryAPISerializer()
