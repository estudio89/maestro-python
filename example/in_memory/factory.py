from maestro.core.metadata import (
    SyncSession,
    ItemVersion,
    ItemChange,
    ConflictLog,
    VectorClock,
)
from maestro.core.metadata import SerializationResult
from maestro.core.execution import ChangesExecutor, ConflictResolver
from example.events import DebugEventsManager
from maestro.backends.in_memory import (
    InMemoryDataStore,
    InMemorySyncProvider,
    NullConverter,
    JSONSerializer,
)
from .types import TodoType
from .api_serializer import InMemoryAPISerializer


def create_provider(local_provider_id: "str"):
    # Provider 1
    data_store = InMemoryDataStore(
        local_provider_id=local_provider_id,
        sync_session_metadata_converter=NullConverter(metadata_class=SyncSession),
        item_version_metadata_converter=NullConverter(metadata_class=ItemVersion),
        item_change_metadata_converter=NullConverter(metadata_class=ItemChange),
        conflict_log_metadata_converter=NullConverter(metadata_class=ConflictLog),
        vector_clock_metadata_converter=NullConverter(metadata_class=VectorClock),
        item_serializer=JSONSerializer(),
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
