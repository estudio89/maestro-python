from maestro.backends.base_nosql.converters import QueryMetadataConverter
from maestro.core.events import EventsManager
from maestro.core.execution import ChangesExecutor, ConflictResolver
from maestro.backends.mongo import (
    SyncSessionMetadataConverter,
    ItemVersionMetadataConverter,
    ItemChangeMetadataConverter,
    ConflictLogMetadataConverter,
    VectorClockMetadataConverter,
    VectorClockItemMetadataConverter,
    MongoDataStore,
    MongoItemSerializer,
    MongoSyncProvider,
    TrackedQueryMetadataConverter,
)
from pymongo import MongoClient
import datetime as dt


def create_mongo_store(
    client: MongoClient,
    database_name: "str",
    provider_id="mongo",
    sync_session_metadata_converter=SyncSessionMetadataConverter(),
    item_version_metadata_converter=None,
    item_change_metadata_converter=None,
    conflict_log_metadata_converter=ConflictLogMetadataConverter(),
    vector_clock_metadata_converter=None,
    item_serializer=MongoItemSerializer(),
    tracked_query_metadata_converter=None,
) -> MongoDataStore:

    if vector_clock_metadata_converter is None:
        vector_clock_metadata_converter = VectorClockMetadataConverter(VectorClockItemMetadataConverter())

    if item_version_metadata_converter is None:
        item_version_metadata_converter = ItemVersionMetadataConverter(
            vector_clock_converter=vector_clock_metadata_converter
        )

    if item_change_metadata_converter is None:
        item_change_metadata_converter = ItemChangeMetadataConverter(
            item_serializer=item_serializer,
            vector_clock_converter=vector_clock_metadata_converter,
        )

    if tracked_query_metadata_converter is None:
        tracked_query_metadata_converter = TrackedQueryMetadataConverter(
            vector_clock_converter=vector_clock_metadata_converter,
            query_converter=QueryMetadataConverter(),
        )

    data_store = MongoDataStore(
        local_provider_id=provider_id,
        sync_session_metadata_converter=sync_session_metadata_converter,
        item_version_metadata_converter=item_version_metadata_converter,
        item_change_metadata_converter=item_change_metadata_converter,
        conflict_log_metadata_converter=conflict_log_metadata_converter,
        vector_clock_metadata_converter=vector_clock_metadata_converter,
        tracked_query_metadata_converter=tracked_query_metadata_converter,
        item_serializer=item_serializer,
        db=client[database_name],
        client=client,
    )

    sync_session_metadata_converter.data_store = data_store
    item_version_metadata_converter.data_store = data_store
    conflict_log_metadata_converter.data_store = data_store

    return data_store


def create_mongo_provider(
    connect_uri: "str",
    database_name: "str",
    provider_id="mongo",
    max_changes_per_session=20,
    sync_session_metadata_converter=SyncSessionMetadataConverter(),
    item_version_metadata_converter=None,
    item_change_metadata_converter=None,
    conflict_log_metadata_converter=ConflictLogMetadataConverter(),
    vector_clock_metadata_converter=None,
    item_serializer=MongoItemSerializer(),
    events_manager_class=EventsManager,
    tracked_query_metadata_converter=None,
):  # pragma: no cover
    if vector_clock_metadata_converter is None:
        vector_clock_metadata_converter = VectorClockMetadataConverter(VectorClockItemMetadataConverter())

    if item_version_metadata_converter is None:
        item_version_metadata_converter = ItemVersionMetadataConverter(
            vector_clock_converter=vector_clock_metadata_converter
        )

    if item_change_metadata_converter is None:
        item_change_metadata_converter = ItemChangeMetadataConverter(
            item_serializer=item_serializer,
            vector_clock_converter=vector_clock_metadata_converter,
        )

    client = MongoClient(connect_uri, tz_aware=True, tzinfo=dt.timezone.utc)

    data_store = create_mongo_store(
        client=client,
        database_name=database_name,
        provider_id=provider_id,
        sync_session_metadata_converter=sync_session_metadata_converter,
        item_version_metadata_converter=item_version_metadata_converter,
        item_change_metadata_converter=item_change_metadata_converter,
        conflict_log_metadata_converter=conflict_log_metadata_converter,
        vector_clock_metadata_converter=vector_clock_metadata_converter,
        item_serializer=item_serializer,
        tracked_query_metadata_converter=tracked_query_metadata_converter
    )

    events_manager = events_manager_class(data_store=data_store)
    changes_executor = ChangesExecutor(
        data_store=data_store,
        events_manager=events_manager,
        conflict_resolver=ConflictResolver(),
    )
    provider = MongoSyncProvider(
        provider_id=provider_id,
        data_store=data_store,
        events_manager=events_manager,
        changes_executor=changes_executor,
        max_num=max_changes_per_session,
    )

    return provider
