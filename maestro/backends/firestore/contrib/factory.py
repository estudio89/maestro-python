from maestro.core.events import EventsManager
from maestro.core.execution import ChangesExecutor, ConflictResolver
from maestro.backends.firestore import (
    SyncSessionMetadataConverter,
    ItemVersionMetadataConverter,
    ItemChangeMetadataConverter,
    ConflictLogMetadataConverter,
    VectorClockMetadataConverter,
    FirestoreDataStore,
    FirestoreItemSerializer,
    FirestoreSyncProvider,
)
from firebase_admin import firestore


def create_firestore_provider(
    provider_id="firestore",
    max_changes_per_session=20,
    sync_session_metadata_converter=SyncSessionMetadataConverter(),
    item_version_metadata_converter=ItemVersionMetadataConverter(),
    item_change_metadata_converter=None,
    conflict_log_metadata_converter=ConflictLogMetadataConverter(),
    vector_clock_metadata_converter=VectorClockMetadataConverter(),
    item_serializer=FirestoreItemSerializer(),
    events_manager_class=EventsManager,
):  # pragma: no cover

    if item_change_metadata_converter is None:
        item_change_metadata_converter = ItemChangeMetadataConverter(
            item_serializer=item_serializer
        )

    db = firestore.client()
    data_store = FirestoreDataStore(
        local_provider_id=provider_id,
        sync_session_metadata_converter=sync_session_metadata_converter,
        item_version_metadata_converter=item_version_metadata_converter,
        item_change_metadata_converter=item_change_metadata_converter,
        conflict_log_metadata_converter=conflict_log_metadata_converter,
        vector_clock_metadata_converter=vector_clock_metadata_converter,
        item_serializer=item_serializer,
        db=db,
    )

    sync_session_metadata_converter.data_store = data_store
    item_version_metadata_converter.data_store = data_store
    conflict_log_metadata_converter.data_store = data_store

    events_manager = events_manager_class(data_store=data_store)
    changes_executor = ChangesExecutor(
        data_store=data_store,
        events_manager=events_manager,
        conflict_resolver=ConflictResolver(),
    )
    firestore_provider = FirestoreSyncProvider(
        provider_id=provider_id,
        data_store=data_store,
        events_manager=events_manager,
        changes_executor=changes_executor,
        max_num=max_changes_per_session,
    )

    return firestore_provider
