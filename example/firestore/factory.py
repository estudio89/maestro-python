from maestro.core.execution import ChangesExecutor, ConflictResolver

from maestro.backends.firestore import (
    FirestoreDataStore,
    FirestoreSyncProvider,
    SyncSessionMetadataConverter,
    ItemVersionMetadataConverter,
    ItemChangeMetadataConverter,
    ConflictLogMetadataConverter,
    VectorClockMetadataConverter,
)
from example.events import DebugEventsManager
from example.base_nosql.api_serializer import NoSQLAPISerializer
from example.base_nosql.factory import NoSQLExampleSerializer
import os
import firebase_admin
from firebase_admin import credentials, firestore


class FirestoreExampleDataStore(FirestoreDataStore):
    def get_items(self):
        docs = self.db.collection("todos_todo").get()
        items = []
        for doc in docs:
            record = self._document_to_raw_instance(doc)
            record["collection_name"] = "todos_todo"
            items.append(record)
        return items


def create_provider(local_provider_id: "str"):
    # Firebase setup
    os.environ["FIRESTORE_EMULATOR_HOST"] = "0.0.0.0:7070"

    directory = os.path.dirname(__file__)
    cred = credentials.Certificate(
        os.path.join(directory, "firebase-project/serviceAccountKey.json")
    )
    firebase_admin.initialize_app(cred)

    db = firestore.client()

    # Dependency injection
    item_serializer = NoSQLExampleSerializer()
    sync_session_metadata_converter = SyncSessionMetadataConverter()
    item_version_metadata_converter = ItemVersionMetadataConverter()
    item_change_metadata_converter = ItemChangeMetadataConverter(
        item_serializer=item_serializer
    )
    conflict_log_metadata_converter = ConflictLogMetadataConverter()
    vector_clock_metadata_converter = VectorClockMetadataConverter()

    data_store = FirestoreExampleDataStore(
        local_provider_id=local_provider_id,
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

    events_manager = DebugEventsManager(data_store=data_store)
    changes_executor = ChangesExecutor(
        data_store=data_store,
        events_manager=events_manager,
        conflict_resolver=ConflictResolver(),
    )
    provider = FirestoreSyncProvider(
        provider_id=local_provider_id,
        data_store=data_store,
        events_manager=events_manager,
        changes_executor=changes_executor,
        max_num=10,
    )
    return provider, NoSQLAPISerializer()
