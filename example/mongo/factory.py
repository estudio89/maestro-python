from maestro.core.execution import ChangesExecutor, ConflictResolver
from maestro.backends.mongo import (
    MongoDataStore,
    MongoSyncProvider,
    SyncSessionMetadataConverter,
    ItemVersionMetadataConverter,
    ItemChangeMetadataConverter,
    ConflictLogMetadataConverter,
    VectorClockMetadataConverter,
)
from example.base_nosql.api_serializer import NoSQLAPISerializer
from example.base_nosql.factory import NoSQLExampleSerializer
from example.events import DebugEventsManager
from pymongo import MongoClient
import datetime as dt


class MongoExampleDataStore(MongoDataStore):
    def get_items(self):
        docs = self.db["todos_todo"].find(filter={})
        items = []
        for doc in docs:
            record = self._document_to_raw_instance(doc)
            record["collection_name"] = "todos_todo"
            items.append(record)
        return items


def create_provider(local_provider_id: "str"):
    # Mongo setup
    connect_uri = "mongodb://maestro:maestro@localhost:27000"
    client = MongoClient(connect_uri, tz_aware=True, tzinfo=dt.timezone.utc)
    db = client.example_db

    # Dependency injection
    item_serializer = NoSQLExampleSerializer()
    sync_session_metadata_converter = SyncSessionMetadataConverter()
    conflict_log_metadata_converter = ConflictLogMetadataConverter()
    vector_clock_metadata_converter = VectorClockMetadataConverter()
    item_version_metadata_converter = ItemVersionMetadataConverter(
        vector_clock_converter=vector_clock_metadata_converter
    )
    item_change_metadata_converter = ItemChangeMetadataConverter(
        item_serializer=item_serializer,
        vector_clock_converter=vector_clock_metadata_converter,
    )

    data_store = MongoExampleDataStore(
        local_provider_id=local_provider_id,
        sync_session_metadata_converter=sync_session_metadata_converter,
        item_version_metadata_converter=item_version_metadata_converter,
        item_change_metadata_converter=item_change_metadata_converter,
        conflict_log_metadata_converter=conflict_log_metadata_converter,
        vector_clock_metadata_converter=vector_clock_metadata_converter,
        item_serializer=item_serializer,
        db=db,
        client=client,
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
    provider = MongoSyncProvider(
        provider_id=local_provider_id,
        data_store=data_store,
        events_manager=events_manager,
        changes_executor=changes_executor,
        max_num=10,
    )
    return provider, NoSQLAPISerializer()
