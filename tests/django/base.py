from django.apps import apps
from maestro.core.store import BaseDataStore
from maestro.core.utils import BaseSyncLock
from maestro.core.events import EventsManager
from maestro.core.exceptions import ItemNotFoundException
from maestro.core.execution import ChangesExecutor
from maestro.core.metadata import (
    SyncSession,
    ItemVersion,
    ItemChange,
    ConflictLog,
    VectorClock,
)
from maestro.core.provider import BaseSyncProvider
from maestro.backends.in_memory import (
    InMemoryDataStore,
    InMemorySyncProvider,
    NullConverter,
    JSONSerializer,
)

from maestro.backends.django import (
    DjangoDataStore,
    SyncSessionMetadataConverter,
    ItemVersionMetadataConverter,
    ItemChangeMetadataConverter,
    ConflictLogMetadataConverter,
    VectorClockMetadataConverter,
    DjangoItemSerializer,
    DjangoSyncProvider,
    DjangoSyncLock,
)
from typing import TYPE_CHECKING, Any, Optional, cast
import uuid
import tests.base

if TYPE_CHECKING:
    from tests.django.models import Item


class TestInMemoryDataStore(InMemoryDataStore):
    __test__ = False

    def _get_hashable_item(self, item):
        return item

    def update_item(self, item: "Optional[Item]", serialized_item: "str") -> "Item":
        serializer = DjangoItemSerializer()
        deserialized = serializer.deserialize_item(serialized_item=serialized_item)

        deserialized_item = cast("Item", deserialized)
        if item:
            item.version = deserialized_item.version
            item.name = deserialized_item.name
            return item
        else:
            return deserialized_item

    def serialize_item(self, item: "Item") -> "str":
        return (
            '{"fields": {"name": "%s", "version": "%s"}, "pk": "%s", "table_name": "my_app_item"}'
            % (item.name, item.version, str(item.id))
        )


class TestDjangoDataStore(DjangoDataStore):
    __test__ = False

    def get_items(self):
        Item = apps.get_model("my_app", "Item")
        return list(Item.objects.all())

    def get_item_by_id(self, id):
        try:
            Item = apps.get_model("my_app", "Item")
            return Item.objects.get(id=id)
        except Item.DoesNotExist:
            raise ItemNotFoundException(item_type="Item", id=id)


class DjangoBackendTestMixin(tests.base.BackendTestMixin):
    def _create_item(self, id: "str", name: "str", version: "str"):

        Item = apps.get_model("my_app", "Item")
        return Item(id=uuid.UUID(id), name=name, version=version)

    def _serialize_item(self, id: "str", name: "str", version: "str"):
        return (
            '{"fields": {"name": "%s", "version": "%s"}, "pk": "%s", "table_name": "my_app_item"}'
            % (name, version, str(id))
        )

    def _deserialize_item(self, id, name, version):
        return self._create_item(id=id, name=name, version=version)

    def _create_data_store(self, local_provider_id: "str") -> "BaseDataStore":
        if local_provider_id == "other_provider":
            return TestInMemoryDataStore(
                local_provider_id=local_provider_id,
                sync_session_metadata_converter=NullConverter(SyncSession),
                item_version_metadata_converter=NullConverter(ItemVersion),
                item_change_metadata_converter=NullConverter(ItemChange),
                conflict_log_metadata_converter=NullConverter(ConflictLog),
                vector_clock_metadata_converter=NullConverter(VectorClock),
                item_serializer=JSONSerializer(),
            )
        else:
            return TestDjangoDataStore(
                local_provider_id=local_provider_id,
                sync_session_metadata_converter=SyncSessionMetadataConverter(),
                item_version_metadata_converter=ItemVersionMetadataConverter(),
                item_change_metadata_converter=ItemChangeMetadataConverter(),
                conflict_log_metadata_converter=ConflictLogMetadataConverter(),
                vector_clock_metadata_converter=VectorClockMetadataConverter(),
                item_serializer=DjangoItemSerializer(),
            )

    def _create_provider(
        self,
        provider_id: "str",
        data_store: "BaseDataStore",
        events_manager: "EventsManager",
        changes_executor: "ChangesExecutor",
        max_num: "int",
    ) -> "BaseSyncProvider":
        if provider_id == "other_provider":
            return InMemorySyncProvider(
                provider_id=provider_id,
                data_store=data_store,
                events_manager=events_manager,
                changes_executor=changes_executor,
                max_num=max_num,
            )
        else:
            return DjangoSyncProvider(
                provider_id=provider_id,
                data_store=data_store,
                events_manager=events_manager,
                changes_executor=changes_executor,
                max_num=max_num,
            )

    def _create_sync_lock(self) -> "BaseSyncLock":
        return DjangoSyncLock()

    def _get_id(self, item):
        return item.id

    def _add_item_change(self, item_change: "ItemChange"):

        vector_clock = [
            {"provider_id": val.provider_id, "timestamp": val.timestamp.isoformat()}
            for val in item_change.vector_clock
        ]
        ContentType = apps.get_model("contenttypes", "ContentType")
        Item = apps.get_model("my_app", "Item")
        content_type = ContentType.objects.get_for_model(Item)
        ItemChangeRecord = apps.get_model("maestro", "ItemChangeRecord")
        return ItemChangeRecord.objects.create(
            id=item_change.id,
            date_created=item_change.date_created,
            operation=item_change.operation.value,
            item_id=item_change.item_id,
            content_type=content_type,
            provider_timestamp=item_change.provider_timestamp,
            provider_id=item_change.provider_id,
            insert_provider_timestamp=item_change.insert_provider_timestamp,
            insert_provider_id=item_change.insert_provider_id,
            serialized_item=item_change.serialized_item,
            should_ignore=item_change.should_ignore,
            is_applied=item_change.is_applied,
            vector_clock=vector_clock,
        )

    def _add_item_version(self, item_version: "ItemVersion"):
        vector_clock = [
            {"provider_id": val.provider_id, "timestamp": val.timestamp.isoformat()}
            for val in item_version.vector_clock
        ]
        ContentType = apps.get_model("contenttypes", "ContentType")
        Item = apps.get_model("my_app", "Item")
        content_type = ContentType.objects.get_for_model(Item)
        ItemVersionRecord = apps.get_model("maestro", "ItemVersionRecord")
        current_item_change = cast("ItemChange", item_version.current_item_change)
        return ItemVersionRecord.objects.create(
            id=item_version.item_id,
            current_item_change_id=current_item_change.id,
            vector_clock=vector_clock,
            content_type=content_type,
            date_created=item_version.date_created,
        )
        self.data_store._db["item_versions"].append(item_version.__dict__)

    def _add_conflict_log(self, conflict_log: "ConflictLog"):
        ConflictLogRecord = apps.get_model("maestro", "ConflictLogRecord")
        return ConflictLogRecord.objects.create(
            id=conflict_log.id,
            created_at=conflict_log.created_at,
            resolved_at=conflict_log.resolved_at,
            item_change_loser_id=conflict_log.item_change_loser.id,
            item_change_winner_id=conflict_log.item_change_winner.id
            if conflict_log.item_change_winner is not None
            else None,
            status=conflict_log.status.value,
            conflict_type=conflict_log.conflict_type.value,
            description=conflict_log.description,
        )

    def _add_item(self, item: "Any"):
        item.save()
