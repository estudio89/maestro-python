from django.db import models, transaction
from sync_framework.core.store import BaseDataStore
from sync_framework.core.exceptions import ItemNotFoundException
from sync_framework.core.metadata import (
    VectorClock,
    ItemVersion,
    ItemChange,
    ItemChangeBatch,
    Operation,
    ConflictLog,
    ConflictStatus,
    SyncSession,
)
from django.apps import apps
from .converters import (
    SyncSessionMetadataConverter,
    ItemVersionMetadataConverter,
    ItemChangeMetadataConverter,
    ConflictLogMetadataConverter,
    VectorClockMetadataConverter,
)
from .serializer import DjangoItemSerializer
from typing import Optional, Any, Callable, List
import uuid
import operator
from functools import reduce


class DjangoDataStore(BaseDataStore):
    sync_session_metadata_converter: "SyncSessionMetadataConverter"
    item_version_metadata_converter: "ItemVersionMetadataConverter"
    item_change_metadata_converter: "ItemChangeMetadataConverter"
    conflict_log_metadata_converter: "ConflictLogMetadataConverter"
    vector_clock_metadata_converter: "VectorClockMetadataConverter"
    item_serializer: "DjangoItemSerializer"

    def get_local_vector_clock(self) -> "VectorClock":
        vector_clock = VectorClock.create_empty(provider_ids=[self.local_provider_id])
        ItemChangeRecord = apps.get_model("sync_framework", "ItemChangeRecord")
        provider_ids = ItemChangeRecord.objects.values_list(
            "provider_id", flat=True
        ).distinct()
        for provider_id in provider_ids:
            ItemChangeRecord = apps.get_model("sync_framework", "ItemChangeRecord")
            max_timestamp = ItemChangeRecord.objects.filter(
                provider_id=provider_id
            ).aggregate(max_timestamp=models.Max("provider_timestamp"))
            if max_timestamp["max_timestamp"]:
                vector_clock.update_vector_clock_item(
                    provider_id=provider_id, timestamp=max_timestamp["max_timestamp"]
                )
        return vector_clock

    def get_item_version(self, item_id: "str") -> "Optional[ItemVersion]":
        ItemVersionRecord = apps.get_model("sync_framework", "ItemVersionRecord")
        try:
            item_version_record = ItemVersionRecord.objects.get(id=item_id)
            item_version = self.item_version_metadata_converter.to_metadata(
                record=item_version_record
            )
            return item_version
        except ItemVersionRecord.DoesNotExist:
            return None

    def get_item_change_by_id(self, id: "uuid.UUID") -> "ItemChange":
        ItemChangeRecord = apps.get_model("sync_framework", "ItemChangeRecord")
        try:
            item_change_record = ItemChangeRecord.objects.get(id=id)
            item_change = self.item_change_metadata_converter.to_metadata(
                record=item_change_record
            )
            return item_change
        except ItemChangeRecord.DoesNotExist:
            raise ItemNotFoundException(item_type="ItemChangeRecord", id=str(id))

    def _paginate_item_change_records(
        self, queryset, max_num: "int"
    ) -> "ItemChangeBatch":
        total_count = queryset.count()
        item_change_records = list(queryset[:max_num])
        is_last_batch = total_count == len(item_change_records)

        item_changes = []
        for item_change_record in item_change_records:
            item_change = self.item_change_metadata_converter.to_metadata(
                record=item_change_record
            )
            item_changes.append(item_change)

        item_change_batch = ItemChangeBatch(
            item_changes=item_changes, is_last_batch=is_last_batch
        )
        return item_change_batch

    def select_changes(
        self, vector_clock: "VectorClock", max_num: "int"
    ) -> "ItemChangeBatch":
        ItemChangeRecord = apps.get_model("sync_framework", "ItemChangeRecord")
        provider_ids = ItemChangeRecord.objects.values_list(
            "provider_id", flat=True
        ).distinct()
        fkwargs = []
        for provider_id in provider_ids:
            vector_clock_item = vector_clock.get_vector_clock_item(
                provider_id=provider_id
            )
            provider_filter = models.Q(
                provider_id=vector_clock_item.provider_id,
                provider_timestamp__gt=vector_clock_item.timestamp,
            )
            fkwargs.append(provider_filter)

        if fkwargs:
            queryset = ItemChangeRecord.objects.filter(
                reduce(operator.or_, fkwargs)
            ).order_by("date_created")
        else:
            queryset = ItemChangeRecord.objects.none()

        item_change_batch = self._paginate_item_change_records(
            queryset=queryset, max_num=max_num
        )
        return item_change_batch

    def select_deferred_changes(
        self, vector_clock: "VectorClock", max_num: "int"
    ) -> "ItemChangeBatch":
        fkwargs = []
        for vector_clock_item in vector_clock:
            provider_filter = models.Q(
                provider_id=vector_clock_item.provider_id,
                provider_timestamp__gt=vector_clock_item.timestamp,
                is_applied=False,
                lost_conflicts__status=ConflictStatus.DEFERRED.value,
            )
            fkwargs.append(provider_filter)

        ItemChangeRecord = apps.get_model("sync_framework", "ItemChangeRecord")
        queryset = ItemChangeRecord.objects.filter(
            reduce(operator.or_, fkwargs)
        ).order_by("date_created")
        item_change_batch = self._paginate_item_change_records(
            queryset=queryset, max_num=max_num
        )
        return item_change_batch

    def save_item_change(
        self, item_change: "ItemChange", is_creating: "bool" = False
    ) -> "ItemChange":
        item_change_record = self.item_change_metadata_converter.to_record(
            metadata_object=item_change
        )
        item_change_record.save()

        return item_change

    def save_item(self, item: "models.Model"):
        item.save()

    def delete_item(self, item: "models.Model"):
        item.delete()

    def run_in_transaction(self, item_change: "ItemChange", callback: "Callable"):
        item = self.deserialize_item(serialized_item=item_change.serialized_item)
        Model = item._meta.model

        with transaction.atomic():
            Model.objects.select_for_update().filter(id=item.id)
            callback()

    def save_conflict_log(self, conflict_log: "ConflictLog"):
        conflict_log_record = self.conflict_log_metadata_converter.to_record(
            metadata_object=conflict_log
        )
        conflict_log_record.save()

    def execute_item_change(self, item_change: "ItemChange"):
        item = self.deserialize_item(serialized_item=item_change.serialized_item)

        if item_change.operation == Operation.DELETE:
            item.delete()
        else:
            item.save()

    def save_item_version(self, item_version: "ItemVersion"):
        item_version_record = self.item_version_metadata_converter.to_record(
            metadata_object=item_version
        )
        item_version_record.save()

    def get_deferred_conflict_logs(
        self, item_change_loser: "ItemChange"
    ) -> "List[ConflictLog]":
        ConflictLogRecord = apps.get_model("sync_framework", "ConflictLogRecord")
        conflict_log_records = list(
            ConflictLogRecord.objects.filter(
                item_change_loser_id=item_change_loser.id,
                status=ConflictStatus.DEFERRED.value,
            )
        )
        conflict_logs = []
        for conflict_log_record in conflict_log_records:
            conflict_log = self.conflict_log_metadata_converter.to_metadata(
                record=conflict_log_record
            )
            conflict_logs.append(conflict_log)

        return conflict_logs

    def save_sync_session(self, sync_session: "SyncSession"):
        with transaction.atomic():
            sync_session_record = self.sync_session_metadata_converter.to_record(
                metadata_object=sync_session
            )
            sync_session_record.save()

        sync_session_record.item_changes.add(*sync_session_record._item_changes)

    def _get_hashable_item(self, item: "Any"):
        return item

    def get_item_changes(self):
        ItemChangeRecord = apps.get_model("sync_framework", "ItemChangeRecord")
        records = list(ItemChangeRecord.objects.order_by("date_created"))
        metadata_objects = []
        converter = ItemChangeMetadataConverter()
        for record in records:
            metadata_object = converter.to_metadata(record)
            metadata_objects.append(metadata_object)
        return metadata_objects

    def get_item_versions(self):
        ItemVersionRecord = apps.get_model("sync_framework", "ItemVersionRecord")
        records = list(ItemVersionRecord.objects.order_by("date_created"))
        metadata_objects = []
        converter = ItemVersionMetadataConverter()
        for record in records:
            metadata_object = converter.to_metadata(record)
            metadata_objects.append(metadata_object)
        return metadata_objects

    def get_sync_sessions(self):
        SyncSessionRecord = apps.get_model("sync_framework", "SyncSessionRecord")
        records = list(SyncSessionRecord.objects.order_by("started_at"))
        metadata_objects = []
        converter = SyncSessionMetadataConverter()
        for record in records:
            metadata_object = converter.to_metadata(record)
            metadata_objects.append(metadata_object)
        return metadata_objects

    def get_conflict_logs(self):
        ConflictLogRecord = apps.get_model("sync_framework", "ConflictLogRecord")
        records = list(ConflictLogRecord.objects.order_by("created_at"))
        metadata_objects = []
        converter = ConflictLogMetadataConverter()
        for record in records:
            metadata_object = converter.to_metadata(record)
            metadata_objects.append(metadata_object)
        return metadata_objects
