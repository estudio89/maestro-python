from django.db import models
from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes.fields import GenericForeignKey
from typing import List

from sync_framework.core.metadata import (
    SyncSessionStatus,
    Operation,
    ConflictStatus,
    ConflictType,
)


class ItemChangeRecord(models.Model):
    id = models.UUIDField(primary_key=True)
    date_created = models.DateTimeField(null=False)
    operation = models.CharField(
        max_length=100,
        choices=(
            (Operation.DELETE.value, "Delete"),
            (Operation.INSERT.value, "Insert"),
            (Operation.UPDATE.value, "Update"),
        ),
    )
    content_type = models.ForeignKey(ContentType, null=False, on_delete=models.CASCADE)
    item_id = models.UUIDField(null=False)
    item = GenericForeignKey("content_type", "item_id")
    provider_timestamp = models.DateTimeField(null=False)
    provider_id = models.CharField(null=False, max_length=200)
    insert_provider_timestamp = models.DateTimeField(null=False)
    insert_provider_id = models.CharField(null=False, max_length=150)
    serialized_item = models.TextField(null=False)
    should_ignore = models.BooleanField(null=False)
    is_applied = models.BooleanField(null=False)
    vector_clock = models.JSONField(
        default=list, null=False
    )  # [{"timestamp": "<timestamp in ISO 8601>", "provider_id": "<provider_id>"}]

    class Meta:
        ordering = ["date_created"]


class SyncLockRecord(models.Model):
    key = models.TextField()


class ItemVersionRecord(models.Model):
    id = models.UUIDField(primary_key=True)
    date_created = models.DateTimeField(null=False)
    current_item_change = models.ForeignKey(ItemChangeRecord, on_delete=models.PROTECT)
    content_type = models.ForeignKey(ContentType, null=False, on_delete=models.CASCADE)
    item = GenericForeignKey("content_type", "id")
    vector_clock = models.JSONField(
        default=list, null=False
    )  # [{"timestamp": "<timestamp in ISO 8601>", "provider_id": "<provider_id>"}]

    class Meta:
        ordering = ["date_created"]


class ConflictLogRecord(models.Model):
    id = models.UUIDField(primary_key=True)
    created_at = models.DateTimeField(null=False)
    resolved_at = models.DateTimeField(null=True)
    item_change_loser = models.ForeignKey(
        ItemChangeRecord,
        null=False,
        on_delete=models.PROTECT,
        related_name="lost_conflicts",
    )
    item_change_winner = models.ForeignKey(
        ItemChangeRecord,
        null=True,
        on_delete=models.PROTECT,
        related_name="won_conflicts",
    )
    status = models.CharField(
        max_length=100,
        choices=(
            (ConflictStatus.DEFERRED.value, "Deferred"),
            (ConflictStatus.RESOLVED.value, "Resolved"),
        ),
    )
    conflict_type = models.CharField(
        max_length=100,
        choices=(
            (
                ConflictType.LOCAL_UPDATE_REMOTE_UPDATE.value,
                "Both the local provider and the remote provider updated the same item",
            ),
            (
                ConflictType.LOCAL_UPDATE_REMOTE_DELETE.value,
                "The local provider updated an item that was deleted by the remote provider",
            ),
            (
                ConflictType.LOCAL_DELETE_REMOTE_UPDATE.value,
                "The local provider deleted an item that was updated by the remote provider",
            ),
            (
                ConflictType.EXCEPTION_OCCURRED.value,
                "There was an error applying a change to an item",
            ),
            (
                ConflictType.LOCAL_UPDATE_REMOTE_INSERT.value,
                "The local provider updated an item that was inserted by the remote provider. There may be an error in the implementation of one of the backends involved in the sync session.",
            ),
            (
                ConflictType.LOCAL_INSERT_REMOTE_UPDATE.value,
                "The local provider inserted an item that was updated by the remote provider. There may be an error in the implementation of one of the backends involved in the sync session.",
            ),
        ),
    )
    description = models.TextField(null=True)

    class Meta:
        ordering = ["created_at"]


class SyncSessionRecord(models.Model):
    id = models.UUIDField(primary_key=True)
    started_at = models.DateTimeField(null=False)
    ended_at = models.DateTimeField(null=True)
    status = models.CharField(
        max_length=100,
        choices=(
            (SyncSessionStatus.IN_PROGRESS.value, "In progress"),
            (SyncSessionStatus.FINISHED.value, "Finished"),
            (SyncSessionStatus.FAILED.value, "Failed"),
        ),
    )
    source_provider_id = models.TextField()
    target_provider_id = models.TextField()
    item_changes = models.ManyToManyField(ItemChangeRecord)

    _item_changes: "List[ItemChangeRecord]"
