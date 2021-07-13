from django.contrib import admin
from sync_framework.backends.django.models import (
    ItemChangeRecord,
    ItemVersionRecord,
    SyncSessionRecord,
    ConflictLogRecord,
)


class ItemChangeRecordAdmin(admin.ModelAdmin):
    date_hierarchy = "date_created"
    list_display = (
        "date_created",
        "operation",
        "content_type",
        "item_id",
        "provider_id",
        "provider_timestamp",
        "serialized_item",
        "should_ignore",
        "is_applied",
    )
    ordering = ["-date_created"]
    search_fields = ["item_id", "operation"]


class ItemVersionRecordAdmin(admin.ModelAdmin):
    date_hierarchy = "date_created"


class SyncSessionRecordAdmin(admin.ModelAdmin):
    date_hierarchy = "started_at"
    list_display = (
        "started_at",
        "ended_at",
        "status",
        "source_provider_id",
        "target_provider_id",
    )


class ConflictLogRecordAdmin(admin.ModelAdmin):
    date_hierarchy = "created_at"
    list_display = (
        "created_at",
        "resolved_at",
        "item_change_loser",
        "item_change_winner",
        "status",
        "conflict_type",
        "description",
    )


admin.site.register(ItemChangeRecord, ItemChangeRecordAdmin)
admin.site.register(ItemVersionRecord, ItemVersionRecordAdmin)
admin.site.register(SyncSessionRecord, SyncSessionRecordAdmin)
admin.site.register(ConflictLogRecord, ConflictLogRecordAdmin)
