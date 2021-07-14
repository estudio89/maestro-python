from django.apps import apps

import django.db.transaction
from django.core.cache import cache
from maestro.core.utils import BaseSyncLock
import json

def app_model_to_table_name(app_model: "str") -> "str":
    return app_model.replace(".", "_")

def table_name_to_app_model(table_name: "str") -> "str":
    app_label, model = table_name.rsplit("_", maxsplit=1)
    return app_label + "." + model

def get_content_type(serialized_item):
    from django.contrib.contenttypes.models import ContentType
    data = json.loads(serialized_item)
    table_name = data["table_name"]
    app_model = table_name_to_app_model(table_name)
    app, model = app_model.split(".")
    content_type = ContentType.objects.get(app_label=app, model=model)
    return content_type


class DjangoSyncLockContext(django.db.transaction.Atomic):
    def __enter__(self, *args, **kwargs):
        super().__enter__(*args, **kwargs)
        cache.set("maestro_running", True, timeout=None)
        SyncLockRecord = apps.get_model("maestro", "SyncLockRecord")
        SyncLockRecord.objects.select_for_update().filter(key="sync_running")

    def __exit__(self, *args, **kwargs):
        super().__exit__(*args, **kwargs)
        cache.set("maestro_running", False, timeout=None)


class DjangoSyncLock(BaseSyncLock):
    def is_running(self) -> "bool":
        return cache.get("maestro_running", False)

    def lock(self):
        return DjangoSyncLockContext(using=None, savepoint=True)
