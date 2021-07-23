from django.apps import apps
from django.db.models import Model
import django.db.transaction
from typing import TYPE_CHECKING
from django.core.cache import cache
from maestro.core.utils import BaseSyncLock

if TYPE_CHECKING:
    from django.contrib.contenttypes.models import ContentType


def app_model_to_entity_name(app_model: "str") -> "str":
    return app_model.replace(".", "_")


def entity_name_to_app_model(entity_name: "str") -> "str":
    app_label, model = entity_name.rsplit("_", maxsplit=1)
    return app_label + "." + model


def entity_name_to_content_type(entity_name: "str") -> "ContentType":
    ContentType = apps.get_model("contenttypes", "ContentType")
    app_model = entity_name_to_app_model(entity_name)
    app, model = app_model.split(".")
    content_type = ContentType.objects.get(app_label=app, model=model)
    return content_type


def content_type_to_entity_name(content_type: "ContentType"):
    app_model = content_type.app_label + "." + content_type.model
    entity_name = app_model_to_entity_name(app_model)
    return entity_name


def model_to_entity_name(model: "Model") -> "str":
    ContentType = apps.get_model("contenttypes", "ContentType")
    content_type = ContentType.objects.get_for_model(model)
    return content_type_to_entity_name(content_type=content_type)


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
