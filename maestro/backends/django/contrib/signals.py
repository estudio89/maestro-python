from django.apps import apps
from django.db import models
from django.db.models.signals import post_save, pre_delete
from typing import Type, Optional, List, cast, TYPE_CHECKING
from maestro.backends.django.settings import maestro_settings
from maestro.backends.django.contrib.factory import create_django_data_store
from maestro.backends.django.utils import model_to_entity_name
from maestro.core.metadata import Operation
from .middleware import _add_operation_to_queue
import copy

if TYPE_CHECKING:
    from maestro.backends.django import DjangoDataStore


def model_saved_signal(
    sender: "Type[models.Model]",
    instance: "models.Model",
    created: "bool",
    raw: "bool",
    using: "str",
    update_fields: "Optional[List[str]]",
    **kwargs,
):
    if getattr(sender, "_maestro_disable_signals", False):
        return

    operation: "Operation"
    if created:
        operation = Operation.INSERT
    else:
        operation = Operation.UPDATE

    data_store: "DjangoDataStore" = create_django_data_store()
    entity_name = model_to_entity_name(instance)
    data_store.commit_item_change(
        operation=operation,
        entity_name=entity_name,
        item_id=str(instance.pk),
        item=copy.deepcopy(instance),
        execute_operation=False,
    )
    _add_operation_to_queue(operation=operation, item=copy.deepcopy(instance))


def model_pre_delete_signal(
    sender: "Type[models.Model]", instance: "models.Model", using: "str", **kwargs
):
    if getattr(sender, "_maestro_disable_signals", False):
        return

    data_store: "DjangoDataStore" = create_django_data_store()
    entity_name = model_to_entity_name(instance)
    data_store.commit_item_change(
        operation=Operation.DELETE,
        entity_name=entity_name,
        item_id=str(instance.pk),
        item=copy.deepcopy(instance),
        execute_operation=False,
    )
    _add_operation_to_queue(operation=Operation.DELETE, item=copy.deepcopy(instance))


def _connect_signal(model: "models.Model"):
    full_label = (
        cast("str", model._meta.app_label) + "_" + cast("str", model._meta.model_name)
    )
    post_save.connect(
        receiver=model_saved_signal,
        sender=model,
        dispatch_uid=full_label + "_update_sync",
    )

    pre_delete.connect(
        receiver=model_pre_delete_signal,
        sender=model,
        dispatch_uid=full_label + "_delete_sync",
    )


def connect_signals():
    for app_model in maestro_settings.MODELS:
        model = apps.get_model(app_model)
        _connect_signal(model=model)


class _DisableSignalsContext:
    def __init__(self, model: "Type[models.Model]"):
        self.model = model

    def __enter__(self):
        self.model._maestro_disable_signals = True

    def __exit__(self, type, value, traceback):
        self.model._maestro_disable_signals = False

def temporarily_disable_signals(model: "Type[models.Model]"):
    return _DisableSignalsContext(model=model)
