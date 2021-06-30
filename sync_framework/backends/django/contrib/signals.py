from django.apps import apps
from django.db import models
from django.db.models.signals import post_save, pre_delete
from typing import Type, Optional, List, cast
from sync_framework.backends.django.settings import sync_framework_settings
from sync_framework.core.metadata import Operation
from .middleware import _add_operation_to_queue
import copy


def model_saved_signal(
    sender: "Type[models.Model]",
    instance: "models.Model",
    created: "bool",
    raw: "bool",
    using: "str",
    update_fields: "Optional[List[str]]",
    **kwargs,
):
    operation: "Operation"
    if created:
        operation = Operation.INSERT
    else:
        operation = Operation.UPDATE

    _add_operation_to_queue(operation=operation, item=copy.deepcopy(instance))


def model_pre_delete_signal(
    sender: "Type[models.Model]", instance: "models.Model", using: "str", **kwargs
):
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
    for app_model in sync_framework_settings.MODELS:
        model = apps.get_model(app_model)
        _connect_signal(model=model)


def _disconnect_signal(model: "models.Model"):
    full_label = (
        cast("str", model._meta.app_label) + "_" + cast("str", model._meta.model_name)
    )
    post_save.disconnect(
        receiver=model_saved_signal,
        sender=model,
        dispatch_uid=full_label + "_update_sync",
    )
    pre_delete.disconnect(
        receiver=model_saved_signal,
        sender=model,
        dispatch_uid=full_label + "_delete_sync",
    )


class _DisableSignalsContext:
    def __init__(self, model: "Type[models.Model]"):
        self.model = model

    def __enter__(self):
        _disconnect_signal(model=self.model)

    def __exit__(self, type, value, traceback):
        label = self.model._meta.app_label + "." + self.model._meta.model_name
        enabled_models = [label.lower() for label in sync_framework_settings.MODELS]
        if label in enabled_models:
            _connect_signal(model=self.model)


def temporarily_disable_signals(model: "Type[models.Model]"):
    return _DisableSignalsContext(model=model)
