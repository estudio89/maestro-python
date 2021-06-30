from django.db import models
from sync_framework.backends.django.contrib.factory import create_django_data_store
from sync_framework.backends.django import DjangoDataStore
from sync_framework.core.metadata import Operation
from typing import Type


def commit_model_changes(model: "Type[models.Model]"):
    queryset = model.objects.all()
    data_store: "DjangoDataStore" = create_django_data_store()

    for item in queryset:
        data_store.commit_item_change(
            operation=Operation.INSERT,
            item_id=str(item.pk),
            item=item,
            execute_operation=False,
        )

