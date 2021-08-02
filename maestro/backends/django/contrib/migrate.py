from django.db import models
from maestro.backends.django.contrib.factory import create_django_data_store
from maestro.backends.django.utils import model_to_entity_name
from maestro.core.metadata import Operation
from typing import Type, TYPE_CHECKING

if TYPE_CHECKING:
    from maestro.backends.django import DjangoDataStore


def commit_model_changes(model: "Type[models.Model]"):
    queryset = model.objects.all()
    data_store: "DjangoDataStore" = create_django_data_store()

    for item in queryset:
        entity_name = model_to_entity_name(item)
        data_store.commit_item_change(
            operation=Operation.INSERT,
            entity_name=entity_name,
            item_id=str(item.pk),
            item=item,
            execute_operation=False,
        )

