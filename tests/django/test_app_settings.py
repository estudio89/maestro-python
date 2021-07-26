from django.test import TestCase, override_settings
from django.apps import apps
from django.urls import re_path
from django.db import models
from maestro.backends.django.contrib.factory import create_django_data_store
from maestro.backends.django.contrib.model_dependencies import get_model_dependencies
from maestro.backends.django.contrib.signals import temporarily_disable_signals
from maestro.backends.django.contrib.migrate import commit_model_changes
from maestro.backends.django.contrib.middleware import _operations_queue
from maestro.core.metadata import Operation
import uuid
from tests.django.views import update_item_view

urlpatterns = [re_path(r"^(?P<item_id>.+)/$", update_item_view)]


@override_settings(ROOT_URLCONF="tests.django.test_app_settings")
class AppSettingsTest(TestCase):
    def setUp(self):
        _operations_queue.items = []

    def test_signals(self):
        self.assertEqual(_operations_queue.items, [])
        Item = apps.get_model("my_app", "Item")
        item = Item(
            id=uuid.UUID("533ce3b4-9ef6-42fe-b220-64c86aaad444"),
            name="item",
            version="1",
        )
        item.save()

        self.assertEqual(len(_operations_queue.items), 1)
        queued_operation = _operations_queue.items[0]
        self.assertEqual(queued_operation.item, item)
        self.assertEqual(queued_operation.operation, Operation.INSERT)

        item.name = "item2"
        item.save()

        self.assertEqual(len(_operations_queue.items), 2)
        queued_operation = _operations_queue.items[1]
        self.assertEqual(queued_operation.item, item)
        self.assertEqual(queued_operation.operation, Operation.UPDATE)

    def test_middleware(self):
        Item = apps.get_model("my_app", "Item")
        ItemChangeRecord = apps.get_model("maestro", "ItemChangeRecord")
        item_id = uuid.UUID("533ce3b4-9ef6-42fe-b220-64c86aaad444")
        item = Item(id=item_id, name="item", version="1",)

        data_store = create_django_data_store()
        data_store.commit_item_change(
            operation=Operation.INSERT,
            entity_name="my_app_item",
            item_id=item_id,
            item=item,
        )
        num_changes = ItemChangeRecord.objects.count()

        response = self.client.get("/" + str(item_id) + "/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(num_changes + 1, ItemChangeRecord.objects.count())
        last_change = ItemChangeRecord.objects.order_by("-date_created").first()
        self.assertEqual(last_change.item_id, item_id)
        self.assertEqual(last_change.operation, Operation.UPDATE.value)

    def test_migrate(self):
        Item = apps.get_model("my_app", "Item")
        ItemChangeRecord = apps.get_model("maestro", "ItemChangeRecord")
        with temporarily_disable_signals(Item):
            Item.objects.create(
                id=uuid.UUID("533ce3b4-9ef6-42fe-b220-64c86aaad444"),
                name="item",
                version="1",
            )
            Item.objects.create(
                id=uuid.UUID("f901423f-8fbb-4f89-ac2a-a1f5c1a41ed2"),
                name="item",
                version="1",
            )

        num_changes = ItemChangeRecord.objects.count()
        commit_model_changes(Item)
        self.assertEqual(num_changes + 2, ItemChangeRecord.objects.count())

    def test_model_dependencies(self):
        all_models = []
        AnotherModel = apps.get_model("my_app", "AnotherModel")
        dependencies_tree = get_model_dependencies(
            model=AnotherModel, all_models=all_models
        )
        self.assertEqual(
            dependencies_tree,
            {
                "model": "my_app.anothermodel",
                "dependencies": [{"model": "my_app.item", "dependencies": []}],
            },
        )
        self.assertEqual(all_models, ["my_app.item", "my_app.anothermodel"])
