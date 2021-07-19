from maestro.core.serializer import (
    RawDataStoreJSONSerializer,
    MetadataSerializer,
)
from maestro.backends.in_memory import InMemorySyncLock

from maestro.core.orchestrator import SyncOrchestrator
from maestro.core.metadata import Operation
import uuid
import json

from typing import Dict

# from .in_memory import create_provider as create_in_memory_provider
# from .firestore import create_provider as create_firestore_provider
from .django import create_provider as create_django_provider
from .mongo import create_provider as create_mongo_provider


class SyncHandler:
    def __init__(self):

        # Provider 1
        self.firestore_providers = []
        # self.provider1, api_serializer = create_firestore_provider(
        #     local_provider_id="provider1",
        # )
        # self.firestore_providers.append(self.provider1)
        # self.provider1.api_serializer = api_serializer
        self.provider1, api_serializer = create_django_provider(
            local_provider_id="provider1",
        )
        self.provider1.api_serializer = api_serializer

        # Provider 2
        # self.provider2, api_serializer = create_django_provider(
        #     local_provider_id="provider2",
        # )
        # self.provider2.api_serializer = api_serializer
        self.provider2, api_serializer = create_mongo_provider(
            local_provider_id="provider2",
        )
        self.provider2.api_serializer = api_serializer

        self.sync_lock = InMemorySyncLock()
        self.orchestrator = SyncOrchestrator(
            sync_lock=self.sync_lock,
            providers=[self.provider1, self.provider2],
            maximum_duration_seconds=5 * 60,
        )

        self.providers_map = {"provider1": self.provider1, "provider2": self.provider2}

    def get_other_provider_id(self, provider_id: "str") -> "str":
        for other_provider_id in self.providers_map.keys():
            if other_provider_id != provider_id:
                return other_provider_id

        raise ValueError()

    def update_item(self, provider_id: "str", item_id: "uuid.UUID", raw_item: "Dict"):
        provider = self.providers_map[provider_id]
        item = provider.api_serializer.from_dict(data=raw_item)

        provider.data_store.commit_item_change(
            operation=Operation.UPDATE, item_id=item_id, item=item
        )

    def delete_item(self, provider_id: "str", item_id: "str", raw_item: "Dict"):
        provider = self.providers_map[provider_id]

        item = provider.api_serializer.from_dict(data=raw_item)
        provider.data_store.commit_item_change(
            operation=Operation.DELETE, item_id=uuid.UUID(item_id), item=item,
        )

    def create_item(self, provider_id: "str", item_id: "str", raw_item: "Dict"):

        provider = self.providers_map[provider_id]

        item = provider.api_serializer.from_dict(data=raw_item)
        provider.data_store.commit_item_change(
            operation=Operation.INSERT, item_id=uuid.UUID(item_id), item=item
        )

    def synchronize(self, initial_source_provider_id: "str"):
        for provider in self.firestore_providers:
            provider.data_store._usage.reset()

        self.orchestrator.run(initial_source_provider_id=initial_source_provider_id)

        for provider in self.firestore_providers:
            print("Firestore usage:", provider.provider_id)
            provider.data_store._usage.show()
            provider.data_store._usage.enabled = False

    def get_items(self, provider_id: "str"):
        provider = self.providers_map[provider_id]

        items = []
        for item in provider.data_store.get_items():
            serialized_dict = provider.api_serializer.to_dict(item=item)
            items.append(serialized_dict)

        return json.dumps(items)

    def get_db(self, provider_id: "str"):
        provider = self.providers_map[provider_id]

        data_store_serializer = RawDataStoreJSONSerializer(
            metadata_serializer=MetadataSerializer(), indent=4
        )
        result = data_store_serializer.serialize(data_store=provider.data_store)
        return result
