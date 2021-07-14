from maestro.core.provider import BaseSyncProvider
from maestro.core.metadata import ItemChangeBatch
from maestro.backends.firestore.store import FirestoreDataStore
from typing import cast


class FirestoreSyncProvider(BaseSyncProvider):
    def upload_changes(self, item_change_batch: "ItemChangeBatch"):
        firestore_data_store = cast("FirestoreDataStore", self.data_store)
        super().upload_changes(item_change_batch=item_change_batch)
        if firestore_data_store._usage.enabled:
            firestore_data_store._usage.show()
