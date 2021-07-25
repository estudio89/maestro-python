from maestro.backends.base_nosql.provider import NoSQLSyncProvider
from maestro.core.metadata import ItemChangeBatch
from maestro.core.query.metadata import Query
from maestro.backends.firestore.store import FirestoreDataStore
from typing import cast, Optional


class FirestoreSyncProvider(NoSQLSyncProvider):
    def upload_changes(
        self, item_change_batch: "ItemChangeBatch", query: "Optional[Query]"
    ):
        if query is not None:
            raise ValueError("This backend doesn't support queries!")

        firestore_data_store = cast("FirestoreDataStore", self.data_store)
        super().upload_changes(item_change_batch=item_change_batch, query=query)
        if firestore_data_store._usage.enabled:
            firestore_data_store._usage.show()
