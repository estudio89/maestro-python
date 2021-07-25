from maestro.core.provider import BaseSyncProvider
from maestro.core.metadata import ItemChangeBatch
from maestro.core.query.metadata import Query
from typing import Optional


class DjangoSyncProvider(BaseSyncProvider):
    def upload_changes(
        self, item_change_batch: "ItemChangeBatch", query: "Optional[Query]"
    ):
        if query is not None:
            raise ValueError("This backend doesn't support queries!")

        super().upload_changes(item_change_batch=item_change_batch, query=query)
