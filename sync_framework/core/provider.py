from typing import TYPE_CHECKING
from abc import ABC

if TYPE_CHECKING:  # pragma: no cover
    from sync_framework.core.store import BaseDataStore
    from sync_framework.core.events import EventsManager
    from sync_framework.core.metadata import VectorClock, ItemChangeBatch
    from sync_framework.core.execution import ChangesExecutor


class BaseSyncProvider(ABC):
    """Manages the changes that will be synchronized to a data store.

    Attributes:
        provider_id (str): This provider's unique identifier.
        data_store (BaseDataStore): The data store.
        events_manager (EventsManager): The class that will handle synchronization events.
        changes_executor (ChangesExecutor): The class that will process the changes to be applied to the data store.
        max_num (int): The maximum number of changes that will be processed in each batch of changes.
    """

    provider_id: "str"
    data_store: "BaseDataStore"
    events_manager: "EventsManager"
    changes_executor: "ChangesExecutor"
    max_num: "int"

    def __init__(
        self,
        provider_id: "str",
        data_store: "BaseDataStore",
        events_manager: "EventsManager",
        changes_executor: "ChangesExecutor",
        max_num: "int",
    ):
        """
        Args:
            provider_id (str): This provider's unique identifier.
            data_store (BaseDataStore): The data store.
            events_manager (EventsManager): The class that will handle synchronization events.
            changes_executor (ChangesExecutor): The class that will process the changes to be applied to the data store.
            max_num (int): The maximum number of changes that will be processed in each batch of changes.
        """
        self.provider_id = provider_id
        self.data_store = data_store
        self.events_manager = events_manager
        self.changes_executor = changes_executor
        self.max_num = max_num

    def get_vector_clock(self) -> "VectorClock":
        """Returns the current VectorClock for this provider.

        Returns:
            VectorClock: The current VectorClock for this provider.
        """
        return self.data_store.get_local_vector_clock()

    def download_changes(self, vector_clock: "VectorClock") -> "ItemChangeBatch":
        """Retrieves the changes that occurred in the data store linked to this provider after the timestamps defined by the given VectorClock.

        Args:
            vector_clock (VectorClock): VectorClock used for selecting changes.

        Returns:
            ItemChangeBatch: The batch of changes that was selected.
        """
        item_change_batch = self.data_store.select_changes(
            vector_clock=vector_clock, max_num=self.max_num
        )
        item_change_batch.reset_status()
        return item_change_batch

    def upload_changes(self, item_change_batch: "ItemChangeBatch"):
        """Applies changes obtained from a remote provider to the data store.

        Args:
            item_change_batch (ItemChangeBatch): The batch of changes to be applied.
        """

        self.changes_executor.run(item_changes=item_change_batch.item_changes)

    def get_deferred_changes(self, vector_clock: "VectorClock") -> "ItemChangeBatch":
        """Retrieves the changes received previously but that weren't applied in the last session due to an exception having occurred.

        Args:
            vector_clock (VectorClock): VectorClock used to select the changes.

        Returns:
            ItemChangeBatch: The batch of changes that was selected.
        """
        return self.data_store.select_deferred_changes(
            vector_clock=vector_clock, max_num=self.max_num
        )

    def __repr__(self):  # pragma: no cover
        return f"{self.__class__.__name__}(provider_id='{self.provider_id}')"
