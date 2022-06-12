import time
from itertools import permutations
from typing import TYPE_CHECKING, Dict, List, Optional

from .metadata import VectorClock
from .utils import BaseSyncLock, SyncTimer

if TYPE_CHECKING:  # pragma: no cover
    from maestro.core.provider import BaseSyncProvider
    from maestro.core.query.metadata import Query


class SyncOrchestrator:
    """Synchronizes data between two providers.

    Attributes:
        sync_lock (BaseSyncLock): Lock used to make sure multiple synchronizations don't happen in parallel.
        maximum_duration_seconds (int): The maximum duration in seconds that the sync session can last. If the session doesn't end by that time, an exception of type SyncTimeoutException is raised.
        queue_timeout_seconds (int): The maximum duration in seconds that a sync session will wait to start if another session is already running otherwise it will be aborted. It defaults to zero.
    """

    sync_lock: "BaseSyncLock"
    _providers_by_id: "Dict[str, BaseSyncProvider]"
    maximum_duration_seconds: "int"
    queue_timeout_seconds: "int"

    def __init__(
        self,
        sync_lock: "BaseSyncLock",
        providers: "List[BaseSyncProvider]",
        maximum_duration_seconds: "int",
        queue_timeout_seconds: "int" = 0
    ):
        """
        Args:
            sync_lock (BaseSyncLock): Lock used to make sure multiple synchronizations don't happen in parallel.
            providers (List[BaseSyncProvider]): Lista of providers that will be synchronized.
            maximum_duration_seconds (int): The maximum duration in seconds that the sync session can last. If the session doesn't end by that time, an exception of type SyncTimeoutException is raised.
            queue_timeout_seconds (int): The maximum duration in seconds that a sync session will wait to start if another session is already running otherwise it will be aborted. It defaults to zero.
        """
        self.sync_lock = sync_lock
        self._providers_by_id = {
            provider.provider_id: provider for provider in providers
        }
        self.maximum_duration_seconds = maximum_duration_seconds
        self.queue_timeout_seconds = queue_timeout_seconds

    def synchronize_providers(
        self,
        source_provider_id: "str",
        target_provider_id: "str",
        query: "Optional[Query]" = None,
    ):
        """Retrieves data from the source provider and sends them to the target provider.

        Args:
            source_provider_id (str): Source provider's identifier.
            target_provider_id (str): Target provider's identifier.
        """

        # Finding providers
        source_provider = self._providers_by_id.get(source_provider_id)
        if not source_provider:
            raise ValueError("Unknown provider: %s" % (source_provider_id))

        target_provider = self._providers_by_id.get(target_provider_id)
        if not target_provider:
            raise ValueError("Unknown provider: %s" % (target_provider_id))

        # Start event
        target_provider.events_manager.on_start_sync_session(
            source_provider_id=source_provider_id,
            target_provider_id=target_provider_id,
            query=query,
        )
        source_provider.events_manager.on_start_sync_session(
            source_provider_id=source_provider_id,
            target_provider_id=target_provider_id,
            query=query,
        )
        sync_timer = SyncTimer(timeout_seconds=self.maximum_duration_seconds)

        # Synchronization

        try:
            # Deferred changes
            deferred_vector_clock = VectorClock.create_empty(
                provider_ids=list(self._providers_by_id.keys())
            )

            while True:
                sync_timer.tick()

                item_change_batch = target_provider.get_deferred_changes(
                    vector_clock=deferred_vector_clock, query=query
                )
                target_provider.upload_changes(
                    item_change_batch=item_change_batch, query=query
                )

                new_deferred_vector_clock = (
                    item_change_batch.get_vector_clock_after_done(
                        initial_vector_clock=deferred_vector_clock
                    )
                )

                if new_deferred_vector_clock == deferred_vector_clock:
                    break

                if item_change_batch.is_last_batch:
                    break

                deferred_vector_clock = new_deferred_vector_clock

            # New changes
            target_vector_clock = target_provider.get_vector_clock(query=query)
            while True:
                sync_timer.tick()

                item_change_batch = source_provider.download_changes(
                    vector_clock=target_vector_clock, query=query
                )

                source_provider.events_manager.on_item_changes_sent(
                    item_changes=item_change_batch.item_changes
                )

                target_provider.upload_changes(
                    item_change_batch=item_change_batch, query=query
                )

                new_target_vector_clock = item_change_batch.get_vector_clock_after_done(
                    initial_vector_clock=target_vector_clock
                )

                if new_target_vector_clock == target_vector_clock:
                    break

                if item_change_batch.is_last_batch:
                    break

                target_vector_clock = new_target_vector_clock

            # End event
            target_provider.events_manager.on_end_sync_session()
            source_provider.events_manager.on_end_sync_session()
        except Exception as e:
            target_provider.events_manager.on_failed_sync_session(exception=e)
            source_provider.events_manager.on_failed_sync_session(exception=e)

    def run(self, initial_source_provider_id: "str"):
        """Runs two synchronization sessions for each pair of providers:
            1) initial_source_provider_id => other_provider
            2) other_provider => initial_source_provider_id

        Args:
            initial_source_provider_id (str): The identifier of the provider that will first send data.
        """

        if self.queue_timeout_seconds > 0:
            start_time = time.time()
            while self.sync_lock.is_running():
                current_time = time.time()
                if current_time - start_time > self.queue_timeout_seconds:
                    return
                time.sleep(0.1)
        else:
            if self.sync_lock.is_running():
                return

        with self.sync_lock.lock():
            pairs = list(permutations(self._providers_by_id.keys(), 2))
            pairs.sort(
                key=lambda pair: 0
                if pair[0] == initial_source_provider_id
                else 1
                if pair[1] == initial_source_provider_id
                else 2
            )

            for source_provider_id, target_provider_id in pairs:
                self.synchronize_providers(
                    source_provider_id=source_provider_id,
                    target_provider_id=target_provider_id,
                )
