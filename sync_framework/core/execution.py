from typing import TYPE_CHECKING, List, Callable, NamedTuple, Optional
from sync_framework.core.metadata import (
    ItemChange,
    ConflictType,
    Operation,
    ItemVersion,
)

if TYPE_CHECKING:  # pragma: no cover
    from sync_framework.core.store import BaseDataStore
    from sync_framework.core.events import EventsManager


class ConflictCheckResult(NamedTuple):
    '''Stores the result of a conflict check.'''

    has_conflict: "bool"
    conflict_type: "Optional[ConflictType]"
    local_item_change: "Optional[ItemChange]"
    remote_item_change: "Optional[ItemChange]"
    local_version: "ItemVersion"


class ConflictResolution(NamedTuple):
    '''Stores information about which change won and which lost a conflict. '''

    item_change_loser: "ItemChange"
    item_change_winner: "ItemChange"


class ConflictResolver:
    """Determines which change won a conflict."""

    def resolve(
        self,
        conflict_type: "ConflictType",
        local_item_change: "ItemChange",
        remote_item_change: "ItemChange",
    ) -> "ConflictResolution":
        """Selects the winning change between two conflicting changes. It applies the following resolution rules for each type of conflict:

            ConflictType.LOCAL_UPDATE_REMOTE_UPDATE - Most recent change wins
            ConflictType.LOCAL_UPDATE_REMOTE_DELETE - Deletion wins
            ConflictType.LOCAL_DELETE_REMOTE_UPDATE - Deletion wins
            ConflictType.LOCAL_DELETE_REMOTE_DELETE - Local change wins

        Args:
            conflict_type (ConflictType): Type of conflict
            local_item_change (ItemChange): Local change
            remote_item_change (ItemChange): Remote change
        """
        if (
            conflict_type == ConflictType.LOCAL_UPDATE_REMOTE_UPDATE
            or conflict_type == ConflictType.LOCAL_DELETE_REMOTE_DELETE
            or conflict_type == ConflictType.LOCAL_INSERT_REMOTE_UPDATE
            or conflict_type == ConflictType.LOCAL_UPDATE_REMOTE_INSERT
        ):  # Most recent change wins
            if (
                local_item_change.provider_timestamp
                > remote_item_change.provider_timestamp
            ):
                item_change_winner = local_item_change
                item_change_loser = remote_item_change
            else:
                item_change_winner = remote_item_change
                item_change_loser = local_item_change
        elif (
            conflict_type == ConflictType.LOCAL_UPDATE_REMOTE_DELETE
        ):  # Deletion wins
            item_change_winner = remote_item_change
            item_change_loser = local_item_change
        elif conflict_type == ConflictType.LOCAL_DELETE_REMOTE_UPDATE:
            item_change_winner = local_item_change
            item_change_loser = remote_item_change

        return ConflictResolution(
            item_change_loser=item_change_loser, item_change_winner=item_change_winner
        )


class ChangesExecutor:
    """Processes and applies each change received from a remote provider."""

    data_store: "BaseDataStore"
    events_manager: "EventsManager"
    conflict_resolver: "ConflictResolver"

    def __init__(
        self,
        data_store: "BaseDataStore",
        events_manager: "EventsManager",
        conflict_resolver: "ConflictResolver",
    ):
        self.data_store = data_store
        self.events_manager = events_manager
        self.conflict_resolver = conflict_resolver

    def run(self, item_changes: "List[ItemChange]"):
        """Iterates the changes and applies each one.

        Args:
            item_changes (List[ItemChange]): list of changes to be processed.
        """
        for item_change in item_changes:
            self.process_remote_change(item_change=item_change)

    def process_remote_change(self, item_change: "ItemChange"):
        """Processes a change received from a remote provider. Processing means:

            - Checking if it needs to be applied
            - Checking to see if the change causes a conflict
            - Handling conflicts
            - Executing the change
            - Posting events

        Args:
            item_change (ItemChange): The change to be processed
        """
        item_change = self.data_store.get_or_create_item_change(item_change=item_change)
        self.events_manager.on_item_change_processed(item_change=item_change)
        if item_change.is_applied:
            return

        if item_change.should_ignore:
            item_change.is_applied = True
            self.data_store.save_item_change(item_change=item_change)
            return

        post_transaction_callback: "Callable"

        def in_transaction():
            nonlocal post_transaction_callback
            result = self.check_conflict(remote_item_change=item_change)
            if result.has_conflict:
                post_transaction_callback = self.handle_conflict(
                    conflict_type=result.conflict_type,
                    local_item_change=result.local_item_change,
                    remote_item_change=result.remote_item_change,
                    local_version=result.local_version,
                )
            else:
                self.apply_item_change(
                    item_change=item_change, old_version=result.local_version
                )
                post_transaction_callback = lambda: self.events_manager.on_item_change_applied(
                    item_change=item_change
                )

        try:
            self.data_store.run_in_transaction(
                item_change=item_change, callback=in_transaction
            )
            post_transaction_callback()
        except Exception as e:
            self.handle_exception(remote_item_change=item_change, exception=e)

    def check_conflict(self, remote_item_change: "ItemChange") -> "ConflictCheckResult":
        """Checks if the remote change causes a conflict.
            A conflict occurs any time the remote provider wasn't aware of the change
            currently linked to the local version of the item being changed.

        Args:
            remote_item_change (ItemChange): The change received from the remote provider.

        Returns:
            ConflictCheckResult: The result of the analysis
        """
        local_version = self.data_store.get_local_version(
            item_id=remote_item_change.item_id
        )
        if local_version.current_item_change is None:
            # The item does not exist in storage yet, therefore there's no conflict
            return ConflictCheckResult(
                has_conflict=False,
                conflict_type=None,
                local_item_change=None,
                remote_item_change=None,
                local_version=local_version,
            )

        local_item_change = local_version.current_item_change
        local_vector_clock = local_item_change.vector_clock
        local_vector_clock_item = local_vector_clock.get_vector_clock_item(
            provider_id=local_item_change.provider_id
        )

        remote_vector_clock = remote_item_change.vector_clock
        remote_vector_clock_item = remote_vector_clock.get_vector_clock_item(
            provider_id=local_item_change.provider_id
        )

        if local_vector_clock_item.timestamp > remote_vector_clock_item.timestamp:
            # Source provider was not aware of the local version of the item = conflict

            if (
                local_item_change.operation == Operation.UPDATE
                and remote_item_change.operation == Operation.UPDATE
            ):
                conflict_type = ConflictType.LOCAL_UPDATE_REMOTE_UPDATE
            elif (
                local_item_change.operation == Operation.UPDATE
                and remote_item_change.operation == Operation.DELETE
            ):
                conflict_type = ConflictType.LOCAL_UPDATE_REMOTE_DELETE
            elif (
                local_item_change.operation == Operation.DELETE
                and remote_item_change.operation == Operation.UPDATE
            ):
                conflict_type = ConflictType.LOCAL_DELETE_REMOTE_UPDATE
            elif (
                local_item_change.operation == Operation.DELETE
                and remote_item_change.operation == Operation.DELETE
            ):
                conflict_type = ConflictType.LOCAL_DELETE_REMOTE_DELETE
            elif (
                local_item_change.operation == Operation.INSERT
                and remote_item_change.operation == Operation.UPDATE
            ):
                conflict_type = ConflictType.LOCAL_INSERT_REMOTE_UPDATE
            elif (
                local_item_change.operation == Operation.UPDATE
                and remote_item_change.operation == Operation.INSERT
            ):
                conflict_type = ConflictType.LOCAL_INSERT_REMOTE_UPDATE

            return ConflictCheckResult(
                has_conflict=True,
                conflict_type=conflict_type,
                local_item_change=local_item_change,
                remote_item_change=remote_item_change,
                local_version=local_version,
            )
        else:
            # Source provider was aware of the local version of the item = no conflict

            return ConflictCheckResult(
                has_conflict=False,
                conflict_type=None,
                local_item_change=None,
                remote_item_change=None,
                local_version=local_version,
            )

    def handle_conflict(
        self,
        conflict_type: "ConflictType",
        local_item_change: "ItemChange",
        remote_item_change: "ItemChange",
        local_version: "ItemVersion",
    ) -> "Callable":
        """Called whenever a conflict is detected.

        Args:
            conflict_type (ConflictType): Type of conflict
            local_item_change (ItemChange): The local change applied to the item previously
            remote_item_change (ItemChange): The remote change that caused the conflict

        Returns:
            (Callable): A function that is to be called at the end of the transaction
        """
        conflict_resolution = self.conflict_resolver.resolve(
            conflict_type=conflict_type,
            local_item_change=local_item_change,
            remote_item_change=remote_item_change,
        )
        item_change_winner = conflict_resolution.item_change_winner
        item_change_loser = conflict_resolution.item_change_loser

        item_change_loser.is_applied = True
        item_change_loser.should_ignore = True
        self.data_store.save_item_change(item_change=item_change_loser)

        self.apply_item_change(
            item_change=item_change_winner, old_version=local_version
        )

        def post_transaction_callback():
            self.events_manager.on_item_change_applied(item_change=item_change_loser)
            self.events_manager.on_item_change_applied(item_change=item_change_winner)
            self.events_manager.on_conflict_resolved(
                conflict_type=conflict_type,
                item_change_winner=item_change_winner,
                item_change_loser=item_change_loser,
            )

        return post_transaction_callback

    def handle_exception(
        self, remote_item_change: "ItemChange", exception: "Exception"
    ):
        """Called whenever an exception is raised while trying to apply a change to an item.

        Args:
            remote_item_change (ItemChange): The change that was being applied when the exception was raised.
            exception (Exception): The exception that was raised.
        """
        self.events_manager.on_exception(
            remote_item_change=remote_item_change, exception=exception
        )

    def apply_item_change(self, item_change: "ItemChange", old_version: "ItemVersion"):
        """Applies a change. This consists of:
            - Executing the change
            - Marking the change as applied
            - Saving the change to the data store
            - Updating the version of the item referenced by the change

        Args:
            item_change (ItemChange): The change to be applied.
            old_version (ItemVersion): The current local version of the item.

        """
        new_version = ItemVersion(
            current_item_change=item_change,
            item_id=item_change.item_id,
            vector_clock=item_change.vector_clock,
            date_created=old_version.date_created,
        )
        if item_change.is_applied:
            self.data_store.save_item_version(item_version=new_version)
            return

        self.data_store.execute_item_change(item_change=item_change)
        item_change.is_applied = True
        self.data_store.save_item_change(item_change=item_change)
        self.data_store.save_item_version(item_version=new_version)
