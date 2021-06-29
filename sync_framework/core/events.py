from typing import TYPE_CHECKING, List
from .metadata import (
    ItemChange,
    ConflictType,
    ConflictStatus,
    ConflictLog,
    SyncSession,
    SyncSessionStatus,
)
from .utils import get_now_utc
import uuid
import traceback
import sys

if TYPE_CHECKING:  # pragma: no cover
    from .store import BaseDataStore


class EventsManager:
    """Handles the events that happen during a sync session."""

    data_store: "BaseDataStore"
    current_sync_session: "SyncSession"

    def __init__(self, data_store: "BaseDataStore"):
        self.data_store = data_store

    def on_start_sync_session(
        self, source_provider_id: "str", target_provider_id: "str"
    ):
        """This is called at the start of a sync session. It creates a sync session and saves it to the data store.

        Args:
            source_provider_id (str): Source provider id.
            target_provider_id (str): Target provider id.
        """
        now_utc = get_now_utc()
        sync_session = SyncSession(
            id=uuid.uuid4(),
            started_at=now_utc,
            ended_at=None,
            status=SyncSessionStatus.IN_PROGRESS,
            source_provider_id=source_provider_id,
            target_provider_id=target_provider_id,
            item_changes=[],
        )
        self.data_store.save_sync_session(sync_session=sync_session)
        self.current_sync_session = sync_session

    def on_conflict_resolved(
        self,
        conflict_type: "ConflictType",
        item_change_winner: "ItemChange",
        item_change_loser: "ItemChange",
    ):
        """Called after a conflict is resolved. It creates a ConflictLog and saves it to the data store.

        Args:
            conflict_type (ConflictType): Type of conflict
            item_change_winner (ItemChange): ItemChange that won the conflict
            item_change_loser (ItemChange): ItemChange that lost the conflict
        """
        now_utc = get_now_utc()
        conflict_log = ConflictLog(
            id=uuid.uuid4(),
            created_at=now_utc,
            resolved_at=now_utc,
            item_change_loser=item_change_loser,
            item_change_winner=item_change_winner,
            status=ConflictStatus.RESOLVED,
            conflict_type=conflict_type,
            description=None,
        )
        self.data_store.save_conflict_log(conflict_log=conflict_log)

    def _format_stacktrace(self):
        parts = ["Traceback (most recent call last):\n"]
        parts.extend(traceback.format_stack(limit=25)[:-2])
        parts.extend(traceback.format_exception(*sys.exc_info())[1:])
        return "".join(parts)

    def on_exception(self, remote_item_change: "ItemChange", exception: "Exception"):
        """Called when an exception is raised when trying to execute an ItemChange. It creates a ConflictLog and saves it to the data store.

        Args:
            remote_item_change (ItemChange): ItemChange that was being executed when the exception was raised.
            exception (Exception): Exception that was raised.
        """
        conflict_logs = self.data_store.get_deferred_conflict_logs(
            item_change_loser=remote_item_change
        )
        if len(conflict_logs) > 0:
            return

        now_utc = get_now_utc()
        conflict_log = ConflictLog(
            id=uuid.uuid4(),
            created_at=now_utc,
            resolved_at=now_utc,
            item_change_loser=remote_item_change,
            item_change_winner=None,
            status=ConflictStatus.DEFERRED,
            conflict_type=ConflictType.EXCEPTION_OCCURRED,
            description=self._format_stacktrace(),
        )
        self.data_store.save_conflict_log(conflict_log=conflict_log)

    def on_item_change_processed(self, item_change: "ItemChange"):
        """Called after an ItemChange is saved to the data store but before it is executed. It adds the ItemChange to the running sync session.

        Args:
            item_change (ItemChange): ItemChange saved to the data store
        """
        self.current_sync_session.item_changes.append(item_change)

    def on_item_change_applied(self, item_change: "ItemChange"):
        """Called after an ItemChange was executed successfully.

        Args:
            item_change (ItemChange): The change that was executed
        """
        conflict_logs = self.data_store.get_deferred_conflict_logs(
            item_change_loser=item_change
        )
        if not conflict_logs:
            return

        now_utc = get_now_utc()
        for conflict_log in conflict_logs:
            conflict_log.status = ConflictStatus.RESOLVED
            conflict_log.resolved_at = now_utc
            self.data_store.save_conflict_log(conflict_log)

    def on_item_changes_sent(self, item_changes: "List[ItemChange]"):
        """Called after a list of changes is sent to another provider.

        Args:
            item_changes (List[ItemChange]): The changes that were sent.
        """
        self.current_sync_session.item_changes += item_changes

    def on_end_sync_session(self):
        """Called at the end of the sync session.
        """
        now_utc = get_now_utc()
        self.current_sync_session.ended_at = now_utc
        self.current_sync_session.status = SyncSessionStatus.FINISHED
        self.data_store.save_sync_session(sync_session=self.current_sync_session)
        self.current_sync_session = None

    def on_failed_sync_session(self, exception: "Exception"):
        """Called if an exception is raised while running the sync session. Note that this would be called if there was a
        failure in the framework itself, not in the execution of an ItemChange.
        """
        now_utc = get_now_utc()
        self.current_sync_session.ended_at = now_utc
        self.current_sync_session.status = SyncSessionStatus.FAILED
        self.data_store.save_sync_session(sync_session=self.current_sync_session)
