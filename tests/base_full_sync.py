import unittest
import unittest.mock
from maestro.core.utils import make_hashable
from maestro.core.orchestrator import SyncOrchestrator
from maestro.core.store import BaseDataStore
from maestro.core.provider import BaseSyncProvider
from maestro.core.query.store import TrackQueriesStoreMixin
from maestro.core.events import EventsManager
from maestro.core.exceptions import ItemNotFoundException
from maestro.core.execution import ChangesExecutor, ConflictResolver
from maestro.core.query.metadata import Query, Filter, Comparison, Comparator, SortOrder
from maestro.core.metadata import (
    ConflictStatus,
    ConflictType,
    ItemChange,
    Operation,
    SyncSessionStatus,
)
from .base import BackendTestMixin, TestDataStoreMixin
from typing import cast, Union
import uuid
import copy


class DebugEventsManager(EventsManager):
    raise_exception = True

    def on_exception(self, remote_item_change: "ItemChange", exception: "Exception"):
        if self.raise_exception:  # pragma: no cover
            raise exception
        else:
            super().on_exception(
                remote_item_change=remote_item_change, exception=exception
            )

    def on_failed_sync_session(self, exception: "Exception"):
        super().on_failed_sync_session(exception=exception)
        if self.raise_exception:  # pragma: no cover
            raise exception


class FullSyncTest(BackendTestMixin, unittest.TestCase):
    """
        This test is based on an example given in Microsoft's SyncFramework documentation:
        https://docs.microsoft.com/en-us/previous-versions/mt763482(v=msdn.10)?redirectedfrom=MSDN#synchronization-example
    """

    maxDiff = None

    def setUp(self):

        # Provider 1
        self.data_store1 = self._create_data_store(local_provider_id="other_provider",)
        self.events_manager1 = DebugEventsManager(data_store=self.data_store1)
        self.changes_executor1 = ChangesExecutor(
            data_store=self.data_store1,
            events_manager=self.events_manager1,
            conflict_resolver=ConflictResolver(),
        )
        self.other_provider = self._create_provider(
            provider_id="other_provider",
            data_store=self.data_store1,
            events_manager=self.events_manager1,
            changes_executor=self.changes_executor1,
            max_num=5,
        )

        # Provider 2
        self.data_store2 = self._create_data_store(
            local_provider_id="provider_in_test",
        )
        self.events_manager2 = DebugEventsManager(data_store=self.data_store2)
        self.changes_executor2 = ChangesExecutor(
            data_store=self.data_store2,
            events_manager=self.events_manager2,
            conflict_resolver=ConflictResolver(),
        )
        self.provider_in_test = self._create_provider(
            provider_id="provider_in_test",
            data_store=self.data_store2,
            events_manager=self.events_manager2,
            changes_executor=self.changes_executor2,
            max_num=5,
        )

        self.sync_lock = self._create_sync_lock()
        self.orchestrator = SyncOrchestrator(
            sync_lock=self.sync_lock,
            providers=[self.other_provider, self.provider_in_test],
            maximum_duration_seconds=5 * 60,
        )
        self.setUpDataStores()

    def setUpDataStores(self):
        # 1) Saving initial items to each data store

        # > Data store 1
        # >> Item I1
        num_changes = len(self.data_store1.get_item_changes())
        self.item1_id = str(uuid.uuid4())

        self.data_store1.commit_item_change(
            operation=Operation.INSERT,
            entity_name="my_app_item",
            item_id=self.item1_id,
            item=self.data_store1._create_item(
                id=self.item1_id, name="I1", version="1"
            ),
        )
        for i in range(2, 6):
            last_item_change = self.data_store1.commit_item_change(
                operation=Operation.UPDATE,
                entity_name="my_app_item",
                item_id=self.item1_id,
                item=self.data_store1._create_item(
                    id=self.item1_id, name="I1", version=str(i)
                ),
            )

        self.last_item_change_1 = last_item_change
        self.assertEqual(num_changes + 5, len(self.data_store1.get_item_changes()))

        item = self.data_store1.get_item_by_id(id=self.item1_id)

        self.assertEqual(
            item,
            self.data_store1._create_item(
                id=str(self.item1_id), name="I1", version="5"
            ),
        )

        version = self.data_store1.get_local_version(
            item_id=last_item_change.serialization_result.item_id
        )
        self.assertEqual(version.item_id, self.item1_id)

        self.assertEqual(version.current_item_change, last_item_change)

        self.assertEqual(
            self.to_dict(
                version.current_item_change.serialization_result.serialized_item
            ),
            self.to_dict(
                self._serialize_item(name="I1", version="5", id=self.item1_id)
            ),
        )
        self.assertEqual(len(self.data_store1.get_item_versions()), 1)

        # >> Item I2
        num_changes = len(self.data_store1.get_item_changes())
        self.item2_id = str(uuid.uuid4())
        self.data_store1.commit_item_change(
            operation=Operation.INSERT,
            entity_name="my_app_item",
            item_id=self.item2_id,
            item=self.data_store1._create_item(
                id=self.item2_id, name="I2", version="1"
            ),
        )
        for i in range(2, 4):
            last_item_change = self.data_store1.commit_item_change(
                operation=Operation.UPDATE,
                entity_name="my_app_item",
                item_id=self.item2_id,
                item=self.data_store1._create_item(
                    id=self.item2_id, name="I2", version=str(i)
                ),
            )

        self.last_item_change_2 = last_item_change
        self.assertEqual(num_changes + 3, len(self.data_store1.get_item_changes()))

        item = self.data_store1.get_item_by_id(id=self.item2_id)
        self.assertEqual(
            item,
            self.data_store1._create_item(
                id=str(self.item2_id), name="I2", version="3"
            ),
        )

        version = self.data_store1.get_local_version(
            item_id=last_item_change.serialization_result.item_id
        )
        self.assertEqual(version.item_id, self.item2_id)
        self.assertEqual(version.current_item_change, last_item_change)

        self.assertEqual(
            self.to_dict(
                version.current_item_change.serialization_result.serialized_item
            ),
            self.to_dict(
                self._serialize_item(name="I2", version="3", id=self.item2_id)
            ),
        )
        self.assertEqual(len(self.data_store1.get_item_versions()), 2)

        # >> Item I3
        num_changes = len(self.data_store1.get_item_changes())
        self.item3_id = str(uuid.uuid4())
        self.data_store1.commit_item_change(
            operation=Operation.INSERT,
            entity_name="my_app_item",
            item_id=self.item3_id,
            item=self.data_store1._create_item(
                id=self.item3_id, name="I3", version="1"
            ),
        )
        for i in range(2, 5):
            last_item_change = self.data_store1.commit_item_change(
                operation=Operation.UPDATE,
                entity_name="my_app_item",
                item_id=self.item3_id,
                item=self.data_store1._create_item(
                    id=self.item3_id, name="I3", version=str(i)
                ),
            )

        self.last_item_change_3 = last_item_change
        self.assertEqual(num_changes + 4, len(self.data_store1.get_item_changes()))

        item = self.data_store1.get_item_by_id(id=self.item3_id)
        self.assertEqual(
            item,
            self.data_store1._create_item(
                id=str(self.item3_id), name="I3", version="4"
            ),
        )

        version = self.data_store1.get_local_version(
            item_id=last_item_change.serialization_result.item_id
        )
        self.assertEqual(version.item_id, self.item3_id)
        self.assertEqual(version.current_item_change, last_item_change)

        self.assertEqual(
            self.to_dict(
                version.current_item_change.serialization_result.serialized_item
            ),
            self.to_dict(
                self._serialize_item(name="I3", version="4", id=self.item3_id)
            ),
        )
        self.assertEqual(len(self.data_store1.get_item_versions()), 3)

        # > Data store 2
        # >> Item I104
        num_changes = len(self.data_store2.get_item_changes())
        self.item104_id = str(uuid.uuid4())

        self.data_store2.commit_item_change(
            operation=Operation.INSERT,
            entity_name="my_app_item",
            item_id=self.item104_id,
            item=self.data_store2._create_item(
                id=self.item104_id, name="I104", version="1"
            ),
        )
        for i in range(2, 3):
            last_item_change = self.data_store2.commit_item_change(
                operation=Operation.UPDATE,
                entity_name="my_app_item",
                item_id=self.item104_id,
                item=self.data_store2._create_item(
                    id=self.item104_id, name="I104", version=str(i)
                ),
            )

        self.last_item_change_104 = last_item_change
        self.assertEqual(num_changes + 2, len(self.data_store2.get_item_changes()))

        item = self.data_store2.get_item_by_id(id=self.item104_id)
        self.assertEqual(
            item,
            self.data_store2._create_item(id=self.item104_id, name="I104", version="2"),
        )

        version = self.data_store2.get_local_version(
            item_id=last_item_change.serialization_result.item_id
        )
        self.assertEqual(version.item_id, self.item104_id)
        self.assertEqual(version.current_item_change, last_item_change)

        self.assertEqual(
            self.to_dict(
                version.current_item_change.serialization_result.serialized_item
            ),
            self.to_dict(
                self._serialize_item(name="I104", version="2", id=self.item104_id)
            ),
        )

        self.assertEqual(len(self.data_store2.get_item_versions()), 1)

        # >> Item I105
        num_changes = len(self.data_store2.get_item_changes())
        self.item105_id = str(uuid.uuid4())
        self.data_store2.commit_item_change(
            operation=Operation.INSERT,
            entity_name="my_app_item",
            item_id=self.item105_id,
            item=self.data_store2._create_item(
                id=self.item105_id, name="I105", version="1"
            ),
        )
        for i in range(2, 5):
            last_item_change = self.data_store2.commit_item_change(
                operation=Operation.UPDATE,
                entity_name="my_app_item",
                item_id=self.item105_id,
                item=self.data_store2._create_item(
                    id=self.item105_id, name="I105", version=str(i)
                ),
            )

        self.last_item_change_105 = last_item_change

        self.assertEqual(num_changes + 4, len(self.data_store2.get_item_changes()))

        item = self.data_store2.get_item_by_id(id=self.item105_id)
        self.assertEqual(
            item,
            self.data_store2._create_item(id=self.item105_id, name="I105", version="4"),
        )

        version = self.data_store2.get_local_version(
            item_id=last_item_change.serialization_result.item_id
        )
        self.assertEqual(version.item_id, self.item105_id)
        self.assertEqual(version.current_item_change, last_item_change)

        self.assertEqual(
            self.to_dict(
                version.current_item_change.serialization_result.serialized_item
            ),
            self.to_dict(
                self._serialize_item(name="I105", version="4", id=self.item105_id)
            ),
        )
        self.assertEqual(len(self.data_store2.get_item_versions()), 2)

    def _full_sync_no_conflict_before_first_sync(self):
        pass

    def _full_sync_no_conflict_after_first_sync(self):
        pass

    def _full_sync_no_conflict_before_second_sync(self):
        pass

    def _full_sync_no_conflict_after_second_sync(self):
        pass

    def test_full_sync_no_conflict(self):

        # 1) Synchronizing data from other_provider (1) to provider_in_test (2)

        initial_item_changes = copy.deepcopy(self.data_store2.get_item_changes())
        initial_item_versions = copy.deepcopy(self.data_store2.get_item_versions())
        initial_conflict_logs = copy.deepcopy(self.data_store2.get_conflict_logs())
        initial_items = copy.deepcopy(self.data_store2.get_items())
        self.assertEqual(len(initial_item_changes), 6)
        self.assertEqual(len(initial_item_versions), 2)
        self.assertEqual(len(initial_conflict_logs), 0)
        self.assertEqual(len(initial_items), 2)

        self._full_sync_no_conflict_before_first_sync()
        self.orchestrator.synchronize_providers(
            source_provider_id="other_provider", target_provider_id="provider_in_test"
        )
        self._full_sync_no_conflict_after_first_sync()

        final_item_changes = copy.deepcopy(self.data_store2.get_item_changes())
        final_item_versions = copy.deepcopy(self.data_store2.get_item_versions())
        final_conflict_logs = copy.deepcopy(self.data_store2.get_conflict_logs())
        final_items = copy.deepcopy(self.data_store2.get_items())

        self.assertEqual(
            len(final_item_changes), 6 + len(self.data_store1.get_item_changes())
        )
        self.assertEqual(len(final_item_versions), 2 + 3)
        self.assertEqual(len(final_conflict_logs), 0)
        self.assertEqual(len(final_items), 2 + 3)

        # 2) Checking data
        # Making sure that pre-existing changes were kept
        self.assertEqual(final_item_changes[:6], initial_item_changes)

        # Making sure new changes were synced correctly
        self.assertEqual(final_item_changes[6:], self.data_store1.get_item_changes())

        # Checking if the items were synced correctly
        item1 = self.data_store2.get_item_by_id(self.item1_id)
        item2 = self.data_store2.get_item_by_id(self.item2_id)
        item3 = self.data_store2.get_item_by_id(self.item3_id)
        item104 = self.data_store2.get_item_by_id(self.item104_id)
        item105 = self.data_store2.get_item_by_id(self.item105_id)

        self.assertEqual(
            item1,
            self.data_store2._create_item(
                id=str(self.item1_id), name="I1", version="5"
            ),
        )
        self.assertEqual(
            item2,
            self.data_store2._create_item(
                id=str(self.item2_id), name="I2", version="3"
            ),
        )
        self.assertEqual(
            item3,
            self.data_store2._create_item(
                id=str(self.item3_id), name="I3", version="4"
            ),
        )
        self.assertEqual(
            item104,
            self.data_store2._create_item(
                id=str(self.item104_id), name="I104", version="2"
            ),
        )
        self.assertEqual(
            item105,
            self.data_store2._create_item(
                id=str(self.item105_id), name="I105", version="4"
            ),
        )

        # Checking if the versions were created correctly
        self.assertEqual(
            {
                (str(item_version.item_id), str(item_version.current_item_change.id),)
                for item_version in self.data_store2.get_item_versions()
            },
            {
                (str(self.data_store2._get_id(item1)), str(self.last_item_change_1.id)),
                (str(self.data_store2._get_id(item2)), str(self.last_item_change_2.id)),
                (str(self.data_store2._get_id(item3)), str(self.last_item_change_3.id)),
                (
                    str(self.data_store2._get_id(item104)),
                    str(self.last_item_change_104.id),
                ),
                (
                    str(self.data_store2._get_id(item105)),
                    str(self.last_item_change_105.id),
                ),
            },
        )

        # Checking if the sync session was saved correctly
        self.assertEqual(1, len(self.data_store1.get_sync_sessions()))
        sync_session_1 = self.data_store1.get_sync_sessions()[0]
        self.assertEqual(sync_session_1.status, SyncSessionStatus.FINISHED)
        self.assertTrue(sync_session_1.started_at < sync_session_1.ended_at)
        self.assertEqual(
            [item_change.id for item_change in sync_session_1.item_changes],
            [item_change.id for item_change in self.data_store1.get_item_changes()],
        )
        self.assertEqual(sync_session_1.source_provider_id, "other_provider")
        self.assertEqual(sync_session_1.target_provider_id, "provider_in_test")

        self.assertEqual(1, len(self.data_store2.get_sync_sessions()))
        sync_session_2 = self.data_store2.get_sync_sessions()[0]
        self.assertEqual(sync_session_2.status, SyncSessionStatus.FINISHED)
        self.assertTrue(sync_session_2.started_at < sync_session_2.ended_at)

        self.assertEqual(
            sync_session_2.item_changes, self.data_store1.get_item_changes()
        )
        self.assertEqual(sync_session_2.source_provider_id, "other_provider")
        self.assertEqual(sync_session_2.target_provider_id, "provider_in_test")
        self.assertNotEqual(self.data_store1, self.data_store2)

        # 3) Synchronizing data from provider_in_test (2) to other_provider (1)

        initial_item_changes = copy.deepcopy(self.data_store1.get_item_changes())
        initial_item_versions = copy.deepcopy(self.data_store1.get_item_versions())
        initial_conflict_logs = copy.deepcopy(self.data_store1.get_conflict_logs())
        initial_items = copy.deepcopy(self.data_store1.get_items())
        self.assertEqual(len(initial_item_changes), 12)
        self.assertEqual(len(initial_item_versions), 3)
        self.assertEqual(len(initial_conflict_logs), 0)
        self.assertEqual(len(initial_items), 3)

        self._full_sync_no_conflict_before_second_sync()
        self.orchestrator.synchronize_providers(
            source_provider_id="provider_in_test", target_provider_id="other_provider"
        )
        self._full_sync_no_conflict_after_second_sync()

        final_item_changes = copy.deepcopy(self.data_store1.get_item_changes())
        final_item_versions = copy.deepcopy(self.data_store1.get_item_versions())
        final_conflict_logs = copy.deepcopy(self.data_store1.get_conflict_logs())
        final_items = copy.deepcopy(self.data_store1.get_items())

        self.assertEqual(len(final_item_changes), 12 + 6)
        self.assertEqual(len(final_item_versions), 3 + 2)
        self.assertEqual(len(final_conflict_logs), 0)
        self.assertEqual(len(final_items), 3 + 2)

        # 4) Checking data
        # Making sure that pre-existing changes were kept
        self.assertEqual(final_item_changes[:12], initial_item_changes)

        # Making sure new changes were synced correctly
        self.assertEqual(
            final_item_changes[12:], self.data_store2.get_item_changes()[:6]
        )

        # Making sure the items were synchronized correctly
        item1 = self.data_store1.get_item_by_id(self.item1_id)
        item2 = self.data_store1.get_item_by_id(self.item2_id)
        item3 = self.data_store1.get_item_by_id(self.item3_id)
        item104 = self.data_store1.get_item_by_id(self.item104_id)
        item105 = self.data_store1.get_item_by_id(self.item105_id)

        self.assertEqual(
            item1,
            self.data_store1._create_item(
                id=str(self.item1_id), name="I1", version="5"
            ),
        )
        self.assertEqual(
            item2,
            self.data_store1._create_item(
                id=str(self.item2_id), name="I2", version="3"
            ),
        )
        self.assertEqual(
            item3,
            self.data_store1._create_item(
                id=str(self.item3_id), name="I3", version="4"
            ),
        )
        self.assertEqual(
            item104,
            self.data_store1._create_item(
                id=str(self.item104_id), name="I104", version="2"
            ),
        )
        self.assertEqual(
            item105,
            self.data_store1._create_item(
                id=str(self.item105_id), name="I105", version="4"
            ),
        )

        # Making sure the version were created correctly
        self.assertEqual(
            {
                (str(item_version.item_id), str(item_version.current_item_change.id),)
                for item_version in self.data_store2.get_item_versions()
            },
            {
                (str(self.data_store1._get_id(item1)), str(self.last_item_change_1.id)),
                (str(self.data_store1._get_id(item2)), str(self.last_item_change_2.id)),
                (str(self.data_store1._get_id(item3)), str(self.last_item_change_3.id)),
                (
                    str(self.data_store1._get_id(item104)),
                    str(self.last_item_change_104.id),
                ),
                (
                    str(self.data_store1._get_id(item105)),
                    str(self.last_item_change_105.id),
                ),
            },
        )

        # Checking if the sync session was saved correctly
        self.assertEqual(2, len(self.data_store1.get_sync_sessions()))
        sync_session_1 = self.data_store1.get_sync_sessions()[-1]
        self.assertEqual(sync_session_1.status, SyncSessionStatus.FINISHED)
        self.assertTrue(sync_session_1.started_at < sync_session_1.ended_at)
        self.assertEqual(
            sync_session_1.item_changes, self.data_store2.get_item_changes()[:6]
        )
        self.assertEqual(sync_session_1.source_provider_id, "provider_in_test")
        self.assertEqual(sync_session_1.target_provider_id, "other_provider")

        self.assertEqual(2, len(self.data_store2.get_sync_sessions()))
        sync_session_2 = self.data_store2.get_sync_sessions()[-1]
        self.assertEqual(sync_session_2.status, SyncSessionStatus.FINISHED)
        self.assertTrue(sync_session_2.started_at < sync_session_2.ended_at)
        self.assertEqual(
            [item_change.id for item_change in sync_session_2.item_changes],
            [item_change.id for item_change in self.data_store2.get_item_changes()[:6]],
        )
        self.assertEqual(sync_session_2.source_provider_id, "provider_in_test")
        self.assertEqual(sync_session_2.target_provider_id, "other_provider")
        self.assertEqual(self.data_store1, self.data_store2)

    def test_conflict_update_update(self):
        """Simulates a conflict of type LOCAL_UPDATE_REMOTE_UPDATE during the synchronization session. """

        # Performing initial full sync
        # Both data stores will have the objects: I1, I2, I3, I104 e I105
        self.orchestrator.run(initial_source_provider_id="other_provider")

        # Item I2 is updated by data store 1 (other_provider)
        other_provider_change = self.data_store1.commit_item_change(
            operation=Operation.UPDATE,
            entity_name="my_app_item",
            item_id=self.item2_id,
            item=self.data_store1._create_item(
                id=str(self.item2_id), name="I2", version="other_provider_version",
            ),
        )

        # Same item I2 is updated by data store 2 (provider_in_test)
        provider_in_test_change = self.data_store2.commit_item_change(
            operation=Operation.UPDATE,
            entity_name="my_app_item",
            item_id=self.item2_id,
            item=self.data_store2._create_item(
                id=str(self.item2_id), name="I2", version="provider_in_test_version",
            ),
        )

        # Sinchronization: other_provider (1) -> provider_in_test (2)
        # Expected results: the change from provider_in_test wins because it's the most recent
        num_changes = len(self.data_store2.get_item_changes())
        self.orchestrator.synchronize_providers(
            source_provider_id="other_provider", target_provider_id="provider_in_test"
        )

        # Checking data
        conflict_logs = self.data_store2.get_conflict_logs()
        self.assertEqual(len(conflict_logs), 1)
        self.assertEqual(conflict_logs[0].status, ConflictStatus.RESOLVED)
        self.assertEqual(
            conflict_logs[0].conflict_type, ConflictType.LOCAL_UPDATE_REMOTE_UPDATE
        )
        self.assertEqual(
            conflict_logs[0].item_change_winner.id, provider_in_test_change.id
        )
        self.assertEqual(
            conflict_logs[0].item_change_loser.id, other_provider_change.id
        )
        self.assertEqual(num_changes + 1, len(self.data_store2.get_item_changes()))

        item_version = self.data_store2.get_local_version(
            item_id=provider_in_test_change.serialization_result.item_id
        )
        self.assertEqual(
            item_version.current_item_change.id, provider_in_test_change.id
        )
        item_change_loser = self.data_store2.get_item_change_by_id(
            id=other_provider_change.id
        )
        self.assertTrue(item_change_loser.should_ignore)
        self.assertTrue(item_change_loser.is_applied)

        item_change_winner = self.data_store2.get_item_change_by_id(
            id=provider_in_test_change.id
        )
        self.assertFalse(item_change_winner.should_ignore)
        self.assertTrue(item_change_winner.is_applied)

        item2 = self.data_store2.get_item_by_id(id=self.item2_id)
        self.assertEqual(
            item2,
            self.data_store2._create_item(
                id=str(self.item2_id), name="I2", version="provider_in_test_version"
            ),
        )

        # Sinchronization: provider_in_test (2) -> other_provider (1)
        num_changes = len(self.data_store1.get_item_changes())
        self.orchestrator.synchronize_providers(
            source_provider_id="provider_in_test", target_provider_id="other_provider"
        )

        # Checking data
        conflict_logs = self.data_store1.get_conflict_logs()
        self.assertEqual(len(conflict_logs), 1)
        self.assertEqual(conflict_logs[0].status, ConflictStatus.RESOLVED)
        self.assertEqual(
            conflict_logs[0].conflict_type, ConflictType.LOCAL_UPDATE_REMOTE_UPDATE
        )
        self.assertEqual(
            conflict_logs[0].item_change_winner.id, provider_in_test_change.id
        )
        self.assertEqual(
            conflict_logs[0].item_change_loser.id, other_provider_change.id
        )
        self.assertEqual(num_changes + 1, len(self.data_store1.get_item_changes()))

        item_version = self.data_store1.get_local_version(
            item_id=provider_in_test_change.serialization_result.item_id
        )
        self.assertEqual(
            item_version.current_item_change.id, provider_in_test_change.id
        )
        item_change_loser = self.data_store1.get_item_change_by_id(
            id=other_provider_change.id
        )
        self.assertTrue(item_change_loser.should_ignore)
        self.assertTrue(item_change_loser.is_applied)

        item_change_winner = self.data_store1.get_item_change_by_id(
            id=provider_in_test_change.id
        )
        self.assertFalse(item_change_winner.should_ignore)
        self.assertTrue(item_change_winner.is_applied)

        item2 = self.data_store1.get_item_by_id(id=self.item2_id)
        self.assertEqual(
            item2,
            self.data_store1._create_item(
                id=str(self.item2_id), name="I2", version="provider_in_test_version"
            ),
        )

    def test_conflict_update_delete(self):
        """Simulates conflicts of type LOCAL_UPDATE_REMOTE_DELETE and LOCAL_DELETE_REMOTE_UPDATE in a sync session. """

        # Performing initial full sync
        # Both data stores will have the objects: I1, I2, I3, I104 e I105
        self.orchestrator.run(initial_source_provider_id="other_provider")

        # Item I2 is updated by data_store1 (other_provider)
        other_provider_change = self.data_store1.commit_item_change(
            operation=Operation.UPDATE,
            entity_name="my_app_item",
            item_id=self.item2_id,
            item=self.data_store1._create_item(
                id=str(self.item2_id), name="I2", version="other_provider_version",
            ),
        )

        # Same item I2 is deleted by data_store2 (provider_in_test)
        provider_in_test_change = self.data_store2.commit_item_change(
            operation=Operation.DELETE,
            entity_name="my_app_item",
            item_id=self.item2_id,
            item=self.data_store2._create_item(
                id=str(self.item2_id), name="I2", version="3"
            ),
        )

        # Sinchronization: other_provider (1) -> provider_in_test (2)
        # Expected results: deletion by provider_in_test wins
        num_changes = len(self.data_store2.get_item_changes())
        num_items = len(self.data_store2.get_items())
        self.orchestrator.synchronize_providers(
            source_provider_id="other_provider", target_provider_id="provider_in_test"
        )

        # Checking data
        conflict_logs = self.data_store2.get_conflict_logs()
        self.assertEqual(len(conflict_logs), 1)
        self.assertEqual(conflict_logs[0].status, ConflictStatus.RESOLVED)
        self.assertEqual(
            conflict_logs[0].conflict_type, ConflictType.LOCAL_DELETE_REMOTE_UPDATE
        )
        self.assertEqual(
            conflict_logs[0].item_change_winner.id, provider_in_test_change.id
        )
        self.assertEqual(
            conflict_logs[0].item_change_loser.id, other_provider_change.id
        )
        self.assertEqual(num_changes + 1, len(self.data_store2.get_item_changes()))
        self.assertEqual(num_items, len(self.data_store2.get_items()))

        item_version = self.data_store2.get_local_version(
            item_id=provider_in_test_change.serialization_result.item_id
        )
        self.assertEqual(
            item_version.current_item_change.id, provider_in_test_change.id
        )
        item_change_loser = self.data_store2.get_item_change_by_id(
            id=other_provider_change.id
        )
        self.assertTrue(item_change_loser.should_ignore)
        self.assertTrue(item_change_loser.is_applied)

        item_change_winner = self.data_store2.get_item_change_by_id(
            id=provider_in_test_change.id
        )
        self.assertFalse(item_change_winner.should_ignore)
        self.assertTrue(item_change_winner.is_applied)

        with self.assertRaises(ItemNotFoundException):
            self.data_store2.get_item_by_id(id=self.item2_id)

        # Sincronization: provider_in_test (2) -> other_provider (1)
        num_changes = len(self.data_store1.get_item_changes())
        num_items = len(self.data_store1.get_items())
        self.orchestrator.synchronize_providers(
            source_provider_id="provider_in_test", target_provider_id="other_provider"
        )

        # Checking data
        conflict_logs = self.data_store1.get_conflict_logs()
        self.assertEqual(len(conflict_logs), 1)
        self.assertEqual(conflict_logs[0].status, ConflictStatus.RESOLVED)
        self.assertEqual(
            conflict_logs[0].conflict_type, ConflictType.LOCAL_UPDATE_REMOTE_DELETE
        )
        self.assertEqual(
            conflict_logs[0].item_change_winner.id, provider_in_test_change.id
        )
        self.assertEqual(
            conflict_logs[0].item_change_loser.id, other_provider_change.id
        )
        self.assertEqual(num_changes + 1, len(self.data_store1.get_item_changes()))
        self.assertEqual(num_items - 1, len(self.data_store1.get_items()))

        item_version = self.data_store1.get_local_version(
            item_id=provider_in_test_change.serialization_result.item_id
        )
        self.assertEqual(
            item_version.current_item_change.id, provider_in_test_change.id
        )
        item_change_loser = self.data_store1.get_item_change_by_id(
            id=other_provider_change.id
        )
        self.assertTrue(item_change_loser.should_ignore)
        self.assertTrue(item_change_loser.is_applied)

        item_change_winner = self.data_store1.get_item_change_by_id(
            id=provider_in_test_change.id
        )
        self.assertFalse(item_change_winner.should_ignore)
        self.assertTrue(item_change_winner.is_applied)

        with self.assertRaises(ItemNotFoundException):
            self.data_store1.get_item_by_id(id=self.item2_id)

    def test_conflict_delete_delete(self):
        """Simulates a conflict of type LOCAL_DELETE_REMOTE_DELETE in a sync session. """

        # Performing initial full sync
        # Both data stores will have the objects: I1, I2, I3, I104 e I105
        self.orchestrator.run(initial_source_provider_id="other_provider")

        # Item I2 is deleted by data_store1 (other_provider)
        other_provider_change = self.data_store1.commit_item_change(
            operation=Operation.DELETE,
            entity_name="my_app_item",
            item_id=self.item2_id,
            item=self.data_store1._create_item(
                id=str(self.item2_id), name="I2", version="3"
            ),
        )

        # Same item I2 is also deleted by data_store2 (provider_in_test)
        provider_in_test_change = self.data_store2.commit_item_change(
            operation=Operation.DELETE,
            entity_name="my_app_item",
            item_id=self.item2_id,
            item=self.data_store2._create_item(
                id=str(self.item2_id), name="I2", version="3"
            ),
        )

        # Sinchronization: other_provider (1) -> provider_in_test (2)
        # Expected results: deletion by provider_in_test wins because it's the most recent
        num_changes = len(self.data_store2.get_item_changes())
        num_items = len(self.data_store2.get_items())
        self.orchestrator.synchronize_providers(
            source_provider_id="other_provider", target_provider_id="provider_in_test"
        )

        # Checking data
        conflict_logs = self.data_store2.get_conflict_logs()
        self.assertEqual(len(conflict_logs), 1)
        self.assertEqual(conflict_logs[0].status, ConflictStatus.RESOLVED)
        self.assertEqual(
            conflict_logs[0].conflict_type, ConflictType.LOCAL_DELETE_REMOTE_DELETE
        )
        self.assertEqual(
            conflict_logs[0].item_change_winner.id, provider_in_test_change.id
        )
        self.assertEqual(
            conflict_logs[0].item_change_loser.id, other_provider_change.id
        )
        self.assertEqual(num_changes + 1, len(self.data_store2.get_item_changes()))
        self.assertEqual(num_items, len(self.data_store2.get_items()))

        item_version = self.data_store2.get_local_version(
            item_id=provider_in_test_change.serialization_result.item_id
        )
        self.assertEqual(
            item_version.current_item_change.id, provider_in_test_change.id
        )
        item_change_loser = self.data_store2.get_item_change_by_id(
            id=other_provider_change.id
        )
        self.assertTrue(item_change_loser.should_ignore)
        self.assertTrue(item_change_loser.is_applied)

        item_change_winner = self.data_store2.get_item_change_by_id(
            id=provider_in_test_change.id
        )
        self.assertFalse(item_change_winner.should_ignore)
        self.assertTrue(item_change_winner.is_applied)

        with self.assertRaises(ItemNotFoundException):
            self.data_store2.get_item_by_id(id=self.item2_id)

        # Sincronization: provider_in_test (2) -> other_provider (1)
        num_changes = len(self.data_store1.get_item_changes())
        num_items = len(self.data_store1.get_items())
        self.orchestrator.synchronize_providers(
            source_provider_id="provider_in_test", target_provider_id="other_provider"
        )

        # Checking data
        conflict_logs = self.data_store1.get_conflict_logs()
        self.assertEqual(len(conflict_logs), 1)
        self.assertEqual(conflict_logs[0].status, ConflictStatus.RESOLVED)
        self.assertEqual(
            conflict_logs[0].conflict_type, ConflictType.LOCAL_DELETE_REMOTE_DELETE
        )
        self.assertEqual(
            conflict_logs[0].item_change_winner.id, provider_in_test_change.id
        )
        self.assertEqual(
            conflict_logs[0].item_change_loser.id, other_provider_change.id
        )
        self.assertEqual(num_changes + 1, len(self.data_store1.get_item_changes()))
        self.assertEqual(num_items, len(self.data_store1.get_items()))

        item_version = self.data_store1.get_local_version(
            item_id=provider_in_test_change.serialization_result.item_id
        )
        self.assertEqual(
            item_version.current_item_change.id, provider_in_test_change.id
        )
        item_change_loser = self.data_store1.get_item_change_by_id(
            id=other_provider_change.id
        )
        self.assertTrue(item_change_loser.should_ignore)
        self.assertTrue(item_change_loser.is_applied)

        item_change_winner = self.data_store1.get_item_change_by_id(
            id=provider_in_test_change.id
        )
        self.assertFalse(item_change_winner.should_ignore)
        self.assertTrue(item_change_winner.is_applied)

        with self.assertRaises(ItemNotFoundException):
            self.data_store1.get_item_by_id(id=self.item2_id)

    def test_conflict_exception1(self):
        """
            Simulates the situation:
                other_provider >> Updates item I2
                other_provider >> Updates item I3

                ==> Sinchronization: other_provider (1) -> provider_in_test (2)
                    - Exception is raised when executing change to item I2
                    - Change must be applied to item I3

                ==> Sinchronization: other_provider (1) -> provider_in_test (2)
                    - Exception is raised when executing change to item I2
                    - Nothing happens

                ==> Sinchronization: other_provider (1) -> provider_in_test (2)
                    - This time, no exception is raised when executing change to item I2
                    - Change must be applied to item I2
        """

        # Performing initial full sync
        # Both data stores will have the objects: I1, I2, I3, I104 e I105
        self.orchestrator.run(initial_source_provider_id="other_provider")

        # Items I2 e I3 are updated by data_store1
        other_provider_change_i2 = self.data_store1.commit_item_change(
            operation=Operation.UPDATE,
            entity_name="my_app_item",
            item_id=self.item2_id,
            item=self.data_store1._create_item(
                id=str(self.item2_id), name="I2", version="other_provider_version_i2",
            ),
        )

        other_provider_change_i3 = self.data_store1.commit_item_change(
            operation=Operation.UPDATE,
            entity_name="my_app_item",
            item_id=self.item3_id,
            item=self.data_store1._create_item(
                id=str(self.item3_id), name="I3", version="other_provider_version_i3",
            ),
        )

        # During the sync session other_provider (1) -> provider_in_test (2), an exception is raised when executing the change to I2

        original = self.data_store2.execute_item_change
        item2_id = self.item2_id

        def execute_item_change_mock(item_change: "ItemChange"):
            if str(item_change.serialization_result.item_id) == str(item2_id):
                raise ValueError("Error!")
            else:
                original(item_change)

        self.data_store2.execute_item_change = execute_item_change_mock

        self.events_manager2.raise_exception = False
        num_changes = len(self.data_store2.get_item_changes())
        num_items = len(self.data_store2.get_items())
        num_conflict_logs = len(self.data_store2.get_conflict_logs())

        self.orchestrator.synchronize_providers(
            source_provider_id="other_provider", target_provider_id="provider_in_test"
        )
        self.events_manager2.raise_exception = True

        self.data_store2.execute_item_change = original

        # Expected results:
        #   - Both changes are saved
        #   - Item I2's version remains the same
        #   - Only the change to item I3 is executed
        #   - Item I3's version is updated
        #   - 1 conflict log with status DEFERRED is created
        self.assertEqual(num_changes + 2, len(self.data_store2.get_item_changes()))
        item2 = self.data_store2.get_item_by_id(id=self.item2_id)
        self.assertEqual(
            item2,
            self.data_store2._create_item(
                id=str(self.item2_id), name="I2", version="3"
            ),
        )

        item2_version = self.data_store2.get_local_version(
            item_id=other_provider_change_i2.serialization_result.item_id
        )
        self.assertEqual(
            item2_version.current_item_change.id, self.last_item_change_2.id
        )

        self.assertEqual(
            num_conflict_logs + 1, len(self.data_store2.get_conflict_logs())
        )
        self.assertEqual(num_items, len(self.data_store2.get_items()))

        conflict_log = self.data_store2.get_conflict_logs()[0]
        self.assertEqual(conflict_log.item_change_loser.id, other_provider_change_i2.id)
        self.assertEqual(conflict_log.status, ConflictStatus.DEFERRED)

        item_change = self.data_store2.get_item_change_by_id(
            id=other_provider_change_i2.id
        )
        self.assertFalse(item_change.is_applied)
        self.assertFalse(item_change.should_ignore)

        item_change = self.data_store2.get_item_change_by_id(
            id=other_provider_change_i3.id
        )
        self.assertTrue(item_change.is_applied)
        self.assertFalse(item_change.should_ignore)

        item3 = self.data_store2.get_item_by_id(id=self.item3_id)
        self.assertEqual(
            item3,
            self.data_store2._create_item(
                id=str(self.item3_id), name="I3", version="other_provider_version_i3"
            ),
        )
        item3_version = self.data_store2.get_local_version(
            item_id=other_provider_change_i3.serialization_result.item_id
        )
        self.assertEqual(
            item3_version.current_item_change.id, other_provider_change_i3.id
        )
        self.assertNotEqual(self.data_store1, self.data_store2)

        # Synchronization runs again (other_provider (1) -> provider_in_test (2)) and the exception is raised again when synchronizing the change to I2

        original = self.data_store2.execute_item_change
        item2_id = self.item2_id

        def execute_item_change_mock(item_change: "ItemChange"):
            if str(item_change.serialization_result.item_id) == str(item2_id):
                raise ValueError("Error!")
            else:
                original(item_change)

        self.data_store2.execute_item_change = execute_item_change_mock

        self.events_manager2.raise_exception = False
        num_changes = len(self.data_store2.get_item_changes())
        num_items = len(self.data_store2.get_items())
        num_conflict_logs = len(self.data_store2.get_conflict_logs())

        self.orchestrator.synchronize_providers(
            source_provider_id="other_provider", target_provider_id="provider_in_test"
        )
        self.events_manager2.raise_exception = True

        self.data_store2.execute_item_change = original

        # Expected results:
        #   - No new changes are saved
        #   - Item I2's version remains the same
        self.assertEqual(num_changes, len(self.data_store2.get_item_changes()))
        item2 = self.data_store2.get_item_by_id(id=self.item2_id)
        self.assertEqual(
            item2,
            self.data_store2._create_item(
                id=str(self.item2_id), name="I2", version="3"
            ),
        )

        item2_version = self.data_store2.get_local_version(
            item_id=other_provider_change_i2.serialization_result.item_id
        )
        self.assertEqual(
            item2_version.current_item_change.id, self.last_item_change_2.id
        )

        self.assertEqual(num_conflict_logs, len(self.data_store2.get_conflict_logs()))
        self.assertEqual(num_items, len(self.data_store2.get_items()))

        conflict_log = self.data_store2.get_conflict_logs()[0]
        self.assertEqual(conflict_log.item_change_loser.id, other_provider_change_i2.id)
        self.assertEqual(conflict_log.status, ConflictStatus.DEFERRED)

        item_change = self.data_store2.get_item_change_by_id(
            id=other_provider_change_i2.id
        )
        self.assertFalse(item_change.is_applied)
        self.assertFalse(item_change.should_ignore)

        item_change = self.data_store2.get_item_change_by_id(
            id=other_provider_change_i3.id
        )
        self.assertTrue(item_change.is_applied)
        self.assertFalse(item_change.should_ignore)

        item3 = self.data_store2.get_item_by_id(id=self.item3_id)
        self.assertEqual(
            item3,
            self.data_store2._create_item(
                id=str(self.item3_id), name="I3", version="other_provider_version_i3"
            ),
        )
        item3_version = self.data_store2.get_local_version(
            item_id=other_provider_change_i3.serialization_result.item_id
        )
        self.assertEqual(
            item3_version.current_item_change.id, other_provider_change_i3.id
        )
        self.assertNotEqual(self.data_store1, self.data_store2)

        # Synchronization runs again, no exception this time
        num_changes = len(self.data_store2.get_item_changes())
        num_items = len(self.data_store2.get_items())
        num_conflict_logs = len(self.data_store2.get_conflict_logs())

        self.orchestrator.synchronize_providers(
            source_provider_id="other_provider", target_provider_id="provider_in_test"
        )

        # Expected results:
        #   - Item I2's version is updated
        #   - Conflict log's status is updated to RESOLVED
        self.assertEqual(num_changes, len(self.data_store2.get_item_changes()))
        item2 = self.data_store2.get_item_by_id(id=self.item2_id)
        self.assertEqual(
            item2,
            self.data_store2._create_item(
                id=str(self.item2_id), name="I2", version="other_provider_version_i2"
            ),
        )

        item2_version = self.data_store2.get_local_version(
            item_id=other_provider_change_i2.serialization_result.item_id
        )
        self.assertEqual(
            item2_version.current_item_change.id, other_provider_change_i2.id
        )

        self.assertEqual(num_conflict_logs, len(self.data_store2.get_conflict_logs()))
        self.assertEqual(num_items, len(self.data_store2.get_items()))

        conflict_log = self.data_store2.get_conflict_logs()[0]
        self.assertEqual(conflict_log.item_change_loser.id, other_provider_change_i2.id)
        self.assertEqual(conflict_log.status, ConflictStatus.RESOLVED)

        self.assertEqual(self.data_store1, self.data_store2)

    def test_conflict_exception2(self):
        """
            Simulates the situation:
                - other_provider >> Deletes item I2
                - provider_in_test >> Updates item I2
                - provider_in_test >> Updates item I2
                - other_provider >> Updates item I3
                - Sinchronization: other_provider (1) -> provider_in_test (2)
                    - Exception is raised while executing deletion of item I2
                    - Change must be applied to item I3
                - Sinchronization: provider_in_test (2) -> other_provider (1)
                    - ItemChanges do provider_in_test are saved but not applied
                - provider_in_test >> Updates item I2

                - Sinchronization: other_provider (1) -> provider_in_test (2)
                    - This time no exception is raised when executing deletion of item I2
                    - Item I2 must be deleted
        """

        # Performing initial full sync
        # Both data stores will have the objects: I1, I2, I3, I104 e I105
        self.orchestrator.run(initial_source_provider_id="other_provider")

        # other_provider >> Deletes item I2
        other_provider_delete_i2 = self.data_store1.commit_item_change(
            operation=Operation.DELETE,
            entity_name="my_app_item",
            item_id=self.item2_id,
            item=self.data_store1._create_item(
                id=str(self.item2_id), name=None, version=None
            ),
        )

        # provider_in_test >> Updates item I2
        provider_in_test_update_i2_1 = self.data_store2.commit_item_change(
            operation=Operation.UPDATE,
            entity_name="my_app_item",
            item_id=self.item2_id,
            item=self.data_store2._create_item(
                id=str(self.item2_id),
                name="I2",
                version="provider_in_test_update_i2_1",
            ),
        )

        # provider_in_test >> Updates item I2
        provider_in_test_update_i2_2 = self.data_store2.commit_item_change(
            operation=Operation.UPDATE,
            entity_name="my_app_item",
            item_id=self.item2_id,
            item=self.data_store2._create_item(
                id=str(self.item2_id),
                name="I2",
                version="provider_in_test_update_i2_2",
            ),
        )

        # other_provider >> Updates item I3
        other_provider_change_i3 = self.data_store1.commit_item_change(
            operation=Operation.UPDATE,
            entity_name="my_app_item",
            item_id=self.item3_id,
            item=self.data_store1._create_item(
                id=str(self.item3_id), name="I3", version="other_provider_update_i3_1",
            ),
        )

        # During the sync session other_provider (1) -> provider_in_test (2), an exception is raised when executing the change to I2

        original = self.data_store2.execute_item_change
        item2_id = self.item2_id

        def execute_item_change_mock(item_change: "ItemChange"):
            if str(item_change.serialization_result.item_id) == str(item2_id):
                raise ValueError("Error!")
            else:
                original(item_change)

        self.data_store2.execute_item_change = execute_item_change_mock

        self.events_manager2.raise_exception = False
        num_changes1 = len(self.data_store1.get_item_changes())
        num_items1 = len(self.data_store1.get_items())
        num_conflict_logs1 = len(self.data_store1.get_conflict_logs())
        num_changes2 = len(self.data_store2.get_item_changes())
        num_items2 = len(self.data_store2.get_items())
        num_conflict_logs2 = len(self.data_store2.get_conflict_logs())
        self.orchestrator.run(initial_source_provider_id="other_provider")
        self.events_manager2.raise_exception = True

        self.data_store2.execute_item_change = original

        # Expected results:
        #   - 4 Item changes are saved
        #       - Only the deletion change is not applied
        #   - 1 conflict log with status DEFERRED is created

        # Checking data_store2 (provider_in_test)
        self.assertEqual(num_changes2 + 2, len(self.data_store2.get_item_changes()))
        item2 = self.data_store2.get_item_by_id(id=self.item2_id)
        self.assertEqual(
            item2,
            self.data_store2._create_item(
                id=str(self.item2_id),
                name="I2",
                version="provider_in_test_update_i2_2",
            ),
        )

        item2_version = self.data_store2.get_local_version(
            item_id=provider_in_test_update_i2_2.serialization_result.item_id
        )
        self.assertEqual(
            item2_version.current_item_change.id, provider_in_test_update_i2_2.id
        )

        self.assertEqual(
            num_conflict_logs2 + 1, len(self.data_store2.get_conflict_logs())
        )
        self.assertEqual(num_items2, len(self.data_store2.get_items()))

        conflict_log = self.data_store2.get_conflict_logs()[0]
        self.assertEqual(conflict_log.item_change_loser.id, other_provider_delete_i2.id)
        self.assertEqual(conflict_log.status, ConflictStatus.DEFERRED)

        item_change = self.data_store2.get_item_change_by_id(
            id=other_provider_delete_i2.id
        )
        self.assertFalse(item_change.is_applied)
        self.assertFalse(item_change.should_ignore)

        item_change = self.data_store2.get_item_change_by_id(
            id=other_provider_change_i3.id
        )
        self.assertTrue(item_change.is_applied)
        self.assertFalse(item_change.should_ignore)

        item3 = self.data_store2.get_item_by_id(id=self.item3_id)
        self.assertEqual(
            item3,
            self.data_store2._create_item(
                id=str(self.item3_id), name="I3", version="other_provider_update_i3_1",
            ),
        )
        item3_version = self.data_store2.get_local_version(
            item_id=other_provider_change_i3.serialization_result.item_id
        )
        self.assertEqual(
            item3_version.current_item_change.id, other_provider_change_i3.id
        )
        self.assertNotEqual(self.data_store1, self.data_store2)

        # Checking data_store1 (other_provider)
        self.assertEqual(num_changes1 + 2, len(self.data_store1.get_item_changes()))
        with self.assertRaises(ItemNotFoundException):
            self.data_store1.get_item_by_id(id=self.item2_id)

        item2_version = self.data_store1.get_local_version(
            item_id=other_provider_delete_i2.serialization_result.item_id
        )
        self.assertEqual(
            item2_version.current_item_change.id, other_provider_delete_i2.id
        )

        self.assertEqual(
            num_conflict_logs1 + 2, len(self.data_store1.get_conflict_logs())
        )
        self.assertEqual(num_items1, len(self.data_store1.get_items()))

        conflict_logs1 = self.data_store1.get_conflict_logs()
        self.assertEqual(
            conflict_logs1[0].item_change_loser.id, provider_in_test_update_i2_1.id
        )
        self.assertEqual(
            conflict_logs1[0].item_change_winner.id, other_provider_delete_i2.id
        )
        self.assertEqual(
            conflict_logs1[1].item_change_loser.id, provider_in_test_update_i2_2.id
        )
        self.assertEqual(
            conflict_logs1[1].item_change_winner.id, other_provider_delete_i2.id
        )

        item_change = self.data_store1.get_item_change_by_id(
            id=provider_in_test_update_i2_1.id
        )
        self.assertTrue(item_change.is_applied)
        self.assertTrue(item_change.should_ignore)

        item_change = self.data_store1.get_item_change_by_id(
            id=provider_in_test_update_i2_2.id
        )
        self.assertTrue(item_change.is_applied)
        self.assertTrue(item_change.should_ignore)

        item3 = self.data_store1.get_item_by_id(id=self.item3_id)
        self.assertEqual(
            item3,
            self.data_store1._create_item(
                id=str(self.item3_id), name="I3", version="other_provider_update_i3_1",
            ),
        )
        item3_version = self.data_store1.get_local_version(
            item_id=other_provider_change_i3.serialization_result.item_id
        )
        self.assertEqual(
            item3_version.current_item_change.id, other_provider_change_i3.id
        )
        self.assertNotEqual(self.data_store1, self.data_store2)

        # provider_in_test >> Updates item I2
        provider_in_test_update_i2_3 = self.data_store2.commit_item_change(
            operation=Operation.UPDATE,
            entity_name="my_app_item",
            item_id=self.item2_id,
            item=self.data_store2._create_item(
                id=str(self.item2_id),
                name="I2",
                version="provider_in_test_update_i2_3",
            ),
        )

        # Synchronization runs again, no exception this time
        num_changes1 = len(self.data_store1.get_item_changes())
        num_items1 = len(self.data_store1.get_items())
        num_conflict_logs1 = len(self.data_store1.get_conflict_logs())
        num_changes2 = len(self.data_store2.get_item_changes())
        num_items2 = len(self.data_store2.get_items())
        num_conflict_logs2 = len(self.data_store2.get_conflict_logs())
        self.orchestrator.run(initial_source_provider_id="provider_in_test")

        # Checking data_store2 (provider_in_test)
        self.assertEqual(num_changes2, len(self.data_store2.get_item_changes()))
        with self.assertRaises(ItemNotFoundException):
            self.data_store2.get_item_by_id(id=self.item2_id)

        item2_version = self.data_store2.get_local_version(
            item_id=provider_in_test_update_i2_3.serialization_result.item_id
        )
        self.assertEqual(
            item2_version.current_item_change.id, other_provider_delete_i2.id
        )

        self.assertEqual(
            num_conflict_logs2 + 1, len(self.data_store2.get_conflict_logs())
        )
        self.assertEqual(num_items2 - 1, len(self.data_store2.get_items()))

        conflict_log = self.data_store2.get_conflict_logs()[0]
        self.assertEqual(conflict_log.status, ConflictStatus.RESOLVED)
        self.assertEqual(conflict_log.conflict_type, ConflictType.EXCEPTION_OCCURRED)
        self.assertEqual(conflict_log.item_change_loser.id, other_provider_delete_i2.id)

        conflict_log = self.data_store2.get_conflict_logs()[-1]
        self.assertEqual(
            conflict_log.item_change_loser.id, provider_in_test_update_i2_3.id
        )
        self.assertEqual(conflict_log.status, ConflictStatus.RESOLVED)

        item_change = self.data_store2.get_item_change_by_id(
            id=other_provider_delete_i2.id
        )
        self.assertTrue(item_change.is_applied)
        self.assertFalse(item_change.should_ignore)

        item_change = self.data_store2.get_item_change_by_id(
            id=provider_in_test_update_i2_3.id
        )
        self.assertTrue(item_change.is_applied)
        self.assertTrue(item_change.should_ignore)

        # Checking data_store1 (other_provider)
        self.assertEqual(num_changes1 + 1, len(self.data_store1.get_item_changes()))
        with self.assertRaises(ItemNotFoundException):
            self.data_store1.get_item_by_id(id=self.item2_id)

        item2_version = self.data_store1.get_local_version(
            item_id=other_provider_delete_i2.serialization_result.item_id
        )
        self.assertEqual(
            item2_version.current_item_change.id, other_provider_delete_i2.id
        )

        self.assertEqual(
            num_conflict_logs1 + 1, len(self.data_store1.get_conflict_logs())
        )
        self.assertEqual(num_items1, len(self.data_store1.get_items()))

        conflict_log = self.data_store1.get_conflict_logs()[-1]
        self.assertEqual(
            conflict_log.item_change_loser.id, provider_in_test_update_i2_3.id
        )
        self.assertEqual(
            conflict_log.item_change_winner.id, other_provider_delete_i2.id
        )

        item_change = self.data_store1.get_item_change_by_id(
            id=provider_in_test_update_i2_3.id
        )
        self.assertTrue(item_change.is_applied)
        self.assertTrue(item_change.should_ignore)

        self.assertEqual(self.data_store1, self.data_store2)

    def test_conflict_exception3(self):
        """
            Simulates the situation:
                - other_provider >> Updates item I2
                - provider_in_test >> Deletes item I2
                - other_provider >> Updates item I2

                - Sinchronization: other_provider (1) -> provider_in_test (2)
                    - The item changes generate conflicts that are resolved.

                - Sinchronization: provider_in_test (2) -> other_provider (1)
                    - Exception is raised while executing deletion of item I2

                - other_provider >> Updates item I2

                - Sinchronization: other_provider (1) -> provider_in_test (2)
                    - Update ItemChange is saved but ignored
                    - 1 ConflictLog is created and resolved

                - Sinchronization: provider_in_test (2) -> other_provider (1)
                    - No exception is raised this time
                    - Item I2 is deleted
                    - Exception ConflictLog is marked as resolved
                    - Another ConflictLog is created and is also resolved

        """
        # Performing initial full sync
        # Both data stores will have the objects: I1, I2, I3, I104 e I105
        self.orchestrator.run(initial_source_provider_id="other_provider")

        # other_provider >> Updates item I2
        other_provider_update_i2_1 = self.data_store1.commit_item_change(
            operation=Operation.UPDATE,
            entity_name="my_app_item",
            item_id=self.item2_id,
            item=self.data_store1._create_item(
                id=str(self.item2_id), name="I2", version="other_provider_update_i2_1",
            ),
        )

        # provider_in_test >> Deletes item I2
        provider_in_test_delete_i2 = self.data_store2.commit_item_change(
            operation=Operation.DELETE,
            entity_name="my_app_item",
            item_id=self.item2_id,
            item=self.data_store2._create_item(
                id=str(self.item2_id), name=None, version=None
            ),
        )

        # other_provider >> Updates item I2
        other_provider_update_i2_2 = self.data_store1.commit_item_change(
            operation=Operation.UPDATE,
            entity_name="my_app_item",
            item_id=self.item2_id,
            item=self.data_store1._create_item(
                id=str(self.item2_id), name="I2", version="other_provider_update_i2_2",
            ),
        )

        # Sinchronization: other_provider (1) -> provider_in_test (2) runs without trouble
        # During the sync session provider_in_test (2) -> other_provider (1), an exception is raised when executing deletion of I2

        original = self.data_store1.execute_item_change

        def execute_item_change_mock(item_change: "ItemChange"):
            if str(item_change.id) == str(provider_in_test_delete_i2.id):
                raise ValueError("Error!")
            else:
                original(item_change)

        self.data_store1.execute_item_change = execute_item_change_mock

        self.events_manager1.raise_exception = False
        num_changes1 = len(self.data_store1.get_item_changes())
        num_items1 = len(self.data_store1.get_items())
        num_conflict_logs1 = len(self.data_store1.get_conflict_logs())
        num_changes2 = len(self.data_store2.get_item_changes())
        num_items2 = len(self.data_store2.get_items())
        num_conflict_logs2 = len(self.data_store2.get_conflict_logs())
        self.orchestrator.run(initial_source_provider_id="other_provider")
        self.events_manager1.raise_exception = True

        self.data_store1.execute_item_change = original

        # Expected results:
        #   Provider 1:
        #       - 1 conflict log with status DEFERRED is created
        #   Provider 2:
        #       - 2 conflict logs with status RESOLVED are created

        # Checking data_store2 (provider_in_test)
        self.assertEqual(num_changes2 + 2, len(self.data_store2.get_item_changes()))
        with self.assertRaises(ItemNotFoundException):
            self.data_store2.get_item_by_id(id=self.item2_id)

        item2_version = self.data_store2.get_local_version(item_id=self.item2_id)
        self.assertEqual(
            item2_version.current_item_change.id, provider_in_test_delete_i2.id
        )

        self.assertEqual(
            num_conflict_logs2 + 2, len(self.data_store2.get_conflict_logs())
        )
        self.assertEqual(num_items2, len(self.data_store2.get_items()))

        conflict_log = self.data_store2.get_conflict_logs()[0]
        self.assertEqual(conflict_log.status, ConflictStatus.RESOLVED)
        self.assertEqual(
            conflict_log.conflict_type, ConflictType.LOCAL_DELETE_REMOTE_UPDATE
        )
        self.assertEqual(
            conflict_log.item_change_loser.id, other_provider_update_i2_1.id
        )

        conflict_log = self.data_store2.get_conflict_logs()[-1]
        self.assertEqual(
            conflict_log.item_change_loser.id, other_provider_update_i2_2.id
        )
        self.assertEqual(
            conflict_log.item_change_winner.id, provider_in_test_delete_i2.id
        )
        self.assertEqual(
            conflict_log.conflict_type, ConflictType.LOCAL_DELETE_REMOTE_UPDATE
        )
        self.assertEqual(conflict_log.status, ConflictStatus.RESOLVED)

        item_change = self.data_store2.get_item_change_by_id(
            id=provider_in_test_delete_i2.id
        )
        self.assertTrue(item_change.is_applied)
        self.assertFalse(item_change.should_ignore)

        item_change = self.data_store2.get_item_change_by_id(
            id=other_provider_update_i2_1.id
        )
        self.assertTrue(item_change.is_applied)
        self.assertTrue(item_change.should_ignore)

        item_change = self.data_store2.get_item_change_by_id(
            id=other_provider_update_i2_2.id
        )
        self.assertTrue(item_change.is_applied)
        self.assertTrue(item_change.should_ignore)

        # Checking data_store1 (other_provider)
        self.assertEqual(num_changes1 + 1, len(self.data_store1.get_item_changes()))
        item2 = self.data_store1.get_item_by_id(id=self.item2_id)
        self.assertEqual(
            item2,
            self.data_store1._create_item(
                id=str(self.item2_id), name="I2", version="other_provider_update_i2_2",
            ),
        )

        item2_version = self.data_store1.get_local_version(item_id=self.item2_id)
        self.assertEqual(
            item2_version.current_item_change.id, other_provider_update_i2_2.id
        )

        self.assertEqual(
            num_conflict_logs1 + 1, len(self.data_store1.get_conflict_logs())
        )
        self.assertEqual(num_items1, len(self.data_store1.get_items()))

        conflict_log = self.data_store1.get_conflict_logs()[-1]
        self.assertEqual(
            conflict_log.item_change_loser.id, provider_in_test_delete_i2.id
        )
        self.assertEqual(conflict_log.status, ConflictStatus.DEFERRED)

        item_change = self.data_store1.get_item_change_by_id(
            id=provider_in_test_delete_i2.id
        )
        self.assertFalse(item_change.is_applied)
        self.assertFalse(item_change.should_ignore)

        item_change = self.data_store1.get_item_change_by_id(
            id=other_provider_update_i2_1.id
        )
        self.assertTrue(item_change.is_applied)
        self.assertFalse(item_change.should_ignore)

        item_change = self.data_store1.get_item_change_by_id(
            id=other_provider_update_i2_2.id
        )
        self.assertTrue(item_change.is_applied)
        self.assertTrue(item_change.should_ignore)

        # other_provider >> Updates item I2
        other_provider_update_i2_3 = self.data_store1.commit_item_change(
            operation=Operation.UPDATE,
            entity_name="my_app_item",
            item_id=self.item2_id,
            item=self.data_store1._create_item(
                id=str(self.item2_id), name="I2", version="other_provider_update_i2_3",
            ),
        )

        # Sinchronization: other_provider (1) -> provider_in_test (2) runs without trouble
        # Sinchronization: provider_in_test (2) -> other_provider (1) runs without trouble

        num_changes1 = len(self.data_store1.get_item_changes())
        num_items1 = len(self.data_store1.get_items())
        num_conflict_logs1 = len(self.data_store1.get_conflict_logs())
        num_changes2 = len(self.data_store2.get_item_changes())
        num_items2 = len(self.data_store2.get_items())
        num_conflict_logs2 = len(self.data_store2.get_conflict_logs())

        self.orchestrator.run(initial_source_provider_id="other_provider")

        # Expected results:
        #   Provider 2
        #     - Update ItemChange is saved but ignored
        #     - 1 ConflictLog is created and resolved
        #
        #   Provider 1
        #     - No new ItemChange is created
        #     - Item I2 is deleted
        #     - Exception ConflictLog is marked as resolved
        #     - Another ConflictLog is created and is also resolved

        # Checking data_store2 (provider_in_test)
        self.assertEqual(num_changes2 + 1, len(self.data_store2.get_item_changes()))
        with self.assertRaises(ItemNotFoundException):
            self.data_store2.get_item_by_id(id=self.item2_id)

        item2_version = self.data_store2.get_local_version(item_id=self.item2_id)
        self.assertEqual(
            item2_version.current_item_change.id, provider_in_test_delete_i2.id
        )

        self.assertEqual(
            num_conflict_logs2 + 1, len(self.data_store2.get_conflict_logs())
        )
        self.assertEqual(num_items2, len(self.data_store2.get_items()))

        conflict_log = self.data_store2.get_conflict_logs()[-1]
        self.assertEqual(conflict_log.status, ConflictStatus.RESOLVED)
        self.assertEqual(
            conflict_log.conflict_type, ConflictType.LOCAL_DELETE_REMOTE_UPDATE
        )
        self.assertEqual(
            conflict_log.item_change_loser.id, other_provider_update_i2_3.id
        )
        self.assertEqual(
            conflict_log.item_change_winner.id, provider_in_test_delete_i2.id
        )

        item_change = self.data_store2.get_item_change_by_id(
            id=provider_in_test_delete_i2.id
        )
        self.assertTrue(item_change.is_applied)
        self.assertFalse(item_change.should_ignore)

        item_change = self.data_store2.get_item_change_by_id(
            id=other_provider_update_i2_3.id
        )
        self.assertTrue(item_change.is_applied)
        self.assertTrue(item_change.should_ignore)

        # Checking data_store1 (other_provider)
        self.assertEqual(num_changes1, len(self.data_store1.get_item_changes()))

        with self.assertRaises(ItemNotFoundException):
            self.data_store1.get_item_by_id(id=self.item2_id)

        item2_version = self.data_store1.get_local_version(item_id=self.item2_id)
        self.assertEqual(
            item2_version.current_item_change.id, provider_in_test_delete_i2.id
        )

        self.assertEqual(
            num_conflict_logs1 + 1, len(self.data_store1.get_conflict_logs())
        )
        self.assertEqual(num_items1 - 1, len(self.data_store1.get_items()))

        conflict_log = self.data_store1.get_conflict_logs()[-2]
        self.assertEqual(conflict_log.status, ConflictStatus.RESOLVED)
        self.assertEqual(conflict_log.conflict_type, ConflictType.EXCEPTION_OCCURRED)
        self.assertEqual(
            conflict_log.item_change_loser.id, provider_in_test_delete_i2.id
        )

        conflict_log = self.data_store1.get_conflict_logs()[-1]
        self.assertEqual(conflict_log.status, ConflictStatus.RESOLVED)
        self.assertEqual(
            conflict_log.conflict_type, ConflictType.LOCAL_UPDATE_REMOTE_DELETE
        )
        self.assertEqual(
            conflict_log.item_change_loser.id, other_provider_update_i2_3.id
        )
        self.assertEqual(
            conflict_log.item_change_winner.id, provider_in_test_delete_i2.id
        )

        item_change = self.data_store1.get_item_change_by_id(
            id=provider_in_test_delete_i2.id
        )
        self.assertTrue(item_change.is_applied)
        self.assertFalse(item_change.should_ignore)

        item_change = self.data_store1.get_item_change_by_id(
            id=other_provider_update_i2_3.id
        )
        self.assertTrue(item_change.is_applied)
        self.assertTrue(item_change.should_ignore)

    def test_sync_timeout(self):
        """Simulates a timeout during a sync session.
            The first batch of changes is processed correctly but the second one leads to a timeout.
        """

        self.events_manager2.raise_exception = False
        self.events_manager1.raise_exception = False
        num_changes1 = len(self.data_store1.get_item_changes())
        num_items1 = len(self.data_store1.get_items())
        num_conflict_logs1 = len(self.data_store1.get_conflict_logs())
        num_sessions1 = len(self.data_store1.get_sync_sessions())
        num_changes2 = len(self.data_store2.get_item_changes())
        num_items2 = len(self.data_store2.get_items())
        num_conflict_logs2 = len(self.data_store2.get_conflict_logs())
        num_sessions2 = len(self.data_store2.get_sync_sessions())

        self.orchestrator.maximum_duration_seconds = 300
        timers = [
            0,  # clock_start
            10,  # deferred_tick
            100,  # other_provider_page1_tick
            500,  # other_provider_page2_tick
            0,  # clock_start
            100,  # deferred_tick
            200,  # provider_in_test_page1_tick
            250,  # provider_in_test_page2_tick
        ]  # [clock_start, deferred_tick, other_provider_page1_tick, other_provider_page2_tick, clock_start, deferred_tick, provider_in_test_page1_tick, provider_in_test_page2_tick]

        with unittest.mock.patch("time.time", side_effect=timers):
            self.orchestrator.run(initial_source_provider_id="other_provider")

        # Expected results:
        #   Provider 1:
        #       - All 6 changes are saved
        #       - SyncSession finishes successfully
        #   Provider 2:
        #       - Only 5 changes are saved and applied
        #       - SyncSession finishes with a failure

        # data_store1
        self.assertEqual(num_changes1 + 6, len(self.data_store1.get_item_changes()))
        self.assertEqual(num_items1 + 2, len(self.data_store1.get_items()))
        self.assertEqual(num_conflict_logs1, len(self.data_store1.get_conflict_logs()))
        self.assertEqual(num_sessions1 + 2, len(self.data_store1.get_sync_sessions()))
        sync_sessions = self.data_store1.get_sync_sessions()

        self.assertEqual(sync_sessions[0].status, SyncSessionStatus.FAILED)
        self.assertEqual(sync_sessions[0].source_provider_id, "other_provider")
        self.assertEqual(sync_sessions[0].target_provider_id, "provider_in_test")

        self.assertEqual(sync_sessions[1].status, SyncSessionStatus.FINISHED)
        self.assertEqual(sync_sessions[1].source_provider_id, "provider_in_test")
        self.assertEqual(sync_sessions[1].target_provider_id, "other_provider")

        # data_store2
        self.assertEqual(num_changes2 + 5, len(self.data_store2.get_item_changes()))
        self.assertEqual(num_items2 + 1, len(self.data_store2.get_items()))
        self.assertEqual(num_conflict_logs2, len(self.data_store2.get_conflict_logs()))
        self.assertEqual(num_sessions2 + 2, len(self.data_store2.get_sync_sessions()))
        sync_sessions = self.data_store2.get_sync_sessions()
        self.assertEqual(sync_sessions[0].status, SyncSessionStatus.FAILED)
        self.assertEqual(sync_sessions[0].source_provider_id, "other_provider")
        self.assertEqual(sync_sessions[0].target_provider_id, "provider_in_test")
        self.assertEqual(sync_sessions[1].status, SyncSessionStatus.FINISHED)
        self.assertEqual(sync_sessions[1].source_provider_id, "provider_in_test")
        self.assertEqual(sync_sessions[1].target_provider_id, "other_provider")

        with self.assertRaises(ItemNotFoundException):
            self.data_store2.get_item_by_id(id=self.item2_id)


class QueryFullSyncTest(BackendTestMixin, unittest.TestCase):
    def setUp(self):
        # Provider 1
        self.data_store1 = cast(
            "Union[TestDataStoreMixin, TrackQueriesStoreMixin, BaseDataStore]",
            self._create_data_store(local_provider_id="other_provider",),
        )
        self.events_manager1 = DebugEventsManager(data_store=self.data_store1)
        self.changes_executor1 = ChangesExecutor(
            data_store=self.data_store1,
            events_manager=self.events_manager1,
            conflict_resolver=ConflictResolver(),
        )
        self.other_provider = self._create_provider(
            provider_id="other_provider",
            data_store=self.data_store1,
            events_manager=self.events_manager1,
            changes_executor=self.changes_executor1,
            max_num=5,
        )

        # Provider 2
        self.data_store2 = cast(
            "Union[TestDataStoreMixin, TrackQueriesStoreMixin, BaseDataStore]",
            self._create_data_store(local_provider_id="provider_in_test",),
        )
        self.events_manager2 = DebugEventsManager(data_store=self.data_store2)
        self.changes_executor2 = ChangesExecutor(
            data_store=self.data_store2,
            events_manager=self.events_manager2,
            conflict_resolver=ConflictResolver(),
        )
        self.provider_in_test = self._create_provider(
            provider_id="provider_in_test",
            data_store=self.data_store2,
            events_manager=self.events_manager2,
            changes_executor=self.changes_executor2,
            max_num=5,
        )

        self.sync_lock = self._create_sync_lock()
        self.orchestrator = SyncOrchestrator(
            sync_lock=self.sync_lock,
            providers=[self.other_provider, self.provider_in_test],
            maximum_duration_seconds=5 * 60,
        )

    def _test_sync_query(
        self, source_provider: "BaseSyncProvider", target_provider: "BaseSyncProvider"
    ):
        """Simulates the situation where changes are selected and a query is used to filter the results.

            The timeline is:
                a)
                    - There are 5 items, 3 of them with versions less than 5 (2,3,4) and
                    2 with versions more or equal to 5 (5, 6)
                    - A query is made for 2 items with versions less than 5 ordered by version
                        - Only the changes related to items with version 2 and 3 are returned

                b)
                    - A new item with version 1 is added
                    - The item that had version 3 is updated to version 4
                    - A new query (using the vector clock from the previous request) is made
                        - Both the changes related to the new item and to the updated item
                        are listed

                c)
                    - Item with version 4 is deleted
                    - Item with version 1 is updated (but its version remains the same)
                    - A new query (using the vector clock from the previous request) is made
                        - The change related to the deletion is not returned (as its no longer
                        part of the query)
                        - The change related to the update is returned

                d)
                    - A new query without a vector clock is made for the next 2 items
                        - The changes related to item with version 4 are returned (both INSERT and DELETE)

                e)
                    - The target data store updates item with version 1 to version 4
                        - The target vector clock for both queries is updated accordingly

        """

        # Setup
        source_data_store = source_provider.data_store
        target_data_store = target_provider.data_store

        item1 = cast("TestDataStoreMixin", source_data_store)._create_item(
            id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="2",
        )
        item1_change1 = cast("BaseDataStore", source_data_store).commit_item_change(
            operation=Operation.INSERT,
            entity_name="my_app_item",
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            item=item1,
        )

        item2 = cast("TestDataStoreMixin", source_data_store)._create_item(
            id="6ec0755a-2ca9-407e-87fa-0bd73db8c29f", name="item_2", version="3",
        )
        item2_change1 = cast("BaseDataStore", source_data_store).commit_item_change(
            operation=Operation.INSERT,
            entity_name="my_app_item",
            item_id="6ec0755a-2ca9-407e-87fa-0bd73db8c29f",
            item=item2,
        )

        item3 = cast("TestDataStoreMixin", source_data_store)._create_item(
            id="b45d1471-819b-4048-b888-ddfefde02485", name="item_3", version="4",
        )
        item3_change1 = cast("BaseDataStore", source_data_store).commit_item_change(
            operation=Operation.INSERT,
            entity_name="my_app_item",
            item_id="b45d1471-819b-4048-b888-ddfefde02485",
            item=item3,
        )

        item4 = cast("TestDataStoreMixin", source_data_store)._create_item(
            id="72e754b6-6f75-4999-bc1d-5709e244a52e", name="item_4", version="5",
        )
        item4_change1 = cast("BaseDataStore", source_data_store).commit_item_change(
            operation=Operation.INSERT,
            entity_name="my_app_item",
            item_id="72e754b6-6f75-4999-bc1d-5709e244a52e",
            item=item4,
        )

        item5 = cast("TestDataStoreMixin", source_data_store)._create_item(
            id="18e13474-d6a5-4d1c-8624-38cb45add4d6", name="item_5", version="6",
        )
        item5_change1 = cast("BaseDataStore", source_data_store).commit_item_change(
            operation=Operation.INSERT,
            entity_name="my_app_item",
            item_id="18e13474-d6a5-4d1c-8624-38cb45add4d6",
            item=item5,
        )

        # a)
        query1 = Query(
            entity_name="my_app_item",
            filter=Filter(
                children=[
                    Comparison(
                        field_name="version",
                        comparator=Comparator.LESS_THAN,
                        value="5",
                    )
                ]
            ),
            ordering=[SortOrder(field_name="version")],
            limit=2,
            offset=None,
        )
        cast("TrackQueriesStoreMixin", target_data_store).start_tracking_query(query1)

        num_changes_target = len(target_data_store.get_item_changes())
        num_items_target = len(target_data_store.get_items())

        self.orchestrator.synchronize_providers(
            source_provider_id=source_provider.provider_id,
            target_provider_id=target_provider.provider_id,
            query=query1,
        )

        item_changes = target_data_store.get_item_changes()
        items = target_data_store.get_items()
        self.assertEqual(num_changes_target + 2, len(item_changes))
        self.assertEqual(num_items_target + 2, len(items))
        self.assertEqual(item_changes, [item1_change1, item2_change1])

        self.assertEqual(
            make_hashable(items),
            make_hashable(
                [
                    cast("TestDataStoreMixin", target_data_store)._create_item(
                        id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
                        name="item_1",
                        version="2",
                    ),
                    cast("TestDataStoreMixin", target_data_store)._create_item(
                        id="6ec0755a-2ca9-407e-87fa-0bd73db8c29f",
                        name="item_2",
                        version="3",
                    ),
                ]
            ),
        )

        # b)
        item6 = cast("TestDataStoreMixin", source_data_store)._create_item(
            id="75fdef62-6302-429a-9973-4f7f4d6a0e8f", name="item_6", version="1",
        )
        item6_change1 = cast("BaseDataStore", source_data_store).commit_item_change(
            operation=Operation.INSERT,
            entity_name="my_app_item",
            item_id="18e13474-d6a5-4d1c-8624-38cb45add4d6",
            item=item6,
        )

        item2 = cast("TestDataStoreMixin", source_data_store)._create_item(
            id="6ec0755a-2ca9-407e-87fa-0bd73db8c29f", name="item_2", version="4",
        )
        item2_change2 = cast("BaseDataStore", source_data_store).commit_item_change(
            operation=Operation.UPDATE,
            entity_name="my_app_item",
            item_id="6ec0755a-2ca9-407e-87fa-0bd73db8c29f",
            item=item2,
        )

        num_changes_target = len(target_data_store.get_item_changes())
        num_items_target = len(target_data_store.get_items())

        self.orchestrator.synchronize_providers(
            source_provider_id=source_provider.provider_id,
            target_provider_id=target_provider.provider_id,
            query=query1,
        )

        item_changes = target_data_store.get_item_changes()
        items = target_data_store.get_items()
        self.assertEqual(num_changes_target + 2, len(item_changes))
        self.assertEqual(num_items_target + 1, len(items))
        self.assertEqual(
            item_changes, [item1_change1, item2_change1, item6_change1, item2_change2],
        )

        self.assertEqual(
            make_hashable(items),
            make_hashable(
                [
                    cast("TestDataStoreMixin", target_data_store)._create_item(
                        id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
                        name="item_1",
                        version="2",
                    ),
                    cast("TestDataStoreMixin", target_data_store)._create_item(
                        id="6ec0755a-2ca9-407e-87fa-0bd73db8c29f",
                        name="item_2",
                        version="4",
                    ),
                    cast("TestDataStoreMixin", target_data_store)._create_item(
                        id="75fdef62-6302-429a-9973-4f7f4d6a0e8f",
                        name="item_6",
                        version="1",
                    ),
                ]
            ),
        )

        # c)
        item3_change2 = cast("BaseDataStore", source_data_store).commit_item_change(
            operation=Operation.DELETE,
            entity_name="my_app_item",
            item_id="b45d1471-819b-4048-b888-ddfefde02485",
            item=item3,
        )
        item6 = cast("TestDataStoreMixin", source_data_store)._create_item(
            id="75fdef62-6302-429a-9973-4f7f4d6a0e8f",
            name="item_6_updated",
            version="1",
        )
        item6_change2 = cast("BaseDataStore", source_data_store).commit_item_change(
            operation=Operation.UPDATE,
            entity_name="my_app_item",
            item_id="18e13474-d6a5-4d1c-8624-38cb45add4d6",
            item=item6,
        )

        num_changes_target = len(target_data_store.get_item_changes())
        num_items_target = len(target_data_store.get_items())

        self.orchestrator.synchronize_providers(
            source_provider_id=source_provider.provider_id,
            target_provider_id=target_provider.provider_id,
            query=query1,
        )

        item_changes = target_data_store.get_item_changes()
        items = target_data_store.get_items()

        self.assertEqual(num_changes_target + 1, len(item_changes))
        self.assertEqual(num_items_target, len(items))
        self.assertEqual(
            item_changes,
            [
                item1_change1,
                item2_change1,
                item6_change1,
                item2_change2,
                item6_change2,
            ],
        )
        self.assertEqual(
            make_hashable(items),
            make_hashable(
                [
                    cast("TestDataStoreMixin", target_data_store)._create_item(
                        id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
                        name="item_1",
                        version="2",
                    ),
                    cast("TestDataStoreMixin", target_data_store)._create_item(
                        id="6ec0755a-2ca9-407e-87fa-0bd73db8c29f",
                        name="item_2",
                        version="4",
                    ),
                    cast("TestDataStoreMixin", target_data_store)._create_item(
                        id="75fdef62-6302-429a-9973-4f7f4d6a0e8f",
                        name="item_6_updated",
                        version="1",
                    ),
                ]
            ),
        )

        # d)
        query2 = Query(
            entity_name="my_app_item",
            filter=Filter(
                children=[
                    Comparison(
                        field_name="version",
                        comparator=Comparator.LESS_THAN,
                        value="5",
                    )
                ]
            ),
            ordering=[SortOrder(field_name="version")],
            limit=2,
            offset=2,
        )
        cast("TrackQueriesStoreMixin", target_data_store).start_tracking_query(query2)
        num_changes_target = len(target_data_store.get_item_changes())
        num_items_target = len(target_data_store.get_items())

        self.orchestrator.synchronize_providers(
            source_provider_id=source_provider.provider_id,
            target_provider_id=target_provider.provider_id,
            query=query2,
        )

        item_changes = target_data_store.get_item_changes()
        items = target_data_store.get_items()
        self.assertEqual(num_changes_target + 2, len(item_changes))
        self.assertEqual(num_items_target, len(items))
        self.assertEqual(
            item_changes,
            [
                item1_change1,
                item2_change1,
                item6_change1,
                item2_change2,
                item6_change2,
                item3_change1,
                item3_change2,
            ],
        )
        self.assertEqual(
            make_hashable(items),
            make_hashable(
                [
                    cast("TestDataStoreMixin", target_data_store)._create_item(
                        id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
                        name="item_1",
                        version="2",
                    ),
                    cast("TestDataStoreMixin", target_data_store)._create_item(
                        id="6ec0755a-2ca9-407e-87fa-0bd73db8c29f",
                        name="item_2",
                        version="4",
                    ),
                    cast("TestDataStoreMixin", target_data_store)._create_item(
                        id="75fdef62-6302-429a-9973-4f7f4d6a0e8f",
                        name="item_6_updated",
                        version="1",
                    ),
                ]
            ),
        )

        # e)
        item6 = cast("TestDataStoreMixin", target_data_store)._create_item(
            id="75fdef62-6302-429a-9973-4f7f4d6a0e8f",
            name="item_6_updated",
            version="4",
        )

        item6_change3 = target_data_store.commit_item_change(
            operation=Operation.UPDATE,
            entity_name="my_app_item",
            item_id="75fdef62-6302-429a-9973-4f7f4d6a0e8f",
            item=item6,
        )

        vector_clock1 = target_data_store.get_local_vector_clock(query=query1)
        vector_clock2 = target_data_store.get_local_vector_clock(query=query2)

        self.assertEqual(
            vector_clock1.get_vector_clock_item(
                provider_id=target_provider.provider_id
            ),
            item6_change3.change_vector_clock_item,
        )

        self.assertEqual(
            vector_clock2.get_vector_clock_item(
                provider_id=target_provider.provider_id
            ),
            item6_change3.change_vector_clock_item,
        )

    def _test_overlapping_queries(
        self, source_provider: "BaseSyncProvider", target_provider: "BaseSyncProvider"
    ):
        """Simulates the situation where two overlapping queries are being tracked.

        The timeline is:
            a)
                - There are 2 items, one with version 3 and the other with version 5
                - The target data store starts tracking to queries, one that selects
                all items with version less or equal to 3 (query1) and one that selects all
                items with version less or equal to 6 (query2)
                - The target data store syncs both queries once

            b)
                - The item with version 5 is updated (but its version stays the same)
                - The item with version 3 is updated (but its version stays the same)
                - The target data store syncs query1
                    - The change to item with version 3 is returned

                - The target data store syncs query2
                    - Both the changes to items with versions 3 and 5 are returned

        """

        source_data_store = source_provider.data_store
        target_data_store = target_provider.data_store

        # a)

        item1 = cast("TestDataStoreMixin", source_data_store)._create_item(
            id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="3",
        )
        item1_change1 = cast("BaseDataStore", source_data_store).commit_item_change(
            operation=Operation.INSERT,
            entity_name="my_app_item",
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            item=item1,
        )

        item2 = cast("TestDataStoreMixin", source_data_store)._create_item(
            id="6ec0755a-2ca9-407e-87fa-0bd73db8c29f", name="item_2", version="5",
        )
        item2_change1 = cast("BaseDataStore", source_data_store).commit_item_change(
            operation=Operation.INSERT,
            entity_name="my_app_item",
            item_id="6ec0755a-2ca9-407e-87fa-0bd73db8c29f",
            item=item2,
        )

        query1 = Query(
            entity_name="my_app_item",
            filter=Filter(
                children=[
                    Comparison(
                        field_name="version",
                        comparator=Comparator.LESS_THAN_OR_EQUALS,
                        value="3",
                    )
                ]
            ),
            ordering=[SortOrder(field_name="version")],
            limit=2,
            offset=None,
        )
        cast("TrackQueriesStoreMixin", target_data_store).start_tracking_query(query1)

        query2 = Query(
            entity_name="my_app_item",
            filter=Filter(
                children=[
                    Comparison(
                        field_name="version",
                        comparator=Comparator.LESS_THAN_OR_EQUALS,
                        value="6",
                    )
                ]
            ),
            ordering=[SortOrder(field_name="version")],
            limit=2,
            offset=None,
        )
        cast("TrackQueriesStoreMixin", target_data_store).start_tracking_query(query2)

        item_changes = target_data_store.get_item_changes()
        items = target_data_store.get_items()
        num_changes_target = len(item_changes)
        num_items_target = len(items)

        self.orchestrator.synchronize_providers(
            source_provider_id=source_provider.provider_id,
            target_provider_id=target_provider.provider_id,
            query=query1,
        )

        item_changes = target_data_store.get_item_changes()
        items = target_data_store.get_items()
        self.assertEqual(num_changes_target + 1, len(item_changes))
        self.assertEqual(num_items_target + 1, len(items))
        self.assertEqual(item_changes, [item1_change1])
        self.assertEqual(
            make_hashable(items),
            make_hashable(
                [
                    cast("TestDataStoreMixin", target_data_store)._create_item(
                        id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
                        name="item_1",
                        version="3",
                    )
                ]
            ),
        )

        item_changes = target_data_store.get_item_changes()
        items = target_data_store.get_items()
        num_changes_target = len(item_changes)
        num_items_target = len(items)

        self.orchestrator.synchronize_providers(
            source_provider_id=source_provider.provider_id,
            target_provider_id=target_provider.provider_id,
            query=query2,
        )

        item_changes = target_data_store.get_item_changes()
        items = target_data_store.get_items()

        self.assertEqual(num_changes_target + 1, len(item_changes))
        self.assertEqual(num_items_target + 1, len(items))
        self.assertEqual(item_changes, [item1_change1, item2_change1])
        self.assertEqual(
            make_hashable(items),
            make_hashable(
                [
                    cast("TestDataStoreMixin", target_data_store)._create_item(
                        id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
                        name="item_1",
                        version="3",
                    ),
                    cast("TestDataStoreMixin", target_data_store)._create_item(
                        id="6ec0755a-2ca9-407e-87fa-0bd73db8c29f",
                        name="item_2",
                        version="5",
                    ),
                ]
            ),
        )

        # b)
        item2_updated = cast("TestDataStoreMixin", source_data_store)._create_item(
            id="6ec0755a-2ca9-407e-87fa-0bd73db8c29f",
            name="item_2_updated",
            version="5",
        )
        item2_change2 = cast("BaseDataStore", source_data_store).commit_item_change(
            operation=Operation.UPDATE,
            entity_name="my_app_item",
            item_id="6ec0755a-2ca9-407e-87fa-0bd73db8c29f",
            item=item2_updated,
        )

        item1_updated = cast("TestDataStoreMixin", source_data_store)._create_item(
            id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            name="item_1_update",
            version="3",
        )
        item1_change2 = cast("BaseDataStore", source_data_store).commit_item_change(
            operation=Operation.UPDATE,
            entity_name="my_app_item",
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            item=item1_updated,
        )

        item_changes = target_data_store.get_item_changes()
        items = target_data_store.get_items()
        num_changes_target = len(item_changes)
        num_items_target = len(items)

        self.orchestrator.synchronize_providers(
            source_provider_id=source_provider.provider_id,
            target_provider_id=target_provider.provider_id,
            query=query1,
        )

        item_changes = target_data_store.get_item_changes()
        items = target_data_store.get_items()
        self.assertEqual(num_changes_target + 1, len(item_changes))
        self.assertEqual(num_items_target, len(items))
        self.assertEqual(item_changes, [item1_change1, item2_change1, item1_change2])
        self.assertEqual(
            make_hashable(items),
            make_hashable(
                [
                    cast("TestDataStoreMixin", target_data_store)._create_item(
                        id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
                        name="item_1_update",
                        version="3",
                    ),
                    cast("TestDataStoreMixin", target_data_store)._create_item(
                        id="6ec0755a-2ca9-407e-87fa-0bd73db8c29f",
                        name="item_2",
                        version="5",
                    ),
                ]
            ),
        )

        item_changes = target_data_store.get_item_changes()
        items = target_data_store.get_items()
        num_changes_target = len(item_changes)
        num_items_target = len(items)

        self.orchestrator.synchronize_providers(
            source_provider_id=source_provider.provider_id,
            target_provider_id=target_provider.provider_id,
            query=query2,
        )

        item_changes = target_data_store.get_item_changes()
        items = target_data_store.get_items()
        self.assertEqual(num_changes_target + 1, len(item_changes))
        self.assertEqual(num_items_target, len(items))
        self.assertEqual(
            item_changes, [item1_change1, item2_change1, item1_change2, item2_change2]
        )
        self.assertEqual(
            make_hashable(items),
            make_hashable(
                [
                    cast("TestDataStoreMixin", target_data_store)._create_item(
                        id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
                        name="item_1_update",
                        version="3",
                    ),
                    cast("TestDataStoreMixin", target_data_store)._create_item(
                        id="6ec0755a-2ca9-407e-87fa-0bd73db8c29f",
                        name="item_2_updated",
                        version="5",
                    ),
                ]
            ),
        )

    def test_sync_query_source(self):
        self._test_sync_query(
            source_provider=self.provider_in_test, target_provider=self.other_provider
        )

    def test_sync_query_target(self):
        self._test_sync_query(
            source_provider=self.other_provider, target_provider=self.provider_in_test
        )

    def test_overlapping_queries_source(self):
        self._test_overlapping_queries(
            source_provider=self.provider_in_test, target_provider=self.other_provider
        )
