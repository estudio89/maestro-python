import unittest
import tests.base_full_sync
import tests.firestore.base
from sync_framework.core.metadata import SyncSessionStatus
from sync_framework.core.exceptions import ItemNotFoundException
from sync_framework.backends.firestore.utils import calculate_firestore_usage


class FirestoreFullSyncTest(
    tests.firestore.base.FirestoreBackendTestMixin,
    tests.base_full_sync.FullSyncTest,
    tests.firestore.base.FirestoreTestCase,
):
    def _full_sync_no_conflict_before_first_sync(self):
        self.data_store2._usage.reset()

    def _full_sync_no_conflict_after_first_sync(self):
        usage = self.data_store2._usage

        calculated_usage = calculate_firestore_usage(
            num_updates_inserts_sent=0,
            num_deletes_sent=0,
            num_updates_inserts_received=12,
            num_deletes_received=0,
            num_items_affected_received=3,
        )
        self.assertEqual(
            usage.num_writes, calculated_usage["num_writes"],
        )
        self.assertEqual(usage.num_reads, calculated_usage["num_reads"])

    def _full_sync_no_conflict_before_second_sync(self):
        self.data_store2._usage.reset()

    def _full_sync_no_conflict_after_second_sync(self):
        calculated_usage = calculate_firestore_usage(
            num_updates_inserts_sent=6,
            num_deletes_sent=0,
            num_updates_inserts_received=0,
            num_deletes_received=0,
            num_items_affected_received=0,
        )
        usage = self.data_store2._usage
        self.assertEqual(
            usage.num_writes, calculated_usage["num_writes"],
        )
        self.assertEqual(usage.num_reads, calculated_usage["num_reads"])

    def test_sync_timeout(self):
        """ Simula a ocorrência de um timeout durante a sincronização.
            A primeira página de alterações é processada corretamente porém a segunda causa um timeout.
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
            200,  # provider2_page1_tick
            250,  # provider2_page2_tick
            290,  # provider2_page3_tick
        ]  # [clock_start, deferred_tick, other_provider_page1_tick, other_provider_page2_tick, clock_start, deferred_tick, provider2_page1_tick, provider2_page2_tick]

        with unittest.mock.patch("time.time", side_effect=timers):
            self.orchestrator.run(initial_source_provider_id="other_provider")

        # Resultado esperado:
        #   Provider 1:
        #       - Todas as 6 alterações são salvas
        #       - SyncSession finalizada com sucesso
        #   Provider 2:
        #       - Apenas 5 alterações são salvas e aplicadas
        #       - SyncSession finalizada com falha

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
