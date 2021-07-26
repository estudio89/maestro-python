import tests.base_full_sync
import tests.in_memory.base


class InMemoryFullSyncTest(
    tests.in_memory.base.InMemoryBackendTestMixin, tests.base_full_sync.FullSyncTest
):
    pass


class InMemoryQueryFullSyncTest(
    tests.in_memory.base.InMemoryBackendTestMixin,
    tests.base_full_sync.QueryFullSyncTest,
):
    pass
