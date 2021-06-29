import tests.base_store
import tests.in_memory.base


class InMemoryStoreTest(
    tests.in_memory.base.InMemoryBackendTestMixin, tests.base_store.BaseStoreTest
):
    pass
