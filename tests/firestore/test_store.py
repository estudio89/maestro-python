import tests.base_store
import tests.firestore.base


class FirestoreStoreTest(
    tests.firestore.base.FirestoreBackendTestMixin,
    tests.base_store.BaseStoreTest,
    tests.firestore.base.FirestoreTestCase,
):
    supports_queries = False
