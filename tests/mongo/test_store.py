import tests.base_store
import tests.mongo.base


class MongoStoreTest(
    tests.mongo.base.MongoBackendTestMixin,
    tests.base_store.BaseStoreTest,
    tests.mongo.base.MongoTestCase,
):
    supports_queries = False
