from maestro.backends.base_nosql.serializer import NoSQLItemSerializer
from typing import List


class MongoItemSerializer(NoSQLItemSerializer):
    def get_skip_fields(self) -> "List[str]":
        return super().get_skip_fields() + ["_lock"]