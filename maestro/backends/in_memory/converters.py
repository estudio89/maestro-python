from maestro.core.utils import BaseMetadataConverter
from maestro.core.query.metadata import TrackedQuery
from typing import Any, Type, Dict
import copy


class NullConverter(BaseMetadataConverter):
    def __init__(self, metadata_class: "Type"):
        self.metadata_class = metadata_class

    def to_metadata(self, record: "Any") -> "Any":
        return self.metadata_class(**record)

    def to_record(self, metadata_object: "Any") -> "Any":
        return copy.deepcopy(metadata_object.__dict__)


class TrackedQueryConverter(BaseMetadataConverter):
    def to_metadata(self, record: "Dict") -> "TrackedQuery":
        return TrackedQuery(query=record["query"], vector_clock=record["vector_clock"])

    def to_record(self, metadata_object: "TrackedQuery") -> "Dict":
        return {
            "id": metadata_object.query.get_id(),
            "query": metadata_object.query,
            "vector_clock": metadata_object.vector_clock,
        }
