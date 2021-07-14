from maestro.core.utils import BaseMetadataConverter
from typing import Any, Type
import copy


class NullConverter(BaseMetadataConverter):
    def __init__(self, metadata_class: "Type"):
        self.metadata_class = metadata_class

    def to_metadata(self, record: "Any") -> "Any":
        return self.metadata_class(**record)

    def to_record(self, metadata_object: "Any") -> "Any":
        return copy.deepcopy(metadata_object.__dict__)
