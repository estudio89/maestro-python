import maestro.backends.base_nosql.converters
from typing import Any, Optional
import datetime as dt


class DateConverter(maestro.backends.base_nosql.converters.DateConverter):
    def serialize_date(self, value: "Optional[dt.datetime]") -> "Optional[Any]":
        if value is None:
            return value

        serialized = value.astimezone(dt.timezone.utc).timestamp()
        return serialized

    def deserialize_date(self, value: "Optional[Any]") -> "Optional[dt.datetime]":
        if value is None:
            return value

        date = dt.datetime.fromtimestamp(value, dt.timezone.utc)
        return date


class SyncSessionMetadataConverter(
    maestro.backends.base_nosql.converters.SyncSessionMetadataConverter
):
    date_converter_class = DateConverter


class ItemVersionMetadataConverter(
    maestro.backends.base_nosql.converters.ItemVersionMetadataConverter
):
    date_converter_class = DateConverter


class ItemChangeMetadataConverter(
    maestro.backends.base_nosql.converters.ItemChangeMetadataConverter
):
    date_converter_class = DateConverter


class ConflictLogMetadataConverter(
    maestro.backends.base_nosql.converters.ConflictLogMetadataConverter
):
    date_converter_class = DateConverter


class VectorClockMetadataConverter(
    maestro.backends.base_nosql.converters.VectorClockMetadataConverter
):
    date_converter_class = DateConverter
