import maestro.backends.base_nosql.converters
import maestro.backends.base_nosql.serializer
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


class ConflictLogMetadataConverter(
    maestro.backends.base_nosql.converters.ConflictLogMetadataConverter
):
    date_converter_class = DateConverter


class VectorClockItemMetadataConverter(
    maestro.backends.base_nosql.converters.VectorClockItemMetadataConverter
):
    date_converter_class = DateConverter


class VectorClockMetadataConverter(
    maestro.backends.base_nosql.converters.VectorClockMetadataConverter
):
    date_converter_class = DateConverter

    def __init__(
        self,
        vector_clock_item_converter: "VectorClockItemMetadataConverter" = VectorClockItemMetadataConverter(),
    ):
        super().__init__(vector_clock_item_converter=vector_clock_item_converter)


class ItemChangeMetadataConverter(
    maestro.backends.base_nosql.converters.ItemChangeMetadataConverter
):
    date_converter_class = DateConverter

    def __init__(
        self,
        item_serializer: "maestro.backends.base_nosql.serializer.NoSQLItemSerializer",
        vector_clock_item_converter: "VectorClockItemMetadataConverter" = VectorClockItemMetadataConverter(),
        vector_clock_converter: "VectorClockMetadataConverter" = VectorClockMetadataConverter(),
    ):
        super().__init__(
            item_serializer=item_serializer,
            vector_clock_item_converter=vector_clock_item_converter,
            vector_clock_converter=vector_clock_converter,
        )


class TrackedQueryMetadataConverter(
    maestro.backends.base_nosql.converters.TrackedQueryMetadataConverter
):
    date_converter_class = DateConverter

    def __init__(
        self,
        vector_clock_converter: "VectorClockMetadataConverter" = VectorClockMetadataConverter(),
        query_converter: "maestro.backends.base_nosql.converters.QueryMetadataConverter" = maestro.backends.base_nosql.converters.QueryMetadataConverter(),
    ):
        super().__init__(
            vector_clock_converter=vector_clock_converter,
            query_converter=query_converter,
        )
