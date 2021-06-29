from typing import ContextManager, Any
import dateutil.parser
from abc import ABC, abstractmethod
from sync_framework.core.exceptions import SyncTimeoutException
import time
import datetime as dt


class BaseSyncLock(ABC):
    """Prevents two synchronization sessions from running at the same time."""

    @abstractmethod
    def is_running(self) -> "bool":  # pragma: no cover
        """
        Indicates whether a synchronization is running.
        """

    @abstractmethod
    def lock(self) -> "ContextManager":
        """Returns a ContextManager that locks the execution.
        """


class BaseMetadataConverter(ABC):
    """Abstract class to be used for converting metadata objects used by the sync framework to records that can be saved to the data store and back."""

    @abstractmethod
    def to_metadata(self, record: "Any") -> "Any":  # pragma: no cover
        """Converts a record from the data store to a metadata object used by the framework.

        Args:
            record (Any): Data store native object.

        Returns:
            (Any): Metadata object.
        """

    @abstractmethod
    def to_record(self, metadata_object: "Any") -> "Any":  # pragma: no cover
        """Converts a metadata object used by the framework to a record that can be saved to the data store.

        Args:
            metadata_object (Any): Metadata object.

        Returns:
            (Any): Data store native object.
        """


class DateCreatedMixin:

    """Mixin used by metadata converters that store the date of creation of an object.
    """

    def get_date_created(self, metadata_object: "Any") -> "dt.datetime":
        """Retrieves the date of creation already associated to the object of returns the current UTC date.

        Args:
            metadata_object (Any): Metadata object.

        Returns:
            dt.datetime: Date of creation
        """
        if metadata_object.date_created:
            return metadata_object.date_created
        else:
            now_utc = get_now_utc()
            return now_utc


class SyncTimer:

    """Times the sync session to make sure it finishes before the timeout.

    Attributes:
        start_time (float): Moment when the timer was started (seconds since epoch).
        timeout_seconds (float): Maximum number of seconds for the timer.
    """

    start_time: "float"
    timeout_seconds: "float"

    def __init__(self, timeout_seconds):
        self.start_time = time.time()
        self.timeout_seconds = timeout_seconds

    def tick(self):
        """Performs a check to see if the timer has finished.

        Raises:
            SyncTimeoutException: If more time than the timeout has elapsed.
        """
        current_time = time.time()
        if current_time - self.start_time > self.timeout_seconds:
            raise SyncTimeoutException(
                elapsed_time=current_time - self.start_time,
                max_duration_seconds=self.timeout_seconds,
            )


def get_now_utc() -> "dt.datetime":
    """Current time in UTC.

    Returns:
        dt.datetime: Current time in UTC.
    """
    return dt.datetime.now(tz=dt.timezone.utc)

def parse_datetime(value: "str") -> "dt.datetime":
    """Converts a string to a dt.datetime object.

    Args:
        value (str): An ISO-8601 string.

    Returns:
        dt.datetime: The parsed dt.datetime
    """
    return dateutil.parser.isoparse(value)
