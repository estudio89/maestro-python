from typing import ContextManager, Any, TypeVar, Optional
import dateutil.parser
from abc import ABC, abstractmethod
from maestro.core.exceptions import SyncTimeoutException
import time
import datetime as dt
import re


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
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

regex = r'^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$'
match_iso8601 = re.compile(regex).match

def parse_datetime(value: "str") -> "dt.datetime":
    """Converts a string to a dt.datetime object.

    Args:
        value (str): An ISO-8601 string.

    Returns:
        dt.datetime: The parsed dt.datetime
    """
    if not match_iso8601(value):
        raise ValueError("Not an ISO-8601 string")

    return dateutil.parser.isoparse(value)

T = TypeVar("T")

def cast_away_optional(arg: Optional[T]) -> T:
    assert arg is not None
    return arg


def is_iterable(value: "Any"):
    """Checks if a value is an iterable"""
    try:
        iter(value)
    except TypeError:
        return False
    else:
        return True

def make_hashable(value):
    """Attempts to make a value hashable.
    If the value is a dictionary, it will be converted to a tuple (key, val) and the dict values will be made hashable recursively.
    If the value is a non-hashable iterable, it will be converted to a tuble and its values will be made hashable recursively.
    """

    if isinstance(value, dict):
        return tuple(
            [(key, make_hashable(nested_value)) for key, nested_value in value.items()]
        )
    # Try hash to avoid converting a hashable iterable (e.g. string, frozenset)
    # to a tuple.
    try:
        hash(value)
    except TypeError:
        if is_iterable(value):
            return tuple(map(make_hashable, value))
        # Non-hashable, non-iterable.
        raise
    return value