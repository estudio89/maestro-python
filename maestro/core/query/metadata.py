from maestro.core.metadata import VectorClock
from maestro.core.utils import make_hashable
from enum import Enum
from typing import List, Any, Union, Optional
import copy

class Comparator(Enum):
    """Represents a comparison operation that can be performed in a field."""

    EQUALS = "=="
    NOT_EQUALS = "!="
    LESS_THAN = "<"
    LESS_THAN_OR_EQUALS = "<="
    GREATER_THAN = ">"
    GREATER_THAN_OR_EQUALS = ">="
    IN = "in"


class Comparison:
    """Stores a comparison that can be done to a field, such as field1 > 2, field1 == 3, etc."""

    field_name: "str"
    comparator: "Comparator"
    value: "Any"

    def __init__(self, field_name: "str", comparator: "Comparator", value: "Any"):
        self.field_name = field_name
        self.comparator = comparator
        self.value = value

    def __str__(self):
        return f"{self.field_name} {self.comparator.value} {self.value}"

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self)

    def __hash__(self):
        return hash((self.__class__, self.comparator, make_hashable(self.value)))

    def __eq__(self, other):
        return (
            isinstance(other, Comparison)
            and self.comparator == other.comparator
            and self.value == other.value
        )

class Connector(Enum):
    """Represents the connection between filters when they are combined."""

    AND = "AND"
    OR = "OR"


class Filter:
    """Represents a filtering operation. The combined filters are stored in a tree where the leaves are always Comparison objects.

    Attributes:
        children (List[Union[Filter, Comparison]]): These are the nested filters that were combined into this one. If this is a single
        non-combined filter, it will contain a single Comparison instance.
        connector (TYPE): The connection between this filter's children. If this is a single non-combined filter, its operator will be
        equal to Connector.AND.
    """

    connector: "Connector"
    children: "List[Union[Filter, Comparison]]"

    def __init__(
        self,
        children: "List[Union[Filter, Comparison]]",
        connector: "Connector" = Connector.AND,
    ):
        self.children = children
        self.connector = connector

    def add(self, child: "Filter"):
        """Add another child to this filter.

        Args:
            child (Filter): the filter being added.

        """
        if child in self.children:
            return

        if child.connector == self.connector or len(child) == 1:
            self.children.extend(child.children)
        else:
            self.children.append(child)

    def combine(self, other: "Filter", connector: "Connector") -> "Filter":
        """Combines two filters using the given connector.

        Args:
            other (Filter): The filter being combined into this one
            connector (Connector): The connector to be used.

        Returns:
            Filter: The combined filter

        Raises:
            TypeError: If the instance passed is not a Filter
        """

        if not isinstance(other, Filter):
            raise TypeError(other)

        if not other:
            return copy.deepcopy(self)
        elif not self:
            return copy.deepcopy(other)

        combined = Filter(connector=connector, children=[])
        combined.add(self)
        combined.add(other)
        return combined

    def __or__(self, other: "Filter") -> "Filter":
        return self.combine(other, Connector.OR)

    def __and__(self, other: "Filter") -> "Filter":
        return self.combine(other, Connector.AND)

    def __str__(self):
        return "(%s: %s)" % (
            self.connector.value,
            ", ".join(str(child) for child in self.children),
        )

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self)

    def __len__(self):
        """Return the number of children this node has."""
        return len(self.children)

    def __bool__(self):
        """Return whether or not this node has children."""
        return bool(self.children)

    def __contains__(self, other):
        """Return True if 'other' is a direct child of this instance."""
        return other in self.children

    def __eq__(self, other):
        return (
            isinstance(other, Filter)
            and self.connector == other.connector
            and self.children == other.children
        )

    def __hash__(self):
        return hash((self.__class__, self.connector, *make_hashable(self.children),))


class SortOrder:
    """Stores an ordering instruction for a particular field"""

    field_name: "str"
    descending: "bool"

    def __init__(self, field_name: "str", descending: "bool" = False):
        self.field_name = field_name
        self.descending = descending

    def __hash__(self):
        return hash((self.field_name, self.descending))

    def __str__(self):
        return f"{self.field_name} -> {'ASC' if not self.descending else 'DESC'}"

    def __repr__(self):
        return self.__str__()


class Query:
    """Represents a query with an optional filter and an ordering.

    Attributes:
        filter (Filter): The filter that needs to be applied
        ordering (List[SortOrder]): The sort order that should be applied
    """

    filter: "Filter"
    ordering: "List[SortOrder]"
    entity_name: "str"
    limit: "Optional[Any]"
    offset: "Optional[Any]"

    def __init__(
        self,
        entity_name: "str",
        filter: "Filter",
        ordering: "List[SortOrder]",
        limit: "Optional[Any]",
        offset: "Optional[Any]",
    ):
        if limit is not None:
            assert ordering, "Can't define limit for unordered query!"
        if offset is not None:
            assert ordering, "Can't define offset for unordered query!"

        self.entity_name = entity_name
        self.filter = filter
        self.ordering = ordering
        self.limit = limit
        self.offset = offset

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f"Query(entity_name='{self.entity_name}', filter={self.filter}, ordering={self.ordering}, limit={self.limit}, offset={self.offset})"

    def __hash__(self):
        return hash(
            (
                self.filter,
                tuple(self.ordering),
                self.entity_name,
                self.limit,
                self.offset,
            )
        )

    def get_id(self):
        """Returns a unique identifier for this query."""

        return str(self.__hash__())


class TrackedQuery:
    query: "Query"
    vector_clock: "VectorClock"

    def __init__(self, query: "Query", vector_clock: "VectorClock"):
        self.query = query
        self.vector_clock = vector_clock

    def __repr__(self):
        return f"TrackedQuery(query={self.query}, vector_clock={self.vector_clock})"
