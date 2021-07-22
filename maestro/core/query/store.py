from maestro.core.utils import BaseMetadataConverter
from maestro.core.query.metadata import Query, TrackedQuery, VectorClock
from maestro.core.query.utils import query_filter_to_lambda
from maestro.core.metadata import ItemChange, Operation
from maestro.core.store import BaseDataStore
from typing import Any, Optional, Callable, List, cast, Set
from abc import abstractmethod
import copy

class TrackQueriesStoreMixin:
    """ A mixin that allows a data store to track queries."""

    tracked_query_metadata_converter: "BaseMetadataConverter"
    item_field_getter: "Callable[[Any, str], Any]"

    def commit_item_change(
        self,
        operation: "Operation",
        item_id: "str",
        item: "Any",
        execute_operation: "bool" = True,
    ) -> "ItemChange":
        item_version = cast("BaseDataStore", self).get_item_version(item_id=item_id)
        old_item_change: "Optional[ItemChange]" = None
        if item_version:
            old_item_change = item_version.current_item_change

        item_change = BaseDataStore.commit_item_change(
            cast("BaseDataStore", self),
            operation=operation,
            item_id=item_id,
            item=item,
            execute_operation=execute_operation,
        )
        self.check_tracked_query_vector_clocks(
            old_item_change=old_item_change,
            new_item_change=item_change,
            ignore_old_change_if_none=True,
        )
        return item_change

    def get_item_ids_for_query(self, query: "Query", vector_clock="VectorClock") -> "Set[str]":
        old_query_items = self.query_items(query=query, vector_clock=vector_clock)
        old_item_ids = {item["id"] for item in old_query_items}

        current_query_items = self.query_items(query=query, vector_clock=None)
        current_item_ids = {item["id"] for item in current_query_items}
        filtered_item_ids = old_item_ids.union(current_item_ids)
        return filtered_item_ids

    @abstractmethod
    def query_items(
        self, query: "Query", vector_clock: "Optional[VectorClock]"
    ) -> "List[Any]":
        """Returns a list of the item ids that satisfy a query.

        Args:
            query (Query): The query being tested
            vector_clock (Optional[VectorClock]): A VectorClock that if provided, returns the items that would have
            matched the query at the time indicated by the clock, enabling time-travel through the data. The items
            are returned in the same state they were at the time of the clock.
        """

    @abstractmethod
    def save_tracked_query(self, tracked_query: "TrackedQuery"):
        """Saves the TrackedQuery to the data store.

        Args:
            tracked_query (TrackedQuery): The TrackedQuery that needs saving.
        """

    @abstractmethod
    def get_tracked_query(self, query: "Query") -> "Optional[TrackedQuery]":
        """Returns the TrackedQuery that corresponds to the given Query or None
        if it's not being tracked

        Args:
            query (Query): The query
        """

    @abstractmethod
    def get_tracked_queries(self) -> "List[TrackedQuery]":
        """Returns all queries being tracked.
        """

    def start_tracking_query(self, query: "Query") -> "TrackedQuery":
        vector_clock = VectorClock.create_empty(
            provider_ids=[cast("BaseDataStore", self).local_provider_id]
        )
        tracked_query = TrackedQuery(query=query, vector_clock=vector_clock)
        self.save_tracked_query(tracked_query=tracked_query)
        return tracked_query

    def update_query_vector_clock(self, query: "Query", item_change: "ItemChange"):
        """Updates the vector clock for the given query.

        Args:
            query (Query): The query
            item_change_record (ItemChangeRecord): The record that caused the update
        """
        tracked_query = self.get_tracked_query(query=query)

        if not tracked_query:
            tracked_query = self.start_tracking_query(query=query)

        vector_clock = copy.deepcopy(tracked_query.vector_clock)
        vector_clock.update_vector_clock_item(
            provider_id=item_change.provider_id,
            timestamp=item_change.provider_timestamp,
        )
        updated_tracked_query = TrackedQuery(query=query, vector_clock=vector_clock)
        self.save_tracked_query(tracked_query=updated_tracked_query)

    def check_impacts_query(
        self, item: "Any", query: "Query", vector_clock: "Optional[VectorClock]"
    ) -> "bool":
        """Checks whether a given item is part of a query.

        Args:
            item (Any): The item being checked
            query (Query): The query
            vector_clock (Optional[VectorClock]): The VectorClock that should
            be considered when evaluating the query
        """

        filter_check = query_filter_to_lambda(
            filter=query.filter, item_field_getter=self.item_field_getter # type: ignore
        )
        if filter_check(item):
            query_items = self.query_items(query=query, vector_clock=vector_clock)
            item_ids = {
                self.item_field_getter(query_item, "id") for query_item in query_items # type: ignore
            }
            if self.item_field_getter(item, "id") in item_ids: # type: ignore
                return True

        return False

    def check_tracked_query_vector_clocks(
        self,
        new_item_change: "ItemChange",
        old_item_change: "Optional[ItemChange]" = None,
        ignore_old_change_if_none: "bool" = False,
    ):
        """Checks whether the change impacts any of the queries being tracked and updates
        their VectorClocks accordingly.

        Args:
            new_item_change (ItemChange): The new change
        """
        item = cast("BaseDataStore", self).item_serializer.deserialize_item(
            new_item_change.serialization_result
        )
        old_item: "Optional[Any]" = None

        if not old_item_change and not ignore_old_change_if_none:
            item_version = cast("BaseDataStore", self).get_item_version(
                item_id=new_item_change.serialization_result.item_id
            )

            if item_version:
                old_item_change = item_version.current_item_change

        if old_item_change:
            old_item = cast("BaseDataStore", self).deserialize_item(
                old_item_change.serialization_result
            )

        for tracked_query in self.get_tracked_queries():
            if self.check_impacts_query(
                item=item, query=tracked_query.query, vector_clock=None
            ):
                self.update_query_vector_clock(
                    query=tracked_query.query, item_change=new_item_change
                )
            elif old_item and self.check_impacts_query(
                item=old_item,
                query=tracked_query.query,
                vector_clock=tracked_query.vector_clock,
            ):
                self.update_query_vector_clock(
                    query=tracked_query.query, item_change=new_item_change
                )
