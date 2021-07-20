from maestro.core.query.metadata import Query, SortOrder, TrackedQuery
from maestro.core.query.utils import query_filter_to_lambda
from maestro.core.query.store import TrackQueriesStoreMixin
from maestro.core.store import BaseDataStore
from maestro.backends.in_memory.converters import TrackedQueryConverter
from maestro.core.metadata import (
    VectorClock,
    ItemChange,
    ItemChangeBatch,
    ItemVersion,
    ConflictStatus,
    ConflictLog,
    Operation,
    SyncSession,
)
from maestro.core.exceptions import ItemNotFoundException
from typing import List, Set, Callable, Any, Dict, Optional, cast
import datetime as dt
import uuid
import copy


class InMemoryDataStore(TrackQueriesStoreMixin, BaseDataStore):
    def __init__(self, *args, **kwargs):
        self.tracked_query_metadata_converter = kwargs.pop(
            "tracked_query_metadata_converter", TrackedQueryConverter()
        )
        self.item_field_getter: "Callable[[Any, str], Any]" = lambda item, field_name: item[
            field_name
        ]
        super().__init__(*args, **kwargs)
        self._db = {
            "item_changes": [],
            "item_versions": [],
            "conflict_logs": [],
            "sync_sessions": [],
            "items": [],
            "tracked_queries": [],
        }

    def get_local_vector_clock(self, query: "Optional[Query]" = None) -> "VectorClock":
        if query is not None:
            tracked_query = self.get_tracked_query(query=query)
            if tracked_query:
                return tracked_query.vector_clock
            else:
                return VectorClock.create_empty(provider_ids=[self.local_provider_id])

        vector_clock = VectorClock.create_empty(provider_ids=[self.local_provider_id])
        item_changes = self.get_item_changes()

        for item_change in item_changes:
            vector_clock.update_vector_clock_item(
                provider_id=item_change.provider_id,
                timestamp=item_change.provider_timestamp,
            )
        return vector_clock

    def update_item(self, item: "Optional[Any]", serialized_item: "str") -> "Any":
        deserialized = self.deserialize_item(serialized_item)
        if item:
            item.update(deserialized)
            deserialized = item
        return deserialized

    def _paginate_item_changes(self, all_changes, max_num: "int") -> "ItemChangeBatch":
        total_count = len(all_changes)
        all_changes.sort(key=lambda item_change: item_change.date_created)
        item_changes = list(all_changes[:max_num])
        is_last_batch = total_count == len(item_changes)

        item_change_batch = ItemChangeBatch(
            item_changes=item_changes, is_last_batch=is_last_batch
        )

        return item_change_batch

    def select_changes(
        self,
        vector_clock: "VectorClock",
        max_num: "int",
        query: "Optional[Query]" = None,
    ) -> "ItemChangeBatch":

        filtered_item_ids: "Optional[Set[str]]" = None
        if query:
            filtered_item_ids = self.get_item_ids_for_query(
                query=query, vector_clock=vector_clock
            )

        selected_changes: "List[ItemChange]" = []
        for item_change in self.get_item_changes():
            remote_timestamp = vector_clock.get_vector_clock_item(
                provider_id=item_change.provider_id
            ).timestamp
            if item_change.provider_timestamp > remote_timestamp:
                if filtered_item_ids:
                    if item_change.item_id not in filtered_item_ids:
                        continue

                selected_changes.append(item_change)

        selected_changes = copy.deepcopy(selected_changes)
        return self._paginate_item_changes(
            all_changes=selected_changes, max_num=max_num
        )

    def select_deferred_changes(
        self,
        vector_clock: "VectorClock",
        max_num: "int",
        query: "Optional[Query]" = None,
    ) -> "ItemChangeBatch":

        selected_changes: "List[ItemChange]" = []

        filtered_item_ids: "Optional[Set[str]]" = None
        if query:
            old_query_items = self.query_items(query=query, vector_clock=vector_clock)
            old_item_ids = {item["id"] for item in old_query_items}

            current_query_items = self.query_items(query=query, vector_clock=None)
            current_item_ids = {item["id"] for item in current_query_items}
            filtered_item_ids = old_item_ids.union(current_item_ids)

        for conflict_log in self.get_conflict_logs():
            if conflict_log.status == ConflictStatus.DEFERRED:
                item_change = conflict_log.item_change_loser

                remote_timestamp = vector_clock.get_vector_clock_item(
                    provider_id=item_change.provider_id
                ).timestamp
                if remote_timestamp < item_change.provider_timestamp:
                    if filtered_item_ids:
                        if item_change.item_id not in filtered_item_ids:
                            continue

                    selected_changes.append(item_change)

        selected_changes = copy.deepcopy(selected_changes)
        return self._paginate_item_changes(
            all_changes=selected_changes, max_num=max_num
        )

    def get_tracked_query(self, query: "Query") -> "Optional[TrackedQuery]":
        try:
            tracked_query_record = self._get_by_id(
                id=query, key="tracked_queries", id_attr="query"
            )
            tracked_query = self.tracked_query_metadata_converter.to_metadata(
                record=tracked_query_record
            )
            return tracked_query
        except ItemNotFoundException:
            return None

    def save_tracked_query(self, tracked_query: "TrackedQuery"):
        instance = self.tracked_query_metadata_converter.to_record(
            metadata_object=tracked_query
        )
        self._save(item=instance, key="tracked_queries")

    def save_item_change(
        self, item_change: "ItemChange", is_creating: "bool" = False
    ) -> "ItemChange":
        item_change_record = self.item_change_metadata_converter.to_record(
            metadata_object=item_change
        )
        self._save(item=item_change_record, key="item_changes")
        if is_creating:
            self.check_tracked_query_vector_clocks(new_item_change=item_change)
        return item_change

    def run_in_transaction(self, item_change: "ItemChange", callback: "Callable"):
        callback()

    def save_conflict_log(self, conflict_log: "ConflictLog"):
        conflict_log_record = self.conflict_log_metadata_converter.to_record(
            metadata_object=conflict_log
        )
        self._save(item=conflict_log_record, key="conflict_logs")

    def execute_item_change(self, item_change: "ItemChange"):
        if (
            item_change.operation == Operation.UPDATE
            or item_change.operation == Operation.INSERT
        ):
            item: "Optional[Dict]"
            try:
                item = self.get_item_by_id(id=item_change.item_id)
            except ItemNotFoundException:
                item = None
            item = self.update_item(item, item_change.serialized_item)
            self.save_item(cast("Dict", item))
        elif item_change.operation == Operation.DELETE:
            item_idx = None
            get_id = lambda item: item["id"] if isinstance(item, dict) else item.id
            item_id = str(item_change.item_id)
            for idx, item in enumerate(self._db["items"]):
                old_item_id = str(get_id(item))
                if item_id == old_item_id:
                    item_idx = idx
                    break

            if item_idx != None:
                del self._db["items"][item_idx]

    def save_item_version(self, item_version: "ItemVersion"):
        item_version_record = self.item_version_metadata_converter.to_record(
            metadata_object=item_version
        )
        self._save(item=item_version_record, key="item_versions", id_attr="item_id")
        return item_version

    def get_deferred_conflict_logs(
        self, item_change_loser: "ItemChange"
    ) -> "List[ConflictLog]":

        selected_logs: "List[ConflictLog]" = []
        for conflict_log in self.get_conflict_logs():
            if (
                conflict_log.status == ConflictStatus.DEFERRED
                and conflict_log.item_change_loser.id == item_change_loser.id
            ):
                selected_logs.append(conflict_log)

        return selected_logs

    def save_sync_session(self, sync_session: "SyncSession"):
        sync_session_record = self.sync_session_metadata_converter.to_record(
            metadata_object=sync_session
        )
        self._save(item=sync_session_record, key="sync_sessions")

    def _get_previous_item_change(
        self, item_id: "str", provider_id: "str", timestamp: "dt.datetime"
    ) -> "Optional[ItemChange]":
        max_item_change = None
        max_vector_clock_item = None

        for item_change in self.get_item_changes():
            # NOT SURE IF THE COMPARISON BELOW MAKES SENSE
            # I think it will be necessary to implement comparison between VectorClocks
            vector_clock_item = item_change.vector_clock.get_vector_clock_item(
                provider_id=provider_id
            )
            if (
                item_change.item_id == item_id
                and item_change.is_applied
                and not item_change.should_ignore
                and vector_clock_item.timestamp < timestamp
            ):
                if (
                    max_vector_clock_item is None
                    or vector_clock_item > max_vector_clock_item
                ):
                    max_vector_clock_item = vector_clock_item
                    max_item_change = item_change

        return max_item_change

    def query_items(
        self, query: "Query", vector_clock: "Optional[VectorClock]"
    ) -> "List[Any]":

        # Filtering
        filter_lambda = query_filter_to_lambda(
            filter=query.filter, item_field_getter=self.item_field_getter
        )
        item_ids_set: "Set[str]" = set()
        items = []
        for item_change in self.get_item_changes():
            if item_change.item_id not in item_ids_set:
                if (
                    vector_clock
                    and vector_clock.get_vector_clock_item(
                        item_change.provider_id
                    ).timestamp
                    < item_change.provider_timestamp
                ):
                    continue
                item = self.deserialize_item(item_change.serialized_item)
                in_query = filter_lambda(item)
                if in_query:
                    items.append(item)

        # Sorting
        for sort_order in cast("List[SortOrder]", reversed(query.ordering)):
            sort_func = lambda item: item[sort_order.field_name]
            items.sort(
                key=sort_func, reverse=sort_order.descending,
            )

        # Pagination
        start_idx = 0 if query.offset is None else query.offset
        end_idx = len(items) if query.limit is None else start_idx + query.limit
        items_page = items[start_idx:end_idx]

        return items_page

    def get_item_changes(self) -> "List[ItemChange]":
        changes = self._db["item_changes"]

        return [
            self.item_change_metadata_converter.to_metadata(record=item)
            for item in sorted(changes, key=lambda value: value["date_created"])
        ]

    def get_tracked_queries(self) -> "List[TrackedQuery]":
        queries = self._db["tracked_queries"]

        return [
            self.tracked_query_metadata_converter.to_metadata(record=item)
            for item in queries
        ]

    def save_item(self, item: "Dict"):
        self._save(item=item, key="items")

    def delete_item(self, item: "Any"):
        get_id = lambda item: item["id"] if isinstance(item, dict) else item.id
        item_id = str(get_id(item))
        item_idx = None
        for idx, old_item in enumerate(self._db["items"]):
            old_id = str(get_id(old_item))
            if old_id == item_id:
                item_idx = idx

        if item_idx is not None:
            del self._db["items"][item_idx]

    def _save(self, item: "Any", key: "str", id_attr: "str" = "id"):
        item_idx = None
        for idx, old_item in enumerate(self._db[key]):
            if isinstance(item, dict):
                new_item_id = item[id_attr]
            else:
                new_item_id = getattr(item, id_attr)

            if isinstance(old_item, dict):
                old_item_id = old_item[id_attr]
            else:
                old_item_id = getattr(old_item, id_attr)

            if str(old_item_id) == str(new_item_id):
                item_idx = idx
                break

        if item_idx is not None:
            self._db[key][item_idx] = copy.deepcopy(item)
        else:
            self._db[key].append(copy.deepcopy(item))

    def _get_by_id(self, id: "Any", key: "str", id_attr: "str" = "id"):
        for item in self._db[key]:
            if isinstance(item, dict):
                item_id = item[id_attr]
            else:
                item_id = getattr(item, id_attr)

            if str(item_id) == str(id):
                return item

        raise ItemNotFoundException(item_type=key, id=id)

    def get_item_versions(self) -> "List[ItemVersion]":
        return [
            self.item_version_metadata_converter.to_metadata(record=item)
            for item in self._db["item_versions"]
        ]

    def get_conflict_logs(self) -> "List[ConflictLog]":
        return [
            self.conflict_log_metadata_converter.to_metadata(record=item)
            for item in self._db["conflict_logs"]
        ]

    def get_sync_sessions(self) -> "List[SyncSession]":
        return [
            self.sync_session_metadata_converter.to_metadata(record=item)
            for item in self._db["sync_sessions"]
        ]

    def get_items(self) -> "List[Dict]":
        return self._db["items"]

    def get_item_by_id(self, id: "str") -> "Dict":
        return self._get_by_id(id=id, key="items")

    def get_item_version(self, item_id: "str") -> "Optional[ItemVersion]":
        try:
            item_version_record = self._get_by_id(
                id=item_id, key="item_versions", id_attr="item_id"
            )
            item_version = self.item_version_metadata_converter.to_metadata(
                record=item_version_record
            )
            return item_version
        except ItemNotFoundException:
            return None

    def get_item_change_by_id(self, id: "uuid.UUID") -> "ItemChange":
        item_change_record = self._get_by_id(id=id, key="item_changes")
        item_change = self.item_change_metadata_converter.to_metadata(
            record=item_change_record
        )
        return item_change

    def _get_hashable_item(self, item: "Any"):
        return tuple(item[attr] for attr in item)
