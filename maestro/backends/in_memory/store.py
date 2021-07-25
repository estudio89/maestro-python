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
    SerializationResult,
)
from maestro.core.exceptions import ItemNotFoundException
from typing import List, Set, Callable, Any, Dict, Optional, cast, Union
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
            "items": {},
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
            vector_clock.update(vector_clock_item=item_change.change_vector_clock_item)
        return vector_clock

    def update_item(
        self, item: "Optional[Any]", serialization_result: "SerializationResult"
    ) -> "Any":
        deserialized = self.deserialize_item(serialization_result)
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
            vector_clock_item = vector_clock.get_vector_clock_item(
                provider_id=item_change.change_vector_clock_item.provider_id
            )
            if item_change.change_vector_clock_item > vector_clock_item:
                if filtered_item_ids:
                    if (
                        item_change.serialization_result.item_id
                        not in filtered_item_ids
                    ):
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

                vector_clock_item = vector_clock.get_vector_clock_item(
                    provider_id=item_change.change_vector_clock_item.provider_id
                )
                if vector_clock_item < item_change.change_vector_clock_item:
                    if filtered_item_ids:
                        if (
                            item_change.serialization_result.item_id
                            not in filtered_item_ids
                        ):
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
        self,
        item_change: "ItemChange",
        is_creating: "bool" = False,
        query: "Optional[Query]" = None,
    ) -> "ItemChange":
        item_change_record = self.item_change_metadata_converter.to_record(
            metadata_object=item_change
        )
        self._save(item=item_change_record, key="item_changes")
        if is_creating and query is not None:
            tracked_query = self.get_tracked_query(query=query)
            if tracked_query is not None:
                self.update_query_vector_clock(
                    tracked_query=tracked_query, item_change=item_change
                )
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
                item = self.get_item_by_id(id=item_change.serialization_result.item_id)
            except ItemNotFoundException:
                item = None
            item = self.update_item(item, item_change.serialization_result)
            self.save_item(cast("Dict", item))
        elif item_change.operation == Operation.DELETE:
            entity_items = self._db["items"].get(
                item_change.serialization_result.entity_name, []
            )
            item = self.deserialize_item(
                serialization_result=item_change.serialization_result
            )
            id_getter = lambda item: item["id"] if isinstance(item, dict) else item.id
            self._delete_entity_item(
                entity_items=entity_items, item=item, id_getter=id_getter
            )

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

    def query_items(
        self, query: "Query", vector_clock: "Optional[VectorClock]"
    ) -> "List[Any]":

        # Filtering
        filter_lambda = query_filter_to_lambda(
            filter=query.filter, item_field_getter=self.item_field_getter
        )
        item_ids_set: "Set[str]" = set()
        items = []
        item_changes = self.get_item_changes()
        item_changes.reverse()  # Reverse so that last changes are first
        for item_change in item_changes:
            serialization_result = item_change.serialization_result
            if (
                serialization_result.entity_name == query.entity_name
                and serialization_result.item_id not in item_ids_set
            ):
                if (
                    vector_clock
                    and vector_clock.get_vector_clock_item(
                        item_change.change_vector_clock_item.provider_id
                    )
                    < item_change.change_vector_clock_item
                ):
                    continue

                item_ids_set.add(serialization_result.item_id)
                item = self.deserialize_item(serialization_result)
                in_query = filter_lambda(item)
                if in_query:
                    item[
                        "inserted_timestamp"
                    ] = item_change.insert_vector_clock_item.timestamp
                    items.append(item)

        # Sorting
        items.sort(key=lambda item: item["inserted_timestamp"])
        for item in items:
            item.pop("inserted_timestamp")

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
        entity_name = copy.deepcopy(item).pop("entity_name")
        self._save(item=item, key="items." + entity_name)

    def _delete_entity_item(
        self, entity_items: "List", item: "Any", id_getter: "Callable"
    ):
        item_idx = None
        item_id = str(id_getter(item))
        for idx, old_item in enumerate(entity_items):
            old_id = str(id_getter(old_item))
            if old_id == item_id:
                item_idx = idx

        if item_idx is not None:
            del entity_items[item_idx]

    def delete_item(self, item: "Any"):
        id_getter = lambda item: item["id"] if isinstance(item, dict) else item.id
        for entity_name in self._db["items"].keys():
            entity_items = self._db["items"][entity_name]
            self._delete_entity_item(
                entity_items=entity_items, item=item, id_getter=id_getter
            )

    def _get_nested_list(self, key: "str") -> "List":
        keys = key.split(".")
        list_obj: "Union[Dict, List]" = self._db[keys[0]]
        for nested_key in keys[1:]:
            if not isinstance(list_obj, dict):
                raise ValueError("Invalid key: " + key)
            nested_list = cast("Dict", list_obj).get(nested_key)
            if nested_list is None:
                list_obj[nested_key] = []
                nested_list = list_obj[nested_key]
            list_obj = nested_list

        return cast("List", list_obj)

    def _save_to_list(self, list_obj: "List", item: "Dict", id_attr: "str"):
        item_idx = None
        for idx, old_item in enumerate(list_obj):
            new_item_id = item[id_attr]
            old_item_id = old_item[id_attr]

            if str(old_item_id) == str(new_item_id):
                item_idx = idx
                break

        if item_idx is not None:
            list_obj[item_idx] = copy.deepcopy(item)
        else:
            list_obj.append(copy.deepcopy(item))

    def _save(self, item: "Dict", key: "str", id_attr: "str" = "id"):
        list_obj = self._get_nested_list(key=key)
        self._save_to_list(list_obj=cast("List", list_obj), item=item, id_attr=id_attr)

    def _get_by_id_from_list(
        self, list_obj: "List", id: "Any", id_attr: "str"
    ) -> "Optional[Any]":
        for item in list_obj:
            if isinstance(item, dict):
                item_id = item[id_attr]
            else:
                item_id = getattr(item, id_attr)

            if str(item_id) == str(id):
                return item

        return None

    def _get_by_id(self, id: "Any", key: "str", id_attr: "str" = "id") -> "Any":
        list_obj = self._get_nested_list(key=key)
        item = self._get_by_id_from_list(
            list_obj=cast("List", list_obj), id=id, id_attr=id_attr
        )

        if item is None:
            raise ItemNotFoundException(item_type=key, id=id)

        return item

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
        all_items: "List[Dict]" = []
        for entity_name in self._db["items"]:
            entity_items = self._db["items"][entity_name]
            all_items.extend(entity_items)

        return all_items

    def get_item_by_id(self, id: "str") -> "Dict":
        for entity_name in self._db["items"].keys():
            try:
                return self._get_by_id(id=id, key="items." + entity_name)
            except ItemNotFoundException:
                continue

        raise ItemNotFoundException(item_type="items", id=id)

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

    def item_to_dict(self, item: "Any") -> "Dict":
        data = copy.deepcopy(cast("Dict", item))
        data.pop("entity_name")
        return data
