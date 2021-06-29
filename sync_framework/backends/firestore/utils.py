from .collections import CollectionType
import math
import json


def table_name_to_collection(table_name: "str") -> "str":
    return table_name


def collection_to_table_name(collection: "str") -> "str":
    return collection


def get_collection_name(serialized_item: "str") -> "str":
    data = json.loads(serialized_item)
    table_name = data["table_name"]
    collection = table_name_to_collection(table_name)
    return collection


def type_to_collection(key: "CollectionType"):
    return f"sync_framework__{key.value}"


def calculate_firestore_usage(
    num_updates_inserts_sent: "int" = 0,
    num_deletes_sent: "int" = 0,
    num_updates_inserts_received: "int" = 0,
    num_deletes_received: "int" = 0,
    num_items_affected_received: "int" = 0,
    max_per_page: "int" = 5
):
    num_writes = 2 + num_updates_inserts_received * 5 + num_deletes_received * 4
    num_changes_received = num_updates_inserts_received + num_items_affected_received
    if num_changes_received > 0:
        num_reads_receiving = 1 + num_changes_received
    else:
        num_reads_receiving = 0

    num_changes_sent = num_updates_inserts_sent + num_deletes_sent
    if num_changes_sent > 0:
        num_pages = math.ceil(num_changes_sent / max_per_page) + 1
        num_reads_sending = num_pages * 2 + num_changes_sent
    else:
        num_reads_sending = 0

    num_reads = num_reads_receiving + num_reads_sending

    num_deletes = num_deletes_received

    return {
        "num_reads": num_reads,
        "num_writes": num_writes,
        "num_deletes": num_deletes,
    }
