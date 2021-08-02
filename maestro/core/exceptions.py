class SyncTimeoutException(Exception):
    def __init__(self, elapsed_time, max_duration_seconds: "int"):
        super().__init__(
            "Sync session timed out. Max duration: %d. Elapsed time: %d"
            % (max_duration_seconds, elapsed_time)
        )

class ItemNotFoundException(Exception):
    def __init__(self, item_type: "str", id: "str"):
        super(ItemNotFoundException, self).__init__(
            f"Item of type '{item_type}' and ID '{id}' not found."
        )