from maestro.core.events import EventsManager
from maestro.core.metadata import ItemChange
from maestro.core.query.metadata import Query
from typing import Optional


class DebugEventsManager(EventsManager):
    raise_exception = True

    def on_exception(
        self,
        remote_item_change: "ItemChange",
        exception: "Exception",
        query: "Optional[Query]",
    ):
        if self.raise_exception:  # pragma: no cover
            raise exception
        else:
            super().on_exception(
                remote_item_change=remote_item_change, exception=exception, query=query
            )

    def on_failed_sync_session(self, exception: "Exception"):
        super().on_failed_sync_session(exception=exception)
        if self.raise_exception:  # pragma: no cover
            raise exception
