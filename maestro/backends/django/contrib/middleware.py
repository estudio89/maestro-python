from django.db import models
from django.core.exceptions import MiddlewareNotUsed
from maestro.core.metadata import Operation
from maestro.backends.django.settings import maestro_settings
from typing import NamedTuple, Callable
import threading


class _QueuedOperation(NamedTuple):
    item: "models.Model"
    operation: "Operation"


def _add_operation_to_queue(operation: "Operation", item: "models.Model"):
    _operations_queue.items.append(_QueuedOperation(operation=operation, item=item))


def _notify_queued_operations():
    if _operations_queue.items:
        if maestro_settings.CHANGES_COMMITTED_CALLBACK:
            callback = maestro_settings.CHANGES_COMMITTED_CALLBACK
            callback()

        _operations_queue.items = []


_operations_queue = threading.local()
_operations_queue.items = []


class SyncQueueMiddleware:
    def __init__(self, get_response: "Callable"):

        if not maestro_settings.MODELS:
            raise MiddlewareNotUsed(
                "You haven't defined any models to be synchronized automatically so you can remove this middleware."
            )

        self.get_response = get_response

    def __call__(self, request):
        _operations_queue.items = []

        response = self.get_response(request)

        _notify_queued_operations()

        return response

    def process_exception(self, request, exception):
        if _operations_queue.items:
            _notify_queued_operations()
