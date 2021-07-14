from typing import ContextManager
from maestro.core.utils import BaseSyncLock

class InMemoryContextManager:
    lock: "InMemorySyncLock"

    def __init__(self, lock: "InMemorySyncLock"):
        self.lock = lock

    def __enter__(self):
        self.lock._running = True

    def __exit__(self, type, value, traceback):
        self.lock._running = False


class InMemorySyncLock(BaseSyncLock):
    def __init__(self):
        self._running = False

    def is_running(self) -> "bool":
        return self._running

    def lock(self) -> "ContextManager":
        return InMemoryContextManager(lock=self)