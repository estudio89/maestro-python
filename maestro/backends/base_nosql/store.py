from maestro.core.store import BaseDataStore
from maestro.core.metadata import ItemChange
from typing import List, Any
from abc import abstractmethod
import json


class NoSQLDataStore(BaseDataStore):
    @abstractmethod
    def find_item_changes(self, ids: "List[str]") -> "List[ItemChange]":
        """
        Finds multiple ItemChanges by their IDs.

        Args:
            ids (List[str]): List of ItemChange IDs
        """
