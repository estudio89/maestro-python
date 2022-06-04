from maestro.core.metadata import Operation
from models import TodoItem
from typing import List
import uuid
import pymongo
from pymongo import MongoClient
from maestro.backends.mongo.contrib.factory import create_mongo_store


class TodoItemRepository:
    def __init__(self):
        self.client = MongoClient(
            "mongodb://maestro:maestro@10.222.0.5:27000/?authSource=admin&readPreference=primary&directConnection=true&ssl=false"
        )
        self.db = self.client["example-db"]
        self.maestro_store = create_mongo_store(
            client=self.client, database_name="example-db"
        )

    def list(self) -> List[TodoItem]:
        documents = self.db["core_todo"].find().sort("date", pymongo.ASCENDING)
        items = []
        for doc in documents:
            items.append(
                TodoItem(
                    id=doc["_id"], text=doc["text"], done=doc["done"], date=doc["date"]
                )
            )
        return items

    def to_dict(self, item: TodoItem):
        data = item.dict()
        data["id"] = str(data["id"])
        data["collection_name"] = "core_todo"
        return data

    def create(self, item: TodoItem) -> TodoItem:
        self.db["core_todo"].insert_one(
            document={
                "_id": str(item.id),
                "text": item.text,
                "done": item.done,
                "date": item.date,
            }
        )
        self.maestro_store.commit_item_change(
            operation=Operation.INSERT,
            entity_name="core_todo",
            item_id=str(item.id),
            item=self.to_dict(item),
        )
        return item

    def update(self, item: TodoItem) -> TodoItem:
        document = self.db["core_todo"].find_one({"_id": str(item.id)})
        if document is None:
            raise ValueError("Item not found")
        document["text"] = item.text
        document["done"] = item.done
        document["date"] = item.date
        self.db["core_todo"].replace_one({"_id": str(item.id)}, document)

        self.maestro_store.commit_item_change(
            operation=Operation.UPDATE,
            entity_name="core_todo",
            item_id=str(item.id),
            item=self.to_dict(item),
        )
        return item

    def delete(self, item_id: uuid.UUID) -> None:
        self.db["core_todo"].delete_one({"_id": str(item_id)})
        self.maestro_store.commit_item_change(
            operation=Operation.DELETE,
            entity_name="core_todo",
            item_id=str(item_id),
            item={"id": str(item_id), "collection_name": "core_todo"},
        )


todo_item_repository = TodoItemRepository()
