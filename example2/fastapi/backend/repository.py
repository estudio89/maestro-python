from models import TodoItem
from typing import List
import uuid
from pymongo import MongoClient


class TodoItemRepository:
    def __init__(self):
        self.client = MongoClient("mongodb://maestro:maestro@10.222.0.5:27000/?authSource=admin&readPreference=primary&directConnection=true&ssl=false")
        self.db = self.client["example-db"]

    def list(self) -> List[TodoItem]:
        documents = self.db.todos.find()
        items = []
        for doc in documents:
            items.append(TodoItem(id=doc["_id"], text=doc["text"], done=doc["done"]))
        return items

    def create(self, item: TodoItem) -> TodoItem:
        self.db.todos.insert_one(
            document={"_id": str(item.id), "text": item.text, "done": item.done}
        )
        return item

    def update(self, item: TodoItem) -> TodoItem:
        document = self.db.todos.find_one({"_id": str(item.id)})
        if document is None:
            raise ValueError("Item not found")
        document["text"] = item.text
        document["done"] = item.done
        self.db.todos.replace_one({"_id": str(item.id)}, document)
        return item

    def delete(self, item_id: uuid.UUID) -> None:
        self.db.todos.delete_one({"_id": str(item_id)})

todo_item_repository = TodoItemRepository()
