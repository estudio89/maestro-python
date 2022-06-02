from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from models import TodoItem
from repository import todo_item_repository
import uuid

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"])


@app.get("/api/todo/")
async def list():
    items = todo_item_repository.list()
    return items


@app.post("/api/todo/")
async def create(item: TodoItem):
    item = todo_item_repository.create(item)
    return item


@app.put("/api/todo/{item_id}")
async def update(item: TodoItem):
    item = todo_item_repository.update(item)
    return item


@app.delete("/api/todo/{item_id}")
async def delete(item_id: uuid.UUID):
    todo_item_repository.delete(item_id)
    return {}
