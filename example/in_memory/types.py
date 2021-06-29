from typing import TypedDict
import uuid

class TodoType(TypedDict):
	id: "str"
	text: "str"
	done: "bool"
	date_created : "str"