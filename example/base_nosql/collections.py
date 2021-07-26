from typing import TypedDict
import datetime as dt

class TodoRecord(TypedDict):
	id: "str"
	text: "str"
	done: "bool"
	date_created : "dt.datetime"