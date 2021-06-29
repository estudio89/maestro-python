from typing import Dict, TYPE_CHECKING
import os
import json

if TYPE_CHECKING:
    from example.server import SyncServerHandler, PollingManager
    from example.sync_handler import SyncHandler


class WebHandlers:
    def __init__(self, polling_manager: "PollingManager", sync_handler: "SyncHandler"):
        self.polling_manager = polling_manager
        self.sync_handler = sync_handler

    def get_items(self, request_handler: "SyncServerHandler", params: "Dict"):
        request_handler._set_headers()
        items = self.sync_handler.get_items(provider_id=params["provider_id"]).encode(
            "utf-8"
        )
        request_handler.wfile.write(items)

    def get_raw_db(self, request_handler: "SyncServerHandler", params: "Dict"):
        request_handler._set_headers()
        raw_db = self.sync_handler.get_db(provider_id=params["provider_id"]).encode(
            "utf-8"
        )
        request_handler.wfile.write(raw_db)

    def get_db_ui(self, request_handler: "SyncServerHandler", params: "Dict"):
        request_handler.send_response(200)
        request_handler.send_header("Content-type", "text/html")
        request_handler.end_headers()

        provider_id = params["provider_id"]
        json_data = self.sync_handler.get_db(provider_id=provider_id)
        base_path = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(base_path, "static", "db.html"), "r") as fobj:
            content = fobj.read()
            content = content.replace("{{ JSON_DATA }}", json_data)
            content = content.replace("{{ PROVIDER_ID }}", provider_id)
            content_bytes = content.encode("utf-8")
            request_handler.send_header("Content-Length", len(content_bytes))

        request_handler.wfile.write(content_bytes)

    def get_provider_index(self, request_handler: "SyncServerHandler", params: "Dict"):
        request_handler._serve_html(name="index.html")

    def get_iframes_index(self, request_handler: "SyncServerHandler", params: "Dict"):
        request_handler._serve_html(name="iframes.html")

    def create_item(self, request_handler: "SyncServerHandler", params: "Dict"):
        str_data = request_handler.rfile.read(int(request_handler.headers["Content-Length"]))
        request_handler._set_headers()
        data = json.loads(str_data)
        provider_id = params["provider_id"]
        self.sync_handler.create_item(
            provider_id=provider_id, item_id=data["id"], raw_item=data
        )
        self.polling_manager.notify(identifier=provider_id + "_db")

    def update_item(self, request_handler: "SyncServerHandler", params: "Dict"):
        request_handler._set_headers()
        str_data = request_handler.rfile.read(int(request_handler.headers["Content-Length"]))

        provider_id = params["provider_id"]
        data = json.loads(str_data)
        self.sync_handler.update_item(
            provider_id=provider_id, item_id=data["id"], raw_item=data
        )
        self.polling_manager.notify(identifier=provider_id + "_db")

    def delete_item(self, request_handler: "SyncServerHandler", params: "Dict"):
        request_handler._set_headers()
        str_data = request_handler.rfile.read(int(request_handler.headers["Content-Length"]))
        data = json.loads(str_data)
        provider_id = params["provider_id"]
        item_id = params["item_id"]
        self.sync_handler.delete_item(
            provider_id=provider_id, item_id=item_id, raw_item=data
        )
        self.polling_manager.notify(identifier=provider_id + "_db")

    def sync_provider(self, request_handler: "SyncServerHandler", params: "Dict"):
        provider_id = params["provider_id"]
        self.sync_handler.synchronize(initial_source_provider_id=provider_id)
        other_provider_id = self.sync_handler.get_other_provider_id(
            provider_id=provider_id
        )
        request_handler._set_headers()
        self.polling_manager.notify(identifier=other_provider_id)
        self.polling_manager.notify(identifier=provider_id + "_db")
        self.polling_manager.notify(identifier=other_provider_id + "_db")

    def db_polling(self, request_handler: "SyncServerHandler", params: "Dict"):
        provider_id = params["provider_id"]

        self.polling_manager.handle(identifier=provider_id + "_db")
        request_handler._set_headers()
        self.polling_manager.processed(identifier=provider_id + "_db")

    def index_polling(self, request_handler: "SyncServerHandler", params: "Dict"):
        provider_id = params["provider_id"]

        self.polling_manager.handle(identifier=provider_id)
        request_handler._set_headers()
        self.polling_manager.processed(identifier=provider_id)


