from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from example.sync_handler import SyncHandler
from example.web_handlers import WebHandlers
import shutil
import os
import time
from typing import Callable, Dict, List
import re


class URLPattern:
    pattern: "re.Pattern"
    method: "str"
    handler: "Callable"

    def __init__(
        self,
        pattern: "str",
        method: "str",
        handler: "Callable[SyncServerHandler, Dict]",
    ):
        self.pattern = re.compile(pattern)
        self.method = method
        self.handler = handler

    def matches(self, path: "str", method: "str") -> "bool":
        return self.method == method and self.pattern.match(path) is not None

    def get_params(self, path: "str") -> "Dict":
        return self.pattern.match(path).groupdict()

    def __repr__(self):
        return f"URLPattern(pattern='{self.pattern}', method='{self.method}', handler={self.handler})"


class PollingManager:
    def __init__(self):
        self.has_data = {}

    def processed(self, identifier: "str"):
        self.has_data[identifier] = False

    def notify(self, identifier: "str"):
        time.sleep(0.1)
        self.has_data[identifier] = True

    def handle(self, identifier: "str"):
        while True:
            if self.has_data.get(identifier):
                return
            time.sleep(0.1)


class SyncServerHandler(BaseHTTPRequestHandler):
    # https://pythonbasics.org/webserver/
    urlpatterns: "List[URLPattern]"

    def _set_headers(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

    def _serve_html(self, name):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()

        base_path = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(base_path, "static", name), "rb") as fobj:
            fs = os.fstat(fobj.fileno())
            self.send_header("Content-Length", str(fs[6]))
            shutil.copyfileobj(fsrc=fobj, fdst=self.wfile)

    def _get_match(self, method: "str"):
        for url_pattern in self.urlpatterns:
            if url_pattern.matches(path=self.path, method=method):
                params = url_pattern.get_params(path=self.path)
                url_pattern.handler(request_handler=self, params=params)
                return

        self.send_response(404)
        self.send_header("Content-type", "text/html")
        self.wfile.write(b"")
        self.end_headers()

    def do_GET(self):
        self._get_match(method="GET")

    def do_POST(self):
        self._get_match(method="POST")

    def do_PUT(self):
        self._get_match(method="PUT")

    def do_DELETE(self):
        self._get_match(method="DELETE")


ThreadingMixIn.daemon_threads = True


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""


if __name__ == "__main__":
    host_name = "127.0.0.1"
    port = 3131
    sync_handler = SyncHandler()
    polling_manager = PollingManager()
    web_handlers = WebHandlers(
        polling_manager=polling_manager, sync_handler=sync_handler
    )
    urlpatterns = [
        URLPattern(
            pattern=r"^/$", method="GET", handler=web_handlers.get_iframes_index
        ),
        URLPattern(
            pattern=r"^/api/(?P<provider_id>\w+)/$",
            method="GET",
            handler=web_handlers.get_items,
        ),
        URLPattern(
            pattern=r"^/api/(?P<provider_id>\w+)/raw/$",
            method="GET",
            handler=web_handlers.get_raw_db,
        ),
        URLPattern(
            pattern=r"^/api/(?P<provider_id>\w+)/db/$",
            method="GET",
            handler=web_handlers.get_db_ui,
        ),
        URLPattern(
            pattern=r"^/(?P<provider_id>\w+)/$",
            method="GET",
            handler=web_handlers.get_provider_index,
        ),
        URLPattern(
            pattern=r"^/api/(?P<provider_id>\w+)/$",
            method="POST",
            handler=web_handlers.create_item,
        ),
        URLPattern(
            pattern=r"^/api/(?P<provider_id>\w+)/(?P<item_id>\b[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}\b)/$",
            method="PUT",
            handler=web_handlers.update_item,
        ),
        URLPattern(
            pattern=r"^/api/(?P<provider_id>\w+)/(?P<item_id>\b[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}\b)/$",
            method="DELETE",
            handler=web_handlers.delete_item,
        ),
        URLPattern(
            pattern=r"^/api/sync/(?P<provider_id>\w+)/$",
            method="POST",
            handler=web_handlers.sync_provider,
        ),
        URLPattern(
            pattern=r"^/api/polling/(?P<provider_id>\w+)/db/$",
            method="POST",
            handler=web_handlers.db_polling,
        ),
        URLPattern(
            pattern=r"^/api/polling/(?P<provider_id>\w+)/$",
            method="POST",
            handler=web_handlers.index_polling,
        ),
    ]
    SyncServerHandler.urlpatterns = urlpatterns
    server = ThreadedHTTPServer((host_name, port), SyncServerHandler)

    print("Server started http://%s:%s" % (host_name, port))

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass

    server.server_close()
    print("Server stopped.")
