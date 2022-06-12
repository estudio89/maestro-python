import requests

def on_changes_committed():
    requests.get("http://0.0.0.0:1215/api/sync/")
