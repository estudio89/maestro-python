from flask import Flask, render_template
from flask_cors import CORS
from sync import start_sync
import time

app = Flask(__name__)
CORS(app)

@app.route("/")
def home():
    return render_template("home.html")

@app.route("/api/sync/")
def run_sync():
    start_sync()
    return ""

def _get_sync_identifier():
    with open("sync.txt", "r") as fobj:
        return fobj.readlines()[0]

@app.route("/api/poll/")
def poll():
    identifier = _get_sync_identifier()

    while True:
        time.sleep(0.2)
        new_identifier = _get_sync_identifier()

        if identifier != new_identifier:
            return ""
