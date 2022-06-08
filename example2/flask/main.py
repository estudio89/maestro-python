from flask import Flask, render_template
from sync import start_sync

app = Flask(__name__)

@app.route("/")
def home():
    return render_template("home.html")

@app.route("/api/sync/")
def run_sync():
    start_sync()
    return ""