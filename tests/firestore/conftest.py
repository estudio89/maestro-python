import firebase_admin
from firebase_admin import credentials
import os

def pytest_configure(config):
    os.environ["FIRESTORE_EMULATOR_HOST"] = "0.0.0.0:7070"

    directory = os.path.dirname(__file__)
    cred = credentials.Certificate(
        os.path.join(directory, "firebase-project/serviceAccountKey.json")
    )
    firebase_admin.initialize_app(cred)

