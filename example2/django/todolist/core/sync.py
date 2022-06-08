from maestro.backends.django.contrib.factory import create_django_provider
from maestro.backends.mongo.contrib.factory import create_mongo_provider

from maestro.backends.firestore.contrib.factory import create_firestore_provider
import maestro.backends.django
from maestro.core.events import EventsManager
from maestro.core.orchestrator import SyncOrchestrator
from maestro.core.utils import PIDSyncLock

import threading, sys, traceback
from typing import TYPE_CHECKING

import firebase_admin
from firebase_admin import credentials
from pathlib import Path
import os

# Initialize firebase SDK
BASE_DIR = Path(__file__).resolve().parent.parent
os.environ["FIRESTORE_EMULATOR_HOST"] = "0.0.0.0:7070"

directory = os.path.dirname(__file__)
cred = credentials.Certificate(
    BASE_DIR / "serviceAccountKey.json"
)
firebase_admin.initialize_app(cred)

class DebugEventsManager(EventsManager):
    def on_exception(
        self,
        remote_item_change,
        exception,
        query,
    ):
        super().on_exception(
            remote_item_change,
            exception,
            query,
        )
        print(
            f"Exception while processing item change {remote_item_change}",
            file=sys.stderr,
        )
        traceback.print_exc()

    def on_failed_sync_session(self, exception: "Exception"):
        super().on_failed_sync_session(exception)
        print(
            f"Exception while processing sync session {self.current_sync_session}",
            file=sys.stderr,
        )
        traceback.print_exc()

def on_changes_committed():
    thread = threading.Thread(target=start_sync, args=["django"])
    thread.start()


def start_sync(initial_source_provider_id: "str"):

    # Django
    django_provider = create_django_provider()

    # Mongo
    mongo_provider = create_mongo_provider(
        connect_uri="mongodb://maestro:maestro@10.222.0.5:27000/?authSource=admin&readPreference=primary&directConnection=true&ssl=false",
        database_name="example-db",
    )

    # # Firestore
    firestore_provider = create_firestore_provider(
        events_manager_class=DebugEventsManager,
    )

    # Sync lock
    sync_lock = PIDSyncLock()

    # # Orchestrator
    orchestrator = SyncOrchestrator(
        sync_lock=sync_lock,
        providers=[django_provider, firestore_provider, mongo_provider],
        maximum_duration_seconds=10 * 60,
    )
    orchestrator.run(initial_source_provider_id=initial_source_provider_id)
