from maestro.backends.django.contrib.factory import create_django_provider
from maestro.backends.mongo.contrib.factory import create_mongo_provider

# from maestro.backends.firestore.contrib.factory import create_firestore_provider
# import maestro.backends.firestore
import maestro.backends.django
from maestro.core.orchestrator import SyncOrchestrator

# from firebase_admin import firestore
import threading
from typing import TYPE_CHECKING


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
    # firestore_provider = create_firestore_provider()

    # Sync lock
    sync_lock = maestro.backends.django.DjangoSyncLock()

    # # Orchestrator
    orchestrator = SyncOrchestrator(
        sync_lock=sync_lock,
        providers=[django_provider, mongo_provider],
        maximum_duration_seconds=10 * 60,
    )
    orchestrator.run(initial_source_provider_id=initial_source_provider_id)
