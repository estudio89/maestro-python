from maestro.backends.django.contrib.factory import create_django_provider
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
    print("WOULD SYNC")

    # # Django
    # django_provider = create_django_provider()

    # # Firestore
    # firestore_provider = create_firestore_provider()

    # # Sync lock
    # sync_lock = maestro.backends.django.DjangoSyncLock()

    # # Orchestrator
    # orchestrator = SyncOrchestrator(
    #     sync_lock=sync_lock,
    #     providers=[django_provider, firestore_provider],
    #     maximum_duration_seconds=10 * 60,
    # )
    # orchestrator.run(initial_source_provider_id=initial_source_provider_id)