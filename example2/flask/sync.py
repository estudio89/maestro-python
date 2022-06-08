from maestro.core.events import EventsManager
from maestro.core.orchestrator import SyncOrchestrator
from maestro.core.utils import PIDSyncLock

import threading, sys, traceback
from typing import TYPE_CHECKING

import firebase_admin
from firebase_admin import credentials
from pathlib import Path
import os, sys

def initialize_firebase():
    # Initialize firebase SDK
    BASE_DIR = Path(__file__).resolve().parent
    os.environ["FIRESTORE_EMULATOR_HOST"] = "0.0.0.0:7070"

    directory = os.path.dirname(__file__)
    cred = credentials.Certificate(
        BASE_DIR / "serviceAccountKey.json"
    )
    firebase_admin.initialize_app(cred)

def initialize_django():
    BASE_DIR = Path(__file__).resolve().parent.parent
    sys.path.append(str(BASE_DIR / "django" / "todolist"))

    all_settings = {
        "SECRET_KEY": "maestro",

        "INSTALLED_APPS": [
            "django.contrib.admin",
            "django.contrib.auth",
            "django.contrib.contenttypes",
            "django.contrib.sessions",
            "django.contrib.messages",
            "django.contrib.staticfiles",
            "maestro.backends.django",
            "core"
        ],

        "USE_TZ": True,

        "MIDDLEWARE": [
            "django.middleware.security.SecurityMiddleware",
            "django.contrib.sessions.middleware.SessionMiddleware",
            "django.middleware.common.CommonMiddleware",
            "django.middleware.csrf.CsrfViewMiddleware",
            "django.contrib.auth.middleware.AuthenticationMiddleware",
            "django.contrib.messages.middleware.MessageMiddleware",
            "django.middleware.clickjacking.XFrameOptionsMiddleware",
        ],

        "TEMPLATES": [
            {
                "BACKEND": "django.template.backends.django.DjangoTemplates",
                "DIRS": [],
                "APP_DIRS": True,
                "OPTIONS": {
                    "context_processors": [
                        "django.template.context_processors.debug",
                        "django.template.context_processors.request",
                        "django.contrib.auth.context_processors.auth",
                        "django.contrib.messages.context_processors.messages",
                    ],
                },
            },
        ],

        # Database
        # https://docs.djangoproject.com/en/3.1/ref/settings/#databases

        "DATABASES": {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": "/home/ubuntu/Dev/maestro/maestro-python/example2/django/todolist/db.sqlite3",}},

        "MAESTRO": {"MODELS": ["core.todo"]}


    }

    import django
    from django.conf import settings

    settings.configure(**all_settings)
    django.setup()

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


initialize_django()
initialize_firebase()

def start_sync():

    from maestro.backends.django.contrib.factory import create_django_provider
    from maestro.backends.mongo.contrib.factory import create_mongo_provider
    from maestro.backends.firestore.contrib.factory import create_firestore_provider


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
    orchestrator.run(initial_source_provider_id="")
