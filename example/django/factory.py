from sync_framework.core.execution import ChangesExecutor, ConflictResolver

from sync_framework.backends.django import (
    DjangoDataStore,
    DjangoSyncProvider,
    DjangoItemSerializer,
    SyncSessionMetadataConverter,
    ItemVersionMetadataConverter,
    ItemChangeMetadataConverter,
    ConflictLogMetadataConverter,
    VectorClockMetadataConverter,
)
from django.apps import apps
from django.core.management import call_command
from example.events import DebugEventsManager
from .api_serializer import DjangoAPISerializer
import os


class DjangoExampleDataStore(DjangoDataStore):
    def get_items(self):
        Todo = apps.get_model('todos','Todo')
        items = Todo.objects.all()
        return items

def create_provider(local_provider_id: "str"):
    # Django setup
    os.remove("example/django/todos.sqlite3")
    from django.conf import settings
    settings.configure(
        INSTALLED_APPS = [
            'django.contrib.admin',
            'django.contrib.auth',
            'django.contrib.contenttypes',
            'django.contrib.sessions',
            'django.contrib.messages',
            'django.contrib.staticfiles',
            'sync_framework.backends.django',
            'example.django'
        ],

        USE_TZ=True,

        MIDDLEWARE = [
            'django.middleware.security.SecurityMiddleware',
            'django.contrib.sessions.middleware.SessionMiddleware',
            'django.middleware.common.CommonMiddleware',
            'django.middleware.csrf.CsrfViewMiddleware',
            'django.contrib.auth.middleware.AuthenticationMiddleware',
            'django.contrib.messages.middleware.MessageMiddleware',
            'django.middleware.clickjacking.XFrameOptionsMiddleware',
        ],

        TEMPLATES = [
            {
                'BACKEND': 'django.template.backends.django.DjangoTemplates',
                'DIRS': [],
                'APP_DIRS': True,
                'OPTIONS': {
                    'context_processors': [
                        'django.template.context_processors.debug',
                        'django.template.context_processors.request',
                        'django.contrib.auth.context_processors.auth',
                        'django.contrib.messages.context_processors.messages',
                    ],
                },
            },
        ],

        # Database
        # https://docs.djangoproject.com/en/3.1/ref/settings/#databases

        DATABASES = {
            'default': {
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': 'example/django/todos.sqlite3',
            }
        }

    )
    import django
    django.setup()
    call_command("migrate")

    # Dependency injection
    sync_session_metadata_converter = SyncSessionMetadataConverter()
    item_version_metadata_converter = ItemVersionMetadataConverter()
    item_change_metadata_converter = ItemChangeMetadataConverter()
    conflict_log_metadata_converter = ConflictLogMetadataConverter()
    vector_clock_metadata_converter = VectorClockMetadataConverter()

    data_store = DjangoExampleDataStore(
        local_provider_id=local_provider_id,
        sync_session_metadata_converter=sync_session_metadata_converter,
        item_version_metadata_converter=item_version_metadata_converter,
        item_change_metadata_converter=item_change_metadata_converter,
        conflict_log_metadata_converter=conflict_log_metadata_converter,
        vector_clock_metadata_converter=vector_clock_metadata_converter,
        item_serializer=DjangoItemSerializer(),
    )

    events_manager = DebugEventsManager(data_store=data_store)
    changes_executor = ChangesExecutor(
        data_store=data_store,
        events_manager=events_manager,
        conflict_resolver=ConflictResolver(),
    )
    provider = DjangoSyncProvider(
        provider_id=local_provider_id,
        data_store=data_store,
        events_manager=events_manager,
        changes_executor=changes_executor,
        max_num=10,
    )
    return provider, DjangoAPISerializer()
