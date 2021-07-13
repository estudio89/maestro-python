from django.utils.module_loading import import_string
from django.conf import settings
from typing import Dict, Any
import copy

DEFAULTS: "Dict[str, Any]" = {
    "MODELS": [],
    "MAX_CHANGES_PER_SESSION": 50,
    "DJANGO_PROVIDER": {
        "EVENTS_MANAGER_CLASS": "sync_framework.core.events.EventsManager",
        "PROVIDER_ID": "django",
        "DJANGO_DATA_STORE_CLASS": "sync_framework.backends.django.DjangoDataStore",
        "SYNC_SESSION_METADATA_CONVERTER_CLASS": "sync_framework.backends.django.SyncSessionMetadataConverter",
        "ITEM_VERSION_METADATA_CONVERTER_CLASS": "sync_framework.backends.django.ItemVersionMetadataConverter",
        "ITEM_CHANGE_METADATA_CONVERTER_CLASS": "sync_framework.backends.django.ItemChangeMetadataConverter",
        "CONFLICT_LOG_METADATA_CONVERTER_CLASS": "sync_framework.backends.django.ConflictLogMetadataConverter",
        "VECTOR_CLOCK_METADATA_CONVERTER_CLASS": "sync_framework.backends.django.VectorClockMetadataConverter",
        "ITEM_SERIALIZER_CLASS": "sync_framework.backends.django.DjangoItemSerializer",
    },
    "CHANGES_COMMITTED_CALLBACK": None
}

IMPORT_STRINGS = [
    "EVENTS_MANAGER_CLASS",
    "DJANGO_DATA_STORE_CLASS",
    "SYNC_SESSION_METADATA_CONVERTER_CLASS",
    "ITEM_VERSION_METADATA_CONVERTER_CLASS",
    "ITEM_CHANGE_METADATA_CONVERTER_CLASS",
    "CONFLICT_LOG_METADATA_CONVERTER_CLASS",
    "VECTOR_CLOCK_METADATA_CONVERTER_CLASS",
    "ITEM_SERIALIZER_CLASS",
    "CHANGES_COMMITTED_CALLBACK"
]


def perform_import(val, setting_name):
    """
    If the given setting is a string import notation,
    then perform the necessary import or imports.
    """
    if val is None:
        return None
    elif isinstance(val, str):
        return import_from_string(val, setting_name)
    elif isinstance(val, (list, tuple)):
        return [import_from_string(item, setting_name) for item in val]
    return val


def import_from_string(val, setting_name):
    """
    Attempt to import a class from a string representation.
    """
    try:
        return import_string(val)
    except ImportError as e:
        msg = "Could not import '%s' for API setting '%s'. %s: %s." % (
            val,
            setting_name,
            e.__class__.__name__,
            e,
        )
        raise ImportError(msg)


class SyncFrameworkSettings:
    """
    A settings object that allows Sync Framework settings to be accessed as
    properties. For example:

        from sync_framework.backends.django.settings import sync_framework_settings
        print(sync_framework_settings.SYNC_FRAMEWORK_MODELS)

    Note:
    This is an internal class that is only compatible with settings namespaced
    under the SYNC_FRAMEWODK name. It is not intended to be used by 3rd-party
    apps, and test helpers like `override_settings` may not work as expected.
    """

    def __init__(
        self,
        defaults=DEFAULTS,
        import_strings=IMPORT_STRINGS,
        user_settings=settings,
        namespace="SYNC_FRAMEWORK",
    ):
        self.defaults = defaults
        self.import_strings = import_strings
        self._cached_attrs = set()
        if isinstance(user_settings, dict):
            self._user_settings = user_settings.get(namespace, {})
        else:
            self._user_settings = getattr(user_settings, namespace, {})

        self._cached_attrs = set()

    def __getattr__(self, attr):
        if attr not in self.defaults:
            raise AttributeError("Invalid setting: '%s" % attr)

        try:
            val = self._user_settings[attr]
        except KeyError:
            val = self.defaults[attr]

        # Coerce import strings into classes
        if attr in self.import_strings:
            val = perform_import(val, attr)

        if isinstance(val, dict):
            val = SyncFrameworkSettings(
                defaults=copy.deepcopy(DEFAULTS[attr]),
                user_settings=self._user_settings if attr in self._user_settings else {},
                namespace=attr
            )

        # Cache the result
        self._cached_attrs.add(attr)
        setattr(self, attr, val)
        return val


sync_framework_settings = SyncFrameworkSettings()
