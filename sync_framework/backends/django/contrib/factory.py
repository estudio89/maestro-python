import sync_framework.backends.django
from sync_framework.backends.django.settings import sync_framework_settings
from sync_framework.core.execution import ChangesExecutor, ConflictResolver


def create_django_data_store():
    return sync_framework_settings.DJANGO_PROVIDER.DJANGO_DATA_STORE_CLASS(
        local_provider_id=sync_framework_settings.DJANGO_PROVIDER.PROVIDER_ID,
        sync_session_metadata_converter=sync_framework_settings.DJANGO_PROVIDER.SYNC_SESSION_METADATA_CONVERTER_CLASS(),
        item_version_metadata_converter=sync_framework_settings.DJANGO_PROVIDER.ITEM_VERSION_METADATA_CONVERTER_CLASS(),
        item_change_metadata_converter=sync_framework_settings.DJANGO_PROVIDER.ITEM_CHANGE_METADATA_CONVERTER_CLASS(),
        conflict_log_metadata_converter=sync_framework_settings.DJANGO_PROVIDER.CONFLICT_LOG_METADATA_CONVERTER_CLASS(),
        vector_clock_metadata_converter=sync_framework_settings.DJANGO_PROVIDER.VECTOR_CLOCK_METADATA_CONVERTER_CLASS(),
        item_serializer=sync_framework_settings.DJANGO_PROVIDER.ITEM_SERIALIZER_CLASS(),
    )


def create_django_provider():
    django_data_store = create_django_data_store()
    django_events_manager = sync_framework_settings.DJANGO_PROVIDER.EVENTS_MANAGER_CLASS(
        data_store=django_data_store
    )
    changes_executor = ChangesExecutor(
        data_store=django_data_store,
        events_manager=django_events_manager,
        conflict_resolver=ConflictResolver(),
    )
    django_provider = sync_framework.backends.django.DjangoSyncProvider(
        provider_id=sync_framework_settings.DJANGO_PROVIDER.PROVIDER_ID,
        data_store=django_data_store,
        events_manager=django_events_manager,
        changes_executor=changes_executor,
        max_num=sync_framework_settings.MAX_CHANGES_PER_SESSION,
    )

    return django_provider
