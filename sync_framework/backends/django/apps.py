from django.apps import AppConfig
from sync_framework.backends.django.contrib.signals import connect_signals


class DjangoBackendConfig(AppConfig):
    name = "sync_framework.backends.django"
    label = "sync_framework"

    def ready(self):
        connect_signals()
