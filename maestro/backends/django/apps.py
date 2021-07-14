from django.apps import AppConfig
from maestro.backends.django.contrib.signals import connect_signals


class DjangoBackendConfig(AppConfig):
    name = "maestro.backends.django"
    label = "maestro"

    def ready(self):
        connect_signals()
