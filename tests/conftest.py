import tests.django.settings
import os

def pytest_configure(config):
    from django.conf import settings

    all_settings = {key: getattr(tests.django.settings, key) for key in dir(tests.django.settings) if not key.startswith("_")}
    settings.configure(**all_settings)
