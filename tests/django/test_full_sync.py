from django.test import TestCase, override_settings
import tests.base_full_sync
import tests.django.base


@override_settings(ROOT_URLCONF=__name__)
class DjangoFullSyncTest(
    tests.django.base.DjangoBackendTestMixin,
    tests.base_full_sync.FullSyncTest,
    TestCase,
):
    pass
