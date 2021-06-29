from django.test import TestCase, override_settings
import tests.base_store
import tests.django.base


@override_settings(ROOT_URLCONF=__name__)
class DjangoStoreTest(
    tests.django.base.DjangoBackendTestMixin, tests.base_store.BaseStoreTest, TestCase
):
    pass
