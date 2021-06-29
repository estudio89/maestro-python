import unittest
import unittest.mock
from sync_framework.core.exceptions import ItemNotFoundException
from sync_framework.core.metadata import (
    ItemChange,
    ItemChangeBatch,
    ItemVersion,
    ConflictLog,
    ConflictStatus,
    ConflictType,
    Operation,
    VectorClock,
    VectorClockItem,
    SyncSession,
    SyncSessionStatus,
)
import uuid
import datetime as dt
import copy
from .base import BackendTestMixin


class BaseStoreTest(BackendTestMixin, unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.data_store = self._create_data_store(local_provider_id="provider_in_test",)

    def test_get_local_version(self):
        """ Tests retrieving the local version of an item. """

        # Adding a change to an object
        item = self._create_item(
            id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="1"
        )
        self._add_item(item=item)

        item_change1 = ItemChange(
            id=uuid.UUID("54423877-370a-4936-b362-419cc86abbb8"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            operation=Operation.INSERT,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            provider_id="provider_in_test",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="1"
            ),
            should_ignore=False,
            is_applied=True,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        day=15,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=40,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )

        self._add_item_change(item_change=item_change1)

        # Adding a version for the same object
        item_version1 = ItemVersion(
            current_item_change=item_change1,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
        )
        self._add_item_version(item_version=item_version1)

        # Adding a change for another object

        item = self._create_item(
            id="915a67f9-e597-491a-a28f-cf0fda241b68", name="item_2", version="1"
        )
        self._add_item(item=item)

        item_change2 = ItemChange(
            id=uuid.UUID("c56c1211-6599-481f-9d47-71d3aafaf46d"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            operation=Operation.INSERT,
            item_id="915a67f9-e597-491a-a28f-cf0fda241b68",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            provider_id="provider_in_test",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="915a67f9-e597-491a-a28f-cf0fda241b68", name="item_2", version="1"
            ),
            should_ignore=False,
            is_applied=True,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        day=15,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=40,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )

        self._add_item_change(item_change=item_change2)

        # Adding a version for another object
        item_version2 = ItemVersion(
            current_item_change=item_change2,
            item_id="915a67f9-e597-491a-a28f-cf0fda241b68",
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
        )
        self._add_item_version(item_version=item_version2)

        # Testing
        local_version = self.data_store.get_local_version(item_id=item_change1.item_id)
        self.assertEqual(local_version, item_version1)

        local_version = self.data_store.get_local_version(item_id=item_change2.item_id)
        self.assertEqual(local_version, item_version2)

        with unittest.mock.patch(
            "sync_framework.core.utils.get_now_utc",
            return_value=dt.datetime(
                year=2021, month=6, day=25, hour=9, minute=37
            ).replace(tzinfo=dt.timezone.utc),
        ):
            blank_version = ItemVersion(
                current_item_change=None,
                item_id="2d24691e-7958-4ed9-830d-1afe7f5157e0",
                vector_clock=VectorClock(
                    VectorClockItem(
                        provider_id="1",
                        timestamp=dt.datetime.min.replace(tzinfo=dt.timezone.utc),
                    ),
                    VectorClockItem(
                        provider_id="2",
                        timestamp=dt.datetime.min.replace(tzinfo=dt.timezone.utc),
                    ),
                ),
                date_created=dt.datetime(
                    year=2021,
                    month=6,
                    day=25,
                    hour=9,
                    minute=37,
                    tzinfo=dt.timezone.utc,
                ),
            )
            local_version = self.data_store.get_local_version(
                item_id="2d24691e-7958-4ed9-830d-1afe7f5157e0"
            )
            self.assertEqual(local_version, blank_version)

    def test_get_or_create_item_change(self):
        item = self._create_item(
            id="2d6c7fef-a337-43cb-828a-4e6d2341ac7d", name="item_1", version="1"
        )
        self._add_item(item=item)

        item_change1 = ItemChange(
            id=uuid.UUID("3a5e71c9-da5c-461a-ae84-9001fd962925"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=9, tzinfo=dt.timezone.utc
            ),
            operation=Operation.UPDATE,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=9, tzinfo=dt.timezone.utc
            ),
            provider_id="provider_in_test",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=9, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="2"
            ),
            should_ignore=True,
            is_applied=True,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=9,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        day=15,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=40,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )
        self._add_item_change(item_change=item_change1)
        item_change1_reset = copy.deepcopy(item_change1)
        item_change1_reset.reset_status()

        self.assertTrue(item_change1.should_ignore)
        self.assertTrue(item_change1.is_applied)
        self.assertTrue(item_change1.date_created)
        self.assertFalse(item_change1_reset.should_ignore)
        self.assertFalse(item_change1_reset.is_applied)
        self.assertFalse(item_change1_reset.date_created)

        item_change2 = ItemChange(
            id=uuid.UUID("54417070-60f1-47e7-a2f3-7755dfafb194"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=10, tzinfo=dt.timezone.utc
            ),
            operation=Operation.UPDATE,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=9, tzinfo=dt.timezone.utc
            ),
            provider_id="other_provider",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=9, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="2.1"
            ),
            should_ignore=False,
            is_applied=True,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=11,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )
        self._add_item_change(item_change=item_change2)
        item_change2_reset = copy.deepcopy(item_change2)
        item_change2_reset.reset_status()
        self.assertFalse(item_change2.should_ignore)
        self.assertTrue(item_change2.is_applied)
        self.assertTrue(item_change2.date_created)
        self.assertFalse(item_change2_reset.should_ignore)
        self.assertFalse(item_change2_reset.is_applied)
        self.assertFalse(item_change2_reset.date_created)

        item_change3 = ItemChange(
            id=uuid.UUID("71049fff-73b8-44d0-ba3c-9d01fc9310a6"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            operation=Operation.UPDATE,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=10, tzinfo=dt.timezone.utc
            ),
            provider_id="other_provider",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=9, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="2.1"
            ),
            should_ignore=False,
            is_applied=False,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=11,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )

        # Testing
        item_change = self.data_store.get_or_create_item_change(
            item_change=item_change1_reset
        )
        self.assertEqual(item_change, item_change1)

        item_change = self.data_store.get_or_create_item_change(
            item_change=item_change2_reset
        )
        self.assertEqual(item_change, item_change2)

        item_change = self.data_store.get_or_create_item_change(
            item_change=item_change3
        )
        self.assertEqual(item_change, item_change3)

        self.assertEqual(
            self.data_store.get_item_changes(),
            [item_change1, item_change2, item_change3],
        )

    def test_serialize_item(self):
        item = self._create_item(
            id="2d24691e-7958-4ed9-830d-1afe7f5157e0", name="my item", version="1"
        )
        serialized = self.data_store.serialize_item(item=item)
        manually_serialized = self._serialize_item(
            id="2d24691e-7958-4ed9-830d-1afe7f5157e0", name="my item", version="1"
        )
        self.assertEqual(serialized, manually_serialized)

    def test_deserialize_item(self):
        item = self._create_item(
            id="2d24691e-7958-4ed9-830d-1afe7f5157e0", name="my item", version="1"
        )
        manually_serialized = self._serialize_item(
            id="2d24691e-7958-4ed9-830d-1afe7f5157e0", name="my item", version="1"
        )
        deserialized_item = self.data_store.deserialize_item(
            serialized_item=manually_serialized
        )
        self.assertEqual(deserialized_item, item)

    def test_get_local_vector_clock(self):

        # Adding a change to an object
        item = self._create_item(
            id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="1"
        )
        self._add_item(item=item)

        item_change1 = ItemChange(
            id=uuid.UUID("54423877-370a-4936-b362-419cc86abbb8"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=0, tzinfo=dt.timezone.utc
            ),
            operation=Operation.INSERT,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=0, tzinfo=dt.timezone.utc
            ),
            provider_id="provider_in_test",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=0, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="1"
            ),
            should_ignore=False,
            is_applied=True,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        day=15,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=40,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )

        self._add_item_change(item_change=item_change1)

        # Adding a change for another object
        item = self._create_item(
            id="915a67f9-e597-491a-a28f-cf0fda241b68", name="item_2", version="1"
        )
        self._add_item(item=item)

        item_change2 = ItemChange(
            id=uuid.UUID("c56c1211-6599-481f-9d47-71d3aafaf46d"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            operation=Operation.INSERT,
            item_id="915a67f9-e597-491a-a28f-cf0fda241b68",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            provider_id="provider_in_test",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="915a67f9-e597-491a-a28f-cf0fda241b68", name="item_2", version="1"
            ),
            should_ignore=False,
            is_applied=True,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        day=15,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=40,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )

        self._add_item_change(item_change=item_change2)

        item_change3 = ItemChange(
            id=uuid.UUID("3a5e71c9-da5c-461a-ae84-9001fd962925"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=9, tzinfo=dt.timezone.utc
            ),
            operation=Operation.UPDATE,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=9, tzinfo=dt.timezone.utc
            ),
            provider_id="provider_in_test",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=0, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="2"
            ),
            should_ignore=True,
            is_applied=True,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=9,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        day=15,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=40,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )
        self._add_item_change(item_change=item_change3)

        item_change4 = ItemChange(
            id=uuid.UUID("54417070-60f1-47e7-a2f3-7755dfafb194"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            operation=Operation.UPDATE,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            provider_id="other_provider",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=0, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="2.1"
            ),
            should_ignore=False,
            is_applied=False,
            vector_clock=VectorClock(
                *[
                    VectorClockItem(
                        provider_id="provider_in_test",
                        timestamp=dt.datetime(
                            year=2021,
                            month=6,
                            day=26,
                            hour=7,
                            minute=2,
                            tzinfo=dt.timezone.utc,
                        ),
                    ),
                    VectorClockItem(
                        provider_id="other_provider",
                        timestamp=dt.datetime(
                            year=2021,
                            month=6,
                            day=26,
                            hour=7,
                            minute=11,
                            tzinfo=dt.timezone.utc,
                        ),
                    ),
                ]
            ),
        )
        self._add_item_change(item_change=item_change4)

        result = self.data_store.get_local_vector_clock()

        self.assertEqual(
            result,
            VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=9,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=11,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )

    def test_get_item_change_by_id(self):
        item = self._create_item(
            id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="2.1"
        )
        self._add_item(item=item)

        item_change = ItemChange(
            id=uuid.UUID("54417070-60f1-47e7-a2f3-7755dfafb194"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            operation=Operation.UPDATE,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            provider_id="other_provider",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="other_provider",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="2.1"
            ),
            should_ignore=False,
            is_applied=False,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=11,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )
        self._add_item_change(item_change=item_change)

        result = self.data_store.get_item_change_by_id(id=item_change.id)
        self.assertEqual(result, item_change)

    def test_select_changes(self):
        # Adding a change to an object
        item = self._create_item(
            id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="1"
        )
        self._add_item(item=item)

        item_change1 = ItemChange(
            id=uuid.UUID("54423877-370a-4936-b362-419cc86abbb8"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=0, tzinfo=dt.timezone.utc
            ),
            operation=Operation.INSERT,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=0, tzinfo=dt.timezone.utc
            ),
            provider_id="provider_in_test",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=0, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="1"
            ),
            should_ignore=False,
            is_applied=True,
            vector_clock=VectorClock(
                *[
                    VectorClockItem(
                        provider_id="provider_in_test",
                        timestamp=dt.datetime(
                            year=2021,
                            month=6,
                            day=26,
                            hour=7,
                            minute=2,
                            tzinfo=dt.timezone.utc,
                        ),
                    ),
                    VectorClockItem(
                        provider_id="other_provider",
                        timestamp=dt.datetime(
                            day=15,
                            month=6,
                            year=2021,
                            hour=15,
                            minute=40,
                            tzinfo=dt.timezone.utc,
                        ),
                    ),
                ]
            ),
        )

        self._add_item_change(item_change=item_change1)

        # Adding a change for another object

        item = self._create_item(
            id="915a67f9-e597-491a-a28f-cf0fda241b68", name="item_2", version="1"
        )
        self._add_item(item=item)

        item_change2 = ItemChange(
            id=uuid.UUID("c56c1211-6599-481f-9d47-71d3aafaf46d"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            operation=Operation.INSERT,
            item_id="915a67f9-e597-491a-a28f-cf0fda241b68",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            provider_id="provider_in_test",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="915a67f9-e597-491a-a28f-cf0fda241b68", name="item_2", version="1"
            ),
            should_ignore=False,
            is_applied=True,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        day=15,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=40,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )

        self._add_item_change(item_change=item_change2)

        item_change3 = ItemChange(
            id=uuid.UUID("3a5e71c9-da5c-461a-ae84-9001fd962925"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=9, tzinfo=dt.timezone.utc
            ),
            operation=Operation.UPDATE,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=9, tzinfo=dt.timezone.utc
            ),
            provider_id="provider_in_test",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=0, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="2"
            ),
            should_ignore=True,
            is_applied=True,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=9,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        day=15,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=40,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )
        self._add_item_change(item_change=item_change3)

        item_change4 = ItemChange(
            id=uuid.UUID("54417070-60f1-47e7-a2f3-7755dfafb194"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            operation=Operation.UPDATE,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            provider_id="other_provider",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=0, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="2.1"
            ),
            should_ignore=True,
            is_applied=False,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=11,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )
        self._add_item_change(item_change=item_change4)

        item_change5 = ItemChange(
            id=uuid.UUID("4d2b46df-2237-436a-801f-14c2af296c4c"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=8, minute=3, tzinfo=dt.timezone.utc
            ),
            operation=Operation.UPDATE,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=8, tzinfo=dt.timezone.utc
            ),
            provider_id="other_provider",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=0, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="2.1"
            ),
            should_ignore=False,
            is_applied=True,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=9,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=8,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )
        self._add_item_change(item_change=item_change5)

        vector_clock1 = VectorClock.create_empty(
            provider_ids=["provider_in_test", "other_provider"]
        )

        result = self.data_store.select_changes(vector_clock=vector_clock1, max_num=10)

        self.assertEqual(
            result.item_changes,
            [item_change1, item_change2, item_change3, item_change4, item_change5,],
        )

        if not result.is_last_batch:
            next_vector_clock = result.get_vector_clock_after_done(
                initial_vector_clock=vector_clock1
            )
            result = self.data_store.select_changes(
                vector_clock=next_vector_clock, max_num=10
            )
            self.assertEqual(
                result, ItemChangeBatch(item_changes=[], is_last_batch=True,),
            )

        vector_clock2 = VectorClock(
            VectorClockItem(
                provider_id="provider_in_test",
                timestamp=dt.datetime(
                    year=2021,
                    month=6,
                    day=26,
                    hour=7,
                    minute=2,
                    tzinfo=dt.timezone.utc,
                ),
            ),
            VectorClockItem(
                provider_id="other_provider",
                timestamp=dt.datetime(
                    year=2021,
                    month=6,
                    day=26,
                    hour=8,
                    minute=0,
                    tzinfo=dt.timezone.utc,
                ),
            ),
        )

        result = self.data_store.select_changes(vector_clock=vector_clock2, max_num=10)

        self.assertEqual(
            result.item_changes, [item_change3,],
        )

        if not result.is_last_batch:
            next_vector_clock = result.get_vector_clock_after_done(
                initial_vector_clock=vector_clock2
            )
            result = self.data_store.select_changes(
                vector_clock=next_vector_clock, max_num=10
            )
            self.assertEqual(
                result, ItemChangeBatch(item_changes=[], is_last_batch=True,),
            )

    def test_select_deferred_changes(self):
        # Adding a change to an object
        item = self._create_item(
            id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="1"
        )
        self._add_item(item=item)

        item_change1 = ItemChange(
            id=uuid.UUID("54423877-370a-4936-b362-419cc86abbb8"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=0, tzinfo=dt.timezone.utc
            ),
            operation=Operation.INSERT,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=0, tzinfo=dt.timezone.utc
            ),
            provider_id="provider_in_test",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=0, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="1"
            ),
            should_ignore=False,
            is_applied=True,
            vector_clock=VectorClock(
                *[
                    VectorClockItem(
                        provider_id="provider_in_test",
                        timestamp=dt.datetime(
                            year=2021,
                            month=6,
                            day=26,
                            hour=7,
                            minute=2,
                            tzinfo=dt.timezone.utc,
                        ),
                    ),
                    VectorClockItem(
                        provider_id="other_provider",
                        timestamp=dt.datetime(
                            day=15,
                            month=6,
                            year=2021,
                            hour=15,
                            minute=40,
                            tzinfo=dt.timezone.utc,
                        ),
                    ),
                ]
            ),
        )

        self._add_item_change(item_change=item_change1)

        # Adding a change for another object
        item = self._create_item(
            id="915a67f9-e597-491a-a28f-cf0fda241b68", name="item_2", version="1"
        )
        self._add_item(item=item)

        item_change2 = ItemChange(
            id=uuid.UUID("c56c1211-6599-481f-9d47-71d3aafaf46d"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            operation=Operation.INSERT,
            item_id="915a67f9-e597-491a-a28f-cf0fda241b68",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            provider_id="provider_in_test",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="915a67f9-e597-491a-a28f-cf0fda241b68", name="item_2", version="1"
            ),
            should_ignore=False,
            is_applied=True,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        day=15,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=40,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )

        self._add_item_change(item_change=item_change2)

        item_change3 = ItemChange(
            id=uuid.UUID("3a5e71c9-da5c-461a-ae84-9001fd962925"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=9, tzinfo=dt.timezone.utc
            ),
            operation=Operation.UPDATE,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=9, tzinfo=dt.timezone.utc
            ),
            provider_id="provider_in_test",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=0, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="2"
            ),
            should_ignore=True,
            is_applied=True,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=9,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        day=15,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=40,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )
        self._add_item_change(item_change=item_change3)

        item_change4 = ItemChange(
            id=uuid.UUID("54417070-60f1-47e7-a2f3-7755dfafb194"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            operation=Operation.UPDATE,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            provider_id="other_provider",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=0, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="2.1"
            ),
            should_ignore=True,
            is_applied=False,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=11,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )
        self._add_item_change(item_change=item_change4)
        conflict_log4 = ConflictLog(
            id=uuid.UUID("019124a5-56b4-4d05-bcde-27751cd9c7c1"),
            created_at=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            resolved_at=None,
            item_change_loser=item_change4,
            item_change_winner=None,
            status=ConflictStatus.DEFERRED,
            conflict_type=ConflictType.EXCEPTION_OCCURRED,
            description="Error!",
        )

        self._add_conflict_log(conflict_log=conflict_log4)

        item_change5 = ItemChange(
            id=uuid.UUID("4d2b46df-2237-436a-801f-14c2af296c4c"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=8, minute=3, tzinfo=dt.timezone.utc
            ),
            operation=Operation.UPDATE,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=8, tzinfo=dt.timezone.utc
            ),
            provider_id="other_provider",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=0, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="2.1"
            ),
            should_ignore=False,
            is_applied=True,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=9,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=8,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )
        self._add_item_change(item_change=item_change5)

        vector_clock1 = VectorClock.create_empty(
            provider_ids=["provider_in_test", "other_provider"]
        )

        result = self.data_store.select_deferred_changes(
            vector_clock=vector_clock1, max_num=10
        )
        self.assertEqual(
            result.item_changes, [item_change4,],
        )

        if not result.is_last_batch:
            vector_clock2 = result.get_vector_clock_after_done(
                initial_vector_clock=vector_clock1
            )
            result = self.data_store.select_deferred_changes(
                vector_clock=vector_clock2, max_num=10
            )
            self.assertEqual(
                result, ItemChangeBatch(item_changes=[], is_last_batch=True,),
            )

    def test_save_item_change(self):
        item = self._create_item(
            id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="1"
        )
        self._add_item(item=item)

        item_change = ItemChange(
            id=uuid.UUID("54417070-60f1-47e7-a2f3-7755dfafb194"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            operation=Operation.UPDATE,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            provider_id="other_provider",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="other_provider",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="2.1"
            ),
            should_ignore=True,
            is_applied=False,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=11,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )
        result1 = self.data_store.save_item_change(item_change=item_change)
        self.assertEqual(item_change, result1)

        result2 = self.data_store.get_item_change_by_id(id=item_change.id)
        self.assertEqual(item_change, result2)

    def test_save_item(self):
        item = self._create_item(
            id="dd418733-571d-4208-af50-ef53765b9dac", name="hello", version="10"
        )
        self.data_store.save_item(item=item)

        result = self.data_store.get_item_by_id(
            id="dd418733-571d-4208-af50-ef53765b9dac"
        )
        self.assertEqual(item, result)

    def test_delete_item(self):
        item = self._create_item(
            id="dd418733-571d-4208-af50-ef53765b9dac", name="hello", version="10"
        )
        self._add_item(item=item)

        self.data_store.delete_item(item=item)

        with self.assertRaises(ItemNotFoundException):
            self.data_store.get_item_by_id(id="dd418733-571d-4208-af50-ef53765b9dac")

    def test_run_in_transaction(self):
        item = self._create_item(
            id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="2.1"
        )
        self._add_item(item=item)

        item_change = ItemChange(
            id=uuid.UUID("54417070-60f1-47e7-a2f3-7755dfafb194"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            operation=Operation.UPDATE,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            provider_id="other_provider",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="other_provider",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="2.1"
            ),
            should_ignore=True,
            is_applied=False,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=11,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )
        self._add_item_change(item_change=item_change)

        result = False

        def callback():
            nonlocal result
            result = True

        self.data_store.run_in_transaction(item_change=item_change, callback=callback)
        self.assertTrue(result)

    def test_save_conflict_log(self):
        item = self._create_item(
            id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="1"
        )
        self._add_item(item=item)

        item_change = ItemChange(
            id=uuid.UUID("54417070-60f1-47e7-a2f3-7755dfafb194"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            operation=Operation.UPDATE,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            provider_id="other_provider",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="other_provider",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="2.1"
            ),
            should_ignore=True,
            is_applied=False,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=11,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )
        self._add_item_change(item_change=item_change)
        conflict_log = ConflictLog(
            id=uuid.UUID("019124a5-56b4-4d05-bcde-27751cd9c7c1"),
            created_at=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            resolved_at=None,
            item_change_loser=item_change,
            item_change_winner=None,
            status=ConflictStatus.DEFERRED,
            conflict_type=ConflictType.EXCEPTION_OCCURRED,
            description="Error!",
        )

        self.data_store.save_conflict_log(conflict_log=conflict_log)
        conflict_logs = self.data_store.get_conflict_logs()
        self.assertEqual(conflict_logs, [conflict_log])

    def test_execute_item_change(self):
        # Insert
        item1 = self._create_item(
            id="dd418733-571d-4208-af50-ef53765b9dac", name="hello", version="12"
        )
        item_change1 = ItemChange(
            id=uuid.UUID("54417070-60f1-47e7-a2f3-7755dfafb194"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            operation=Operation.INSERT,
            item_id="dd418733-571d-4208-af50-ef53765b9dac",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            provider_id="other_provider",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="other_provider",
            serialized_item=self._serialize_item(
                id="dd418733-571d-4208-af50-ef53765b9dac", name="hello", version="12"
            ),
            should_ignore=True,
            is_applied=False,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=11,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )
        self.data_store.execute_item_change(item_change=item_change1)
        items = self.data_store.get_items()
        self.assertEqual(items, [item1])

        # Update
        item2 = self._create_item(
            id="dd418733-571d-4208-af50-ef53765b9dac", name="hello there", version="13"
        )
        item_change2 = ItemChange(
            id=uuid.UUID("54417070-60f1-47e7-a2f3-7755dfafb194"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=12, tzinfo=dt.timezone.utc
            ),
            operation=Operation.UPDATE,
            item_id="dd418733-571d-4208-af50-ef53765b9dac",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=12, tzinfo=dt.timezone.utc
            ),
            provider_id="other_provider",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="other_provider",
            serialized_item=self._serialize_item(
                id="dd418733-571d-4208-af50-ef53765b9dac",
                name="hello there",
                version="13",
            ),
            should_ignore=True,
            is_applied=False,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=12,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )
        self.data_store.execute_item_change(item_change=item_change2)
        items = self.data_store.get_items()
        self.assertEqual(items, [item2])

        # Delete
        item_change3 = ItemChange(
            id=uuid.UUID("54417070-60f1-47e7-a2f3-7755dfafb194"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=13, tzinfo=dt.timezone.utc
            ),
            operation=Operation.DELETE,
            item_id="dd418733-571d-4208-af50-ef53765b9dac",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=13, tzinfo=dt.timezone.utc
            ),
            provider_id="other_provider",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="other_provider",
            serialized_item=self._serialize_item(
                id="dd418733-571d-4208-af50-ef53765b9dac",
                name="hello there",
                version="13",
            ),
            should_ignore=True,
            is_applied=False,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=13,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )
        self.data_store.execute_item_change(item_change=item_change3)
        items = self.data_store.get_items()
        self.assertEqual(items, [])

    def test_save_item_version(self):
        item1 = self._create_item(
            id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="1"
        )
        self._add_item(item=item1)

        item_change1 = ItemChange(
            id=uuid.UUID("54423877-370a-4936-b362-419cc86abbb8"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            operation=Operation.INSERT,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            provider_id="provider_in_test",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="1"
            ),
            should_ignore=False,
            is_applied=True,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        day=15,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=40,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )

        self._add_item_change(item_change=item_change1)

        # Adding a version for the same object
        item_version1 = ItemVersion(
            current_item_change=item_change1,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
        )
        self.data_store.save_item_version(item_version=item_version1)
        item_versions = self.data_store.get_item_versions()
        self.assertEqual(item_versions, [item_version1])

    def test_get_deferred_conflict_logs(self):
        item1 = self._create_item(
            id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="1"
        )
        self._add_item(item=item1)

        item_change1 = ItemChange(
            id=uuid.UUID("54423877-370a-4936-b362-419cc86abbb8"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            operation=Operation.INSERT,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            provider_id="provider_in_test",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="1"
            ),
            should_ignore=False,
            is_applied=True,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        day=15,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=40,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )

        self._add_item_change(item_change=item_change1)

        conflict_log1 = ConflictLog(
            id=uuid.UUID("29200245-81bd-4ab7-b33a-da7adfdb989b"),
            created_at=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            resolved_at=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            item_change_loser=item_change1,
            item_change_winner=None,
            status=ConflictStatus.RESOLVED,
            conflict_type=ConflictType.EXCEPTION_OCCURRED,
            description="Error!",
        )

        self._add_conflict_log(conflict_log=conflict_log1)

        item2 = self._create_item(
            id="915a67f9-e597-491a-a28f-cf0fda241b68", name="item_2", version="1"
        )
        self._add_item(item=item2)

        item_change2 = ItemChange(
            id=uuid.UUID("c56c1211-6599-481f-9d47-71d3aafaf46d"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            operation=Operation.INSERT,
            item_id="915a67f9-e597-491a-a28f-cf0fda241b68",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            provider_id="provider_in_test",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="915a67f9-e597-491a-a28f-cf0fda241b68", name="item_2", version="1"
            ),
            should_ignore=False,
            is_applied=True,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        day=15,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=40,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )
        self._add_item_change(item_change=item_change2)

        conflict_log2 = ConflictLog(
            id=uuid.UUID("019124a5-56b4-4d05-bcde-27751cd9c7c1"),
            created_at=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            resolved_at=None,
            item_change_loser=item_change2,
            item_change_winner=None,
            status=ConflictStatus.DEFERRED,
            conflict_type=ConflictType.EXCEPTION_OCCURRED,
            description="Error!",
        )

        self._add_conflict_log(conflict_log=conflict_log2)

        result = self.data_store.get_deferred_conflict_logs(
            item_change_loser=item_change1
        )
        self.assertEqual(result, [])

        result = self.data_store.get_deferred_conflict_logs(
            item_change_loser=item_change2
        )
        self.assertEqual(result, [conflict_log2])

    def test_save_sync_session(self):
        item1 = self._create_item(
            id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="1"
        )
        self._add_item(item=item1)

        item_change1 = ItemChange(
            id=uuid.UUID("54423877-370a-4936-b362-419cc86abbb8"),
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            operation=Operation.INSERT,
            item_id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1",
            provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            provider_id="provider_in_test",
            insert_provider_timestamp=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            insert_provider_id="provider_in_test",
            serialized_item=self._serialize_item(
                id="e104b1c0-9a15-4ac1-b5fb-b273b91250d1", name="item_1", version="1"
            ),
            should_ignore=False,
            is_applied=True,
            vector_clock=VectorClock(
                VectorClockItem(
                    provider_id="provider_in_test",
                    timestamp=dt.datetime(
                        year=2021,
                        month=6,
                        day=26,
                        hour=7,
                        minute=2,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="other_provider",
                    timestamp=dt.datetime(
                        day=15,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=40,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )

        self._add_item_change(item_change=item_change1)

        sync_session = SyncSession(
            id=uuid.UUID("d797c785-f16b-488c-adfe-79c26717ad59"),
            started_at=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
            ended_at=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=3, tzinfo=dt.timezone.utc
            ),
            status=SyncSessionStatus.FINISHED,
            source_provider_id="other_provider",
            target_provider_id="provider_in_test",
            item_changes=[item_change1],
        )

        self.data_store.save_sync_session(sync_session=sync_session)
        sync_sessions = self.data_store.get_sync_sessions()

        self.assertEqual(sync_sessions, [sync_session])
