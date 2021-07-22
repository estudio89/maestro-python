import unittest
from typing import List
from maestro.core.metadata import (
    VectorClockItem,
    VectorClock,
    ItemChange,
    ItemChangeBatch,
    Operation,
    SerializationResult,
)
import datetime as dt
import uuid


class VectorClockTest(unittest.TestCase):
    def setUp(self):
        self.timestamp1 = dt.datetime(day=17, month=6, year=2021, hour=15, minute=44)
        self.timestamp2 = dt.datetime(day=17, month=6, year=2021, hour=15, minute=45)
        self.timestamp3 = dt.datetime(day=17, month=6, year=2021, hour=15, minute=46)

        self.vector_clock_item1 = VectorClockItem(
            provider_id="provider1", timestamp=self.timestamp1
        )
        self.vector_clock_item2 = VectorClockItem(
            provider_id="provider2", timestamp=self.timestamp2
        )
        self.vector_clock_item3 = VectorClockItem(
            provider_id="provider3", timestamp=self.timestamp3
        )

        self.vector_clock = VectorClock(
            self.vector_clock_item1, self.vector_clock_item2, self.vector_clock_item3
        )

    def test_iteration(self):
        """ Tests iterating a VectorClock."""

        values = list(self.vector_clock)

        self.assertEqual(
            [val.timestamp for val in values],
            [self.timestamp1, self.timestamp2, self.timestamp3],
        )
        self.assertEqual(
            [val.provider_id for val in values], ["provider1", "provider2", "provider3"]
        )

    def test_retrieval(self):
        """Tests obtaining a VectorClockItem from a provider's id. """

        item = self.vector_clock.get_vector_clock_item(provider_id="provider1")
        self.assertEqual(item.provider_id, "provider1")
        self.assertEqual(item.timestamp, self.timestamp1)

        item = self.vector_clock.get_vector_clock_item(provider_id="provider2")
        self.assertEqual(item.provider_id, "provider2")
        self.assertEqual(item.timestamp, self.timestamp2)

        item = self.vector_clock.get_vector_clock_item(provider_id="provider3")
        self.assertEqual(item.provider_id, "provider3")
        self.assertEqual(item.timestamp, self.timestamp3)

        item = self.vector_clock.get_vector_clock_item(provider_id="test")
        self.assertEqual(item.provider_id, "test")
        self.assertEqual(
            item.timestamp, dt.datetime.min.replace(tzinfo=dt.timezone.utc)
        )

    def test_vector_clock_item_comparison(self):
        """ Tests the comparison of two VectorClockItems. """
        timestamp1 = dt.datetime(day=17, month=6, year=2021, hour=15, minute=44)
        timestamp2 = dt.datetime(day=17, month=6, year=2021, hour=15, minute=45)
        timestamp3 = dt.datetime(day=17, month=6, year=2021, hour=15, minute=46)

        vector_clock_item1 = VectorClockItem(
            provider_id="provider1", timestamp=timestamp1
        )
        vector_clock_item2 = VectorClockItem(
            provider_id="provider1", timestamp=timestamp2
        )
        vector_clock_item3 = VectorClockItem(
            provider_id="provider2", timestamp=timestamp3
        )
        vector_clock_item4 = VectorClockItem(
            provider_id="provider1", timestamp=timestamp1
        )

        self.assertTrue(vector_clock_item1 < vector_clock_item2)
        self.assertTrue(vector_clock_item2 > vector_clock_item1)
        self.assertEqual(vector_clock_item1, vector_clock_item4)

        with self.assertRaises(AssertionError):
            vector_clock_item3 > vector_clock_item1

        with self.assertRaises(AssertionError):
            vector_clock_item3 == "asd"

    def test_comparison(self):
        """ Tests the comparison of two VectorClocks. """

        # Situation 1 - Identical
        other_timestamp1 = dt.datetime(day=17, month=6, year=2021, hour=15, minute=44)
        other_timestamp2 = dt.datetime(day=17, month=6, year=2021, hour=15, minute=45)
        other_timestamp3 = dt.datetime(day=17, month=6, year=2021, hour=15, minute=46)

        other_vector_clock_item1 = VectorClockItem(
            provider_id="provider1", timestamp=other_timestamp1
        )
        other_vector_clock_item2 = VectorClockItem(
            provider_id="provider2", timestamp=other_timestamp2
        )
        other_vector_clock_item3 = VectorClockItem(
            provider_id="provider3", timestamp=other_timestamp3
        )

        other_vector_clock1 = VectorClock(
            other_vector_clock_item1, other_vector_clock_item2, other_vector_clock_item3
        )

        self.assertEqual(self.vector_clock, other_vector_clock1)

        # Situation 2 - one provider has a different timestamp

        other_timestamp3_2 = dt.datetime(day=18, month=6, year=2021, hour=15, minute=46)
        other_vector_clock_item3_2 = VectorClockItem(
            provider_id="provider3", timestamp=other_timestamp3_2
        )
        other_vector_clock2 = VectorClock(
            other_vector_clock_item1,
            other_vector_clock_item2,
            other_vector_clock_item3_2,
        )
        self.assertNotEqual(self.vector_clock, other_vector_clock2)

        # Situation 3 - the second VectorClock has an extra provider
        other_timestamp4 = dt.datetime(day=17, month=6, year=2021, hour=15, minute=47)
        other_vector_clock_item4 = VectorClockItem(
            provider_id="provider4", timestamp=other_timestamp4
        )
        other_vector_clock3 = VectorClock(
            other_vector_clock_item1,
            other_vector_clock_item2,
            other_vector_clock_item3,
            other_vector_clock_item4,
        )
        self.assertNotEqual(self.vector_clock, other_vector_clock3)

        # Situation 4 - Comparison with instance from another class
        with self.assertRaises(AssertionError):
            self.vector_clock == "error"

    def test_update(self):
        new_timestamp = dt.datetime(day=17, month=6, year=2021, hour=15, minute=47)
        self.vector_clock.update_vector_clock_item("provider1", new_timestamp)

        self.assertEqual(
            self.vector_clock.get_vector_clock_item("provider1").timestamp,
            new_timestamp,
        )
        self.assertEqual(
            self.vector_clock.get_vector_clock_item("provider2").timestamp,
            self.timestamp2,
        )
        self.assertEqual(
            self.vector_clock.get_vector_clock_item("provider3").timestamp,
            self.timestamp3,
        )


class ItemChangeBatchTest(unittest.TestCase):
    def test_vector_clock_after_done(self):
        item_changes: "List[ItemChange]" = [
            ItemChange(
                id=uuid.uuid4(),
                operation=Operation.INSERT,
                serialization_result=SerializationResult(
                    item_id=uuid.uuid4(), serialized_item="", entity_name="my_app_item"
                ),
                change_vector_clock_item=VectorClockItem(
                    timestamp=dt.datetime(
                        day=17,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=44,
                        tzinfo=dt.timezone.utc,
                    ),
                    provider_id="provider1",
                ),
                insert_vector_clock_item=VectorClockItem(
                    timestamp=dt.datetime(
                        day=17,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=44,
                        tzinfo=dt.timezone.utc,
                    ),
                    provider_id="provider1",
                ),
                should_ignore=False,
                is_applied=False,
                date_created=dt.datetime(
                    day=17,
                    month=6,
                    year=2021,
                    hour=15,
                    minute=44,
                    tzinfo=dt.timezone.utc,
                ),
                vector_clock=VectorClock(
                    VectorClockItem(
                        provider_id="provider1",
                        timestamp=dt.datetime(
                            day=17,
                            month=6,
                            year=2021,
                            hour=15,
                            minute=44,
                            tzinfo=dt.timezone.utc,
                        ),
                    ),
                    VectorClockItem(
                        provider_id="provider2",
                        timestamp=dt.datetime(
                            day=15,
                            month=6,
                            year=2021,
                            hour=15,
                            minute=40,
                            tzinfo=dt.timezone.utc,
                        ),
                    ),
                    VectorClockItem(
                        provider_id="provider3",
                        timestamp=dt.datetime(
                            day=14,
                            month=6,
                            year=2021,
                            hour=15,
                            minute=40,
                            tzinfo=dt.timezone.utc,
                        ),
                    ),
                ),
            ),
            ItemChange(
                id=uuid.uuid4(),
                operation=Operation.UPDATE,
                serialization_result=SerializationResult(
                    item_id=uuid.uuid4(), serialized_item="", entity_name="my_app_item"
                ),
                date_created=dt.datetime(
                    day=18,
                    month=6,
                    year=2021,
                    hour=11,
                    minute=25,
                    tzinfo=dt.timezone.utc,
                ),
                change_vector_clock_item=VectorClockItem(
                    timestamp=dt.datetime(
                        day=18,
                        month=6,
                        year=2021,
                        hour=11,
                        minute=25,
                        tzinfo=dt.timezone.utc,
                    ),
                    provider_id="provider1",
                ),
                insert_vector_clock_item=VectorClockItem(
                    timestamp=dt.datetime(
                        day=17,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=44,
                        tzinfo=dt.timezone.utc,
                    ),
                    provider_id="provider1",
                ),
                should_ignore=False,
                is_applied=False,
                vector_clock=VectorClock(
                    VectorClockItem(
                        provider_id="provider1",
                        timestamp=dt.datetime(
                            day=18,
                            month=6,
                            year=2021,
                            hour=11,
                            minute=25,
                            tzinfo=dt.timezone.utc,
                        ),
                    ),
                    VectorClockItem(
                        provider_id="provider2",
                        timestamp=dt.datetime(
                            day=15,
                            month=6,
                            year=2021,
                            hour=15,
                            minute=40,
                            tzinfo=dt.timezone.utc,
                        ),
                    ),
                    VectorClockItem(
                        provider_id="provider3",
                        timestamp=dt.datetime(
                            day=14,
                            month=6,
                            year=2021,
                            hour=15,
                            minute=40,
                            tzinfo=dt.timezone.utc,
                        ),
                    ),
                ),
            ),
            ItemChange(
                id=uuid.uuid4(),
                operation=Operation.UPDATE,
                serialization_result=SerializationResult(
                    item_id=uuid.uuid4(), serialized_item="", entity_name="my_app_item"
                ),
                date_created=dt.datetime(
                    day=19,
                    month=6,
                    year=2021,
                    hour=15,
                    minute=50,
                    tzinfo=dt.timezone.utc,
                ),
                change_vector_clock_item=VectorClockItem(
                    timestamp=dt.datetime(
                        day=19,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=50,
                        tzinfo=dt.timezone.utc,
                    ),
                    provider_id="provider1",
                ),
                insert_vector_clock_item=VectorClockItem(
                    timestamp=dt.datetime(
                        day=17,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=44,
                        tzinfo=dt.timezone.utc,
                    ),
                    provider_id="provider1",
                ),
                should_ignore=False,
                is_applied=False,
                vector_clock=VectorClock(
                    VectorClockItem(
                        provider_id="provider1",
                        timestamp=dt.datetime(
                            day=19,
                            month=6,
                            year=2021,
                            hour=15,
                            minute=50,
                            tzinfo=dt.timezone.utc,
                        ),
                    ),
                    VectorClockItem(
                        provider_id="provider2",
                        timestamp=dt.datetime(
                            day=15,
                            month=6,
                            year=2021,
                            hour=15,
                            minute=40,
                            tzinfo=dt.timezone.utc,
                        ),
                    ),
                    VectorClockItem(
                        provider_id="provider3",
                        timestamp=dt.datetime(
                            day=19,
                            month=6,
                            year=2021,
                            hour=15,
                            minute=40,
                            tzinfo=dt.timezone.utc,
                        ),
                    ),
                ),
            ),
        ]
        item_change_batch = ItemChangeBatch(
            item_changes=item_changes, is_last_batch=True
        )
        initial_vector_clock = VectorClock(
            VectorClockItem(
                provider_id="provider1",
                timestamp=dt.datetime(
                    day=16,
                    month=6,
                    year=2021,
                    hour=15,
                    minute=0,
                    tzinfo=dt.timezone.utc,
                ),
            ),
            VectorClockItem(
                provider_id="provider2",
                timestamp=dt.datetime(
                    day=15,
                    month=6,
                    year=2021,
                    hour=15,
                    minute=40,
                    tzinfo=dt.timezone.utc,
                ),
            ),
            VectorClockItem(
                provider_id="provider3",
                timestamp=dt.datetime(
                    day=14,
                    month=6,
                    year=2021,
                    hour=15,
                    minute=40,
                    tzinfo=dt.timezone.utc,
                ),
            ),
        )
        final_vector_clock = item_change_batch.get_vector_clock_after_done(
            initial_vector_clock
        )

        self.assertEqual(
            final_vector_clock,
            VectorClock(
                VectorClockItem(
                    provider_id="provider1",
                    timestamp=dt.datetime(
                        day=19,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=50,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="provider2",
                    timestamp=dt.datetime(
                        day=15,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=40,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
                VectorClockItem(
                    provider_id="provider3",
                    timestamp=dt.datetime(
                        day=14,
                        month=6,
                        year=2021,
                        hour=15,
                        minute=40,
                        tzinfo=dt.timezone.utc,
                    ),
                ),
            ),
        )
