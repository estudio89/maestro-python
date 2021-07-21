from maestro.core.query.metadata import Filter, Comparison, Comparator, SortOrder
from maestro.backends.mongo.utils import convert_to_mongo_filter, convert_to_mongo_sort
import pymongo
import tests.base_store
import tests.mongo.base
import datetime as dt


class MongoStoreTest(
    tests.mongo.base.MongoBackendTestMixin,
    tests.base_store.BaseStoreTest,
    tests.mongo.base.MongoTestCase,
):
    def test_filter_conversion(self):
        """Tests the conversion of filter metadata to mongo filters"""

        # 1 - Combined filters
        comparison1 = Comparison(
            field_name="text", comparator=Comparator.EQUALS, value="Item 1"
        )
        filter1 = Filter(children=[comparison1])

        comparison2 = Comparison(
            field_name="done", comparator=Comparator.EQUALS, value=False
        )
        filter2 = Filter(children=[comparison2])

        date = dt.datetime(
            year=2021, month=7, day=19, hour=18, minute=35, tzinfo=dt.timezone.utc
        )
        comparison3 = Comparison(
            field_name="date_created", comparator=Comparator.GREATER_THAN, value=date
        )
        filter3 = Filter(children=[comparison3])

        combined_filter1 = filter1 & (filter2 | filter3)
        mongo_filter1 = convert_to_mongo_filter(
            filter=combined_filter1, field_prefix="serialized_item."
        )
        self.assertEqual(
            mongo_filter1,
            {
                "$and": [
                    {"serialized_item.text": {"$eq": "Item 1"}},
                    {
                        "$or": [
                            {"serialized_item.done": {"$eq": False}},
                            {"serialized_item.date_created": {"$gt": date}},
                        ]
                    },
                ]
            },
        )

        combined_filter2 = (filter1 & filter2) | filter3
        mongo_filter2 = convert_to_mongo_filter(
            filter=combined_filter2, field_prefix="serialized_item."
        )
        self.assertEqual(
            mongo_filter2,
            {
                "$or": [
                    {
                        "$and": [
                            {"serialized_item.text": {"$eq": "Item 1"}},
                            {"serialized_item.done": {"$eq": False}},
                        ]
                    },
                    {"serialized_item.date_created": {"$gt": date}},
                ]
            },
        )

        combined_filter3 = filter1 & filter2 & filter3
        mongo_filter3 = convert_to_mongo_filter(
            filter=combined_filter3, field_prefix="serialized_item."
        )

        self.assertEqual(
            mongo_filter3,
            {
                "$and": [
                    {"serialized_item.text": {"$eq": "Item 1"}},
                    {"serialized_item.done": {"$eq": False}},
                    {"serialized_item.date_created": {"$gt": date}},
                ]
            },
        )

        combined_filter4 = filter1 & filter2 | filter3
        mongo_filter4 = convert_to_mongo_filter(
            filter=combined_filter4, field_prefix="serialized_item."
        )

        self.assertEqual(
            mongo_filter4,
            {
                "$or": [
                    {
                        "$and": [
                            {"serialized_item.text": {"$eq": "Item 1"}},
                            {"serialized_item.done": {"$eq": False}},
                        ]
                    },
                    {"serialized_item.date_created": {"$gt": date}},
                ]
            },
        )

        # 2 - Operators

        comparison5 = Comparison(
            field_name="text", comparator=Comparator.NOT_EQUALS, value="Item 1"
        )
        filter5 = Filter(children=[comparison5])
        mongo_filter5 = convert_to_mongo_filter(
            filter=filter5, field_prefix="serialized_item."
        )
        self.assertEqual(mongo_filter5, {"serialized_item.text": {"$ne": "Item 1"}})

        comparison6 = Comparison(
            field_name="date_created", comparator=Comparator.LESS_THAN, value=date
        )
        filter6 = Filter(children=[comparison6])
        mongo_filter6 = convert_to_mongo_filter(
            filter=filter6, field_prefix="serialized_item."
        )
        self.assertEqual(mongo_filter6, {"serialized_item.date_created": {"$lt": date}})

        comparison7 = Comparison(
            field_name="date_created",
            comparator=Comparator.LESS_THAN_OR_EQUALS,
            value=date,
        )
        filter7 = Filter(children=[comparison7])
        mongo_filter7 = convert_to_mongo_filter(
            filter=filter7, field_prefix="serialized_item."
        )
        self.assertEqual(
            mongo_filter7, {"serialized_item.date_created": {"$lte": date}}
        )

        comparison8 = Comparison(
            field_name="date_created", comparator=Comparator.GREATER_THAN, value=date
        )
        filter8 = Filter(children=[comparison8])
        mongo_filter8 = convert_to_mongo_filter(
            filter=filter8, field_prefix="serialized_item."
        )
        self.assertEqual(mongo_filter8, {"serialized_item.date_created": {"$gt": date}})

        comparison8 = Comparison(
            field_name="date_created",
            comparator=Comparator.GREATER_THAN_OR_EQUALS,
            value=date,
        )
        filter8 = Filter(children=[comparison8])
        mongo_filter8 = convert_to_mongo_filter(
            filter=filter8, field_prefix="serialized_item."
        )
        self.assertEqual(
            mongo_filter8, {"serialized_item.date_created": {"$gte": date}}
        )

        comparison9 = Comparison(
            field_name="text", comparator=Comparator.IN, value=["Item 1", "Item 2"]
        )
        filter9 = Filter(children=[comparison9])
        mongo_filter9 = convert_to_mongo_filter(
            filter=filter9, field_prefix="serialized_item."
        )
        self.assertEqual(
            mongo_filter9, {"serialized_item.text": {"$in": ["Item 1", "Item 2"]}}
        )

        comparison10 = Comparison(
            field_name="text", comparator="WRONG", value=["Item 1", "Item 2"]
        )
        filter10 = Filter(children=[comparison10])
        with self.assertRaises(ValueError):
            convert_to_mongo_filter(filter=filter10, field_prefix="serialized_item.")

    def test_sort_conversion(self):
        """Tests the conversion of sort metadata to mongo sorts"""

        ordering = [
            SortOrder(field_name="field1"),
            SortOrder(field_name="field2", descending=True),
        ]
        mongo_ordering = convert_to_mongo_sort(ordering=ordering)

        self.assertEqual(
            mongo_ordering, {"field1": pymongo.ASCENDING, "field2": pymongo.DESCENDING}
        )
