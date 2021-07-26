import unittest
import unittest.mock
from maestro.core.serializer import (
    MetadataSerializer,
    RawDataStoreJSONSerializer,
)
from maestro.core.metadata import (
    VectorClockItem,
    VectorClock,
    ConflictLog,
    ConflictStatus,
    ConflictType,
    ItemChange,
    ItemVersion,
    Operation,
    SyncSession,
    SyncSessionStatus,
    SerializationResult,
)
import datetime as dt
import uuid


class MetadataSerializerTest(unittest.TestCase):
    def setUp(self):
        self.item_change = ItemChange(
            id=uuid.UUID("14717ffa-5f72-4d3d-91b4-2abf7ac0fdb7"),
            operation=Operation.INSERT,
            serialization_result=SerializationResult(
                item_id=uuid.UUID("69bdd54f-4048-43ca-8aa1-529cd790098e"),
                serialized_item="",
                entity_name="my_app_item",
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
                day=17, month=6, year=2021, hour=15, minute=44, tzinfo=dt.timezone.utc,
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
        )

        self.conflict_log = ConflictLog(
            id=uuid.UUID("019124a5-56b4-4d05-bcde-27751cd9c7c1"),
            created_at=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=11, tzinfo=dt.timezone.utc
            ),
            resolved_at=None,
            item_change_loser=self.item_change,
            item_change_winner=None,
            status=ConflictStatus.DEFERRED,
            conflict_type=ConflictType.EXCEPTION_OCCURRED,
            description="Error!",
        )

        self.sync_session = SyncSession(
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
            item_changes=[self.item_change],
        )

        self.item_version = ItemVersion(
            current_item_change=self.item_change,
            item_id="69bdd54f-4048-43ca-8aa1-529cd790098e",
            date_created=dt.datetime(
                year=2021, month=6, day=26, hour=7, minute=2, tzinfo=dt.timezone.utc
            ),
        )

        self.serializer = MetadataSerializer()

    def test_serialize_item_change(self):

        result = self.serializer.serialize(metadata_object=self.item_change)

        self.assertEqual(
            result,
            {
                "id": "14717ffa-5f72-4d3d-91b4-2abf7ac0fdb7",
                "date_created": "2021-06-17T15:44:00+00:00",
                "operation": "Operation.INSERT",
                "serialization_result": {
                    "item_id": "69bdd54f-4048-43ca-8aa1-529cd790098e",
                    "serialized_item": "",
                    "entity_name": "my_app_item",
                },
                "change_vector_clock_item": {
                    "timestamp": "2021-06-17T15:44:00+00:00",
                    "provider_id": "provider1",
                },
                "insert_vector_clock_item": {
                    "timestamp": "2021-06-17T15:44:00+00:00",
                    "provider_id": "provider1",
                },
                "should_ignore": False,
                "is_applied": False,
                "vector_clock": [
                    {
                        "provider_id": "provider1",
                        "timestamp": "2021-06-17T15:44:00+00:00",
                    },
                    {
                        "provider_id": "provider2",
                        "timestamp": "2021-06-15T15:40:00+00:00",
                    },
                    {
                        "provider_id": "provider3",
                        "timestamp": "2021-06-14T15:40:00+00:00",
                    },
                ],
            },
        )

    def test_serialize_conflict_log(self):
        result = self.serializer.serialize(metadata_object=self.conflict_log)

        self.assertEqual(
            result,
            {
                "id": "019124a5-56b4-4d05-bcde-27751cd9c7c1",
                "created_at": "2021-06-26T07:11:00+00:00",
                "resolved_at": None,
                "item_change_loser": {
                    "id": "14717ffa-5f72-4d3d-91b4-2abf7ac0fdb7",
                    "date_created": "2021-06-17T15:44:00+00:00",
                    "operation": "Operation.INSERT",
                    "serialization_result": {
                        "item_id": "69bdd54f-4048-43ca-8aa1-529cd790098e",
                        "serialized_item": "",
                        "entity_name": "my_app_item",
                    },
                    "change_vector_clock_item": {
                        "timestamp": "2021-06-17T15:44:00+00:00",
                        "provider_id": "provider1",
                    },
                    "insert_vector_clock_item": {
                        "timestamp": "2021-06-17T15:44:00+00:00",
                        "provider_id": "provider1",
                    },
                    "should_ignore": False,
                    "is_applied": False,
                    "vector_clock": [
                        {
                            "provider_id": "provider1",
                            "timestamp": "2021-06-17T15:44:00+00:00",
                        },
                        {
                            "provider_id": "provider2",
                            "timestamp": "2021-06-15T15:40:00+00:00",
                        },
                        {
                            "provider_id": "provider3",
                            "timestamp": "2021-06-14T15:40:00+00:00",
                        },
                    ],
                },
                "item_change_winner": None,
                "status": "ConflictStatus.DEFERRED",
                "conflict_type": "ConflictType.EXCEPTION_OCCURRED",
                "description": "Error!",
            },
        )

    def test_serialize_sync_session(self):
        result = self.serializer.serialize(metadata_object=self.sync_session)

        self.assertEqual(
            result,
            {
                "id": "d797c785-f16b-488c-adfe-79c26717ad59",
                "started_at": "2021-06-26T07:02:00+00:00",
                "ended_at": "2021-06-26T07:03:00+00:00",
                "status": "SyncSessionStatus.FINISHED",
                "source_provider_id": "other_provider",
                "target_provider_id": "provider_in_test",
                "query_id": None,
                "item_changes": [
                    {
                        "id": "14717ffa-5f72-4d3d-91b4-2abf7ac0fdb7",
                        "date_created": "2021-06-17T15:44:00+00:00",
                        "operation": "Operation.INSERT",
                        "serialization_result": {
                            "item_id": "69bdd54f-4048-43ca-8aa1-529cd790098e",
                            "serialized_item": "",
                            "entity_name": "my_app_item",
                        },
                        "change_vector_clock_item": {
                            "timestamp": "2021-06-17T15:44:00+00:00",
                            "provider_id": "provider1",
                        },
                        "insert_vector_clock_item": {
                            "timestamp": "2021-06-17T15:44:00+00:00",
                            "provider_id": "provider1",
                        },
                        "should_ignore": False,
                        "is_applied": False,
                        "vector_clock": [
                            {
                                "provider_id": "provider1",
                                "timestamp": "2021-06-17T15:44:00+00:00",
                            },
                            {
                                "provider_id": "provider2",
                                "timestamp": "2021-06-15T15:40:00+00:00",
                            },
                            {
                                "provider_id": "provider3",
                                "timestamp": "2021-06-14T15:40:00+00:00",
                            },
                        ],
                    }
                ],
            },
        )

    def test_serialize_item_version(self):
        result = self.serializer.serialize(metadata_object=self.item_version)
        self.assertEqual(
            result,
            {
                "current_item_change": {
                    "id": "14717ffa-5f72-4d3d-91b4-2abf7ac0fdb7",
                    "date_created": "2021-06-17T15:44:00+00:00",
                    "operation": "Operation.INSERT",
                    "serialization_result": {
                        "item_id": "69bdd54f-4048-43ca-8aa1-529cd790098e",
                        "serialized_item": "",
                        "entity_name": "my_app_item",
                    },
                    "change_vector_clock_item": {
                        "timestamp": "2021-06-17T15:44:00+00:00",
                        "provider_id": "provider1",
                    },
                    "insert_vector_clock_item": {
                        "timestamp": "2021-06-17T15:44:00+00:00",
                        "provider_id": "provider1",
                    },
                    "should_ignore": False,
                    "is_applied": False,
                    "vector_clock": [
                        {
                            "provider_id": "provider1",
                            "timestamp": "2021-06-17T15:44:00+00:00",
                        },
                        {
                            "provider_id": "provider2",
                            "timestamp": "2021-06-15T15:40:00+00:00",
                        },
                        {
                            "provider_id": "provider3",
                            "timestamp": "2021-06-14T15:40:00+00:00",
                        },
                    ],
                },
                "item_id": "69bdd54f-4048-43ca-8aa1-529cd790098e",
                "vector_clock": [
                    {
                        "provider_id": "provider1",
                        "timestamp": "2021-06-17T15:44:00+00:00",
                    },
                    {
                        "provider_id": "provider2",
                        "timestamp": "2021-06-15T15:40:00+00:00",
                    },
                    {
                        "provider_id": "provider3",
                        "timestamp": "2021-06-14T15:40:00+00:00",
                    },
                ],
                "date_created": "2021-06-26T07:02:00+00:00",
            },
        )

    def test_raw_data_serializer(self):
        serializer = RawDataStoreJSONSerializer(
            metadata_serializer=self.serializer, indent=0
        )

        db = {
            "conflict_logs": [self.conflict_log],
            "item_changes": [self.item_change],
            "item_versions": [self.item_version],
            "sync_sessions": [self.sync_session],
            "items": [],
        }
        data_store = unittest.mock.MagicMock(
            _get_raw_db=unittest.mock.MagicMock(return_value=db)
        )
        result = serializer.serialize(data_store=data_store)

        self.assertEqual(
            result,
            '{\n"conflict_logs": [\n{\n"id": "019124a5-56b4-4d05-bcde-27751cd9c7c1",\n"created_at": "2021-06-26T07:11:00+00:00",\n"resolved_at": null,\n"item_change_loser": {\n"id": "14717ffa-5f72-4d3d-91b4-2abf7ac0fdb7",\n"date_created": "2021-06-17T15:44:00+00:00",\n"operation": "Operation.INSERT",\n"change_vector_clock_item": {\n"provider_id": "provider1",\n"timestamp": "2021-06-17T15:44:00+00:00"\n},\n"insert_vector_clock_item": {\n"provider_id": "provider1",\n"timestamp": "2021-06-17T15:44:00+00:00"\n},\n"serialization_result": {\n"item_id": "69bdd54f-4048-43ca-8aa1-529cd790098e",\n"entity_name": "my_app_item",\n"serialized_item": ""\n},\n"should_ignore": false,\n"is_applied": false,\n"vector_clock": [\n{\n"provider_id": "provider1",\n"timestamp": "2021-06-17T15:44:00+00:00"\n},\n{\n"provider_id": "provider2",\n"timestamp": "2021-06-15T15:40:00+00:00"\n},\n{\n"provider_id": "provider3",\n"timestamp": "2021-06-14T15:40:00+00:00"\n}\n]\n},\n"item_change_winner": null,\n"status": "ConflictStatus.DEFERRED",\n"conflict_type": "ConflictType.EXCEPTION_OCCURRED",\n"description": "Error!"\n}\n],\n"item_changes": [\n{\n"id": "14717ffa-5f72-4d3d-91b4-2abf7ac0fdb7",\n"date_created": "2021-06-17T15:44:00+00:00",\n"operation": "Operation.INSERT",\n"change_vector_clock_item": {\n"provider_id": "provider1",\n"timestamp": "2021-06-17T15:44:00+00:00"\n},\n"insert_vector_clock_item": {\n"provider_id": "provider1",\n"timestamp": "2021-06-17T15:44:00+00:00"\n},\n"serialization_result": {\n"item_id": "69bdd54f-4048-43ca-8aa1-529cd790098e",\n"entity_name": "my_app_item",\n"serialized_item": ""\n},\n"should_ignore": false,\n"is_applied": false,\n"vector_clock": [\n{\n"provider_id": "provider1",\n"timestamp": "2021-06-17T15:44:00+00:00"\n},\n{\n"provider_id": "provider2",\n"timestamp": "2021-06-15T15:40:00+00:00"\n},\n{\n"provider_id": "provider3",\n"timestamp": "2021-06-14T15:40:00+00:00"\n}\n]\n}\n],\n"item_versions": [\n{\n"current_item_change": {\n"id": "14717ffa-5f72-4d3d-91b4-2abf7ac0fdb7",\n"date_created": "2021-06-17T15:44:00+00:00",\n"operation": "Operation.INSERT",\n"change_vector_clock_item": {\n"provider_id": "provider1",\n"timestamp": "2021-06-17T15:44:00+00:00"\n},\n"insert_vector_clock_item": {\n"provider_id": "provider1",\n"timestamp": "2021-06-17T15:44:00+00:00"\n},\n"serialization_result": {\n"item_id": "69bdd54f-4048-43ca-8aa1-529cd790098e",\n"entity_name": "my_app_item",\n"serialized_item": ""\n},\n"should_ignore": false,\n"is_applied": false,\n"vector_clock": [\n{\n"provider_id": "provider1",\n"timestamp": "2021-06-17T15:44:00+00:00"\n},\n{\n"provider_id": "provider2",\n"timestamp": "2021-06-15T15:40:00+00:00"\n},\n{\n"provider_id": "provider3",\n"timestamp": "2021-06-14T15:40:00+00:00"\n}\n]\n},\n"item_id": "69bdd54f-4048-43ca-8aa1-529cd790098e",\n"vector_clock": [\n{\n"provider_id": "provider1",\n"timestamp": "2021-06-17T15:44:00+00:00"\n},\n{\n"provider_id": "provider2",\n"timestamp": "2021-06-15T15:40:00+00:00"\n},\n{\n"provider_id": "provider3",\n"timestamp": "2021-06-14T15:40:00+00:00"\n}\n],\n"date_created": "2021-06-26T07:02:00+00:00"\n}\n],\n"sync_sessions": [\n{\n"id": "d797c785-f16b-488c-adfe-79c26717ad59",\n"started_at": "2021-06-26T07:02:00+00:00",\n"ended_at": "2021-06-26T07:03:00+00:00",\n"status": "SyncSessionStatus.FINISHED",\n"source_provider_id": "other_provider",\n"target_provider_id": "provider_in_test",\n"item_changes": [\n{\n"id": "14717ffa-5f72-4d3d-91b4-2abf7ac0fdb7",\n"date_created": "2021-06-17T15:44:00+00:00",\n"operation": "Operation.INSERT",\n"change_vector_clock_item": {\n"provider_id": "provider1",\n"timestamp": "2021-06-17T15:44:00+00:00"\n},\n"insert_vector_clock_item": {\n"provider_id": "provider1",\n"timestamp": "2021-06-17T15:44:00+00:00"\n},\n"serialization_result": {\n"item_id": "69bdd54f-4048-43ca-8aa1-529cd790098e",\n"entity_name": "my_app_item",\n"serialized_item": ""\n},\n"should_ignore": false,\n"is_applied": false,\n"vector_clock": [\n{\n"provider_id": "provider1",\n"timestamp": "2021-06-17T15:44:00+00:00"\n},\n{\n"provider_id": "provider2",\n"timestamp": "2021-06-15T15:40:00+00:00"\n},\n{\n"provider_id": "provider3",\n"timestamp": "2021-06-14T15:40:00+00:00"\n}\n]\n}\n],\n"query_id": null\n}\n],\n"items": []\n}',
        )
