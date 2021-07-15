import { suite, test } from "@testdeck/mocha";
import * as admin from "firebase-admin";
import {
    FirestoreDataStore,
    ItemChangeMetadataConverter,
    ItemVersionMetadataConverter,
    FirestoreAppItemSerializer,
} from "../../src/backends/firestore";
import { Operation } from "../../src/core/metadata";
import { assert } from "chai";

@suite
class FirestoreDataStoreTest {
    @test
    async testCommitItemChangeInsert() {
        // Setup
        let savedItemChanges: any[] = [];
        let savedItemVersions: any[] = [];
        let savedProviderIds: any[] = [];

        const mockDB = {
            collection: (collectionName: string) => {
                if (collectionName === "maestro__item_versions") {
                    return {
                        doc: (documentId: string) => {
                            return {
                                get: () =>
                                    new Promise((resolve) =>
                                        resolve({
                                            exists: false,
                                        })
                                    ),
                                set: (data: any) =>
                                    new Promise((resolve) => {
                                        savedItemVersions.push(data);
                                        return resolve();
                                    }),
                            };
                        },
                    };
                }
                if (collectionName === "maestro__item_changes") {
                    return {
                        doc: (documentId: string) => {
                            return {
                                set: (data: any) =>
                                    new Promise((resolve) => {
                                        savedItemChanges.push(data);
                                        return resolve();
                                    }),
                            };
                        },
                    };
                }
                if (collectionName === "maestro__provider_ids") {
                    return {
                        doc: (documentId: string) => {
                            return {
                                set: (data: any) =>
                                    new Promise((resolve) => {
                                        savedProviderIds.push({
                                            id: documentId,
                                            ...data,
                                        });
                                        return resolve();
                                    }),
                            };
                        },
                    };
                }
                throw Error("Should't get here! Collection: " + collectionName);
            },
        };

        const itemVersionMetadataConverter = new ItemVersionMetadataConverter();
        const dataStore = new FirestoreDataStore(
            "provider1",
            itemVersionMetadataConverter,
            new ItemChangeMetadataConverter(),
            new FirestoreAppItemSerializer(),
            mockDB as admin.firestore.Firestore
        );
        itemVersionMetadataConverter.dataStore = dataStore;

        const itemId = "7932bf9f-8e45-42da-8e91-43a55ad9dee4";
        const item = {
            id: itemId,
            collectionName: "my_collection",
            hello: "world",
        };

        await dataStore.commitItemChange(Operation.INSERT, itemId, item);

        assert.equal(savedItemChanges.length, 1);
        assert.equal(savedItemVersions.length, 1);

        assert.isTrue(
            savedItemChanges[0].date_created instanceof
                admin.firestore.Timestamp
        );
        assert.equal(savedItemChanges[0].operation, "INSERT");
        assert.equal(savedItemChanges[0].item_id, itemId);
        assert.equal(savedItemChanges[0].collection_name, "my_collection");
        assert.equal(
            savedItemChanges[0].provider_timestamp.toDate().getTime(),
            savedItemChanges[0].date_created.toDate().getTime()
        );
        assert.equal(savedItemChanges[0].provider_id, "provider1");

        assert.equal(
            savedItemChanges[0].insert_provider_timestamp.toDate().getTime(),
            savedItemChanges[0].date_created.toDate().getTime()
        );

        assert.equal(savedItemChanges[0].insert_provider_id, "provider1");
        assert.equal(
            savedItemChanges[0].serialized_item,
            '{"entity_name":"my_collection","pk":"7932bf9f-8e45-42da-8e91-43a55ad9dee4","fields":{"hello":"world"}}'
        );
        assert.isFalse(savedItemChanges[0].should_ignore);
        assert.isTrue(savedItemChanges[0].is_applied);
        assert.equal(savedItemChanges[0].vector_clock.length, 1);
        assert.equal(
            savedItemChanges[0].vector_clock[0].provider_id,
            "provider1"
        );

        assert.equal(savedProviderIds.length, 1);

        assert.equal(savedProviderIds[0].id, "provider1");
        assert.isTrue(
            savedProviderIds[0].timestamp instanceof admin.firestore.Timestamp
        );
    }

    @test
    async testCommitItemChangeUpdate() {
        // Setup
        let savedItemChanges: any[] = [];
        let savedItemVersions: any[] = [];
        let itemVersionDocIds: string[] = [];
        let itemChangeDocIds: string[] = [];
        let savedProviderIds: any[] = [];

        const itemId = "7932bf9f-8e45-42da-8e91-43a55ad9dee4";
        const previousChangeId = "0f403f98-4f7b-441b-b85b-49a2e572d5b7";
        const item = {
            id: itemId,
            collectionName: "my_collection",
            hello: "world",
        };
        const creationDate = admin.firestore.Timestamp.now();

        const mockDB = {
            collection: (collectionName: string) => {
                if (collectionName === "maestro__item_versions") {
                    return {
                        doc: (documentId: string) => {
                            itemVersionDocIds.push(documentId);

                            return {
                                get: () =>
                                    new Promise((resolve) =>
                                        resolve({
                                            exists: true,
                                            id: itemId,
                                            data: () => {
                                                return {
                                                    current_item_change_id:
                                                        previousChangeId,
                                                    date_created: creationDate,
                                                    collection_name:
                                                        "my_collection",
                                                    vector_clock: [
                                                        {
                                                            provider_id:
                                                                "provider2",
                                                            timestamp:
                                                                creationDate,
                                                        },
                                                    ],
                                                };
                                            },
                                        })
                                    ),
                                set: (data: any) =>
                                    new Promise((resolve) => {
                                        savedItemVersions.push(data);
                                        return resolve();
                                    }),
                            };
                        },
                    };
                }
                if (collectionName === "maestro__item_changes") {
                    return {
                        doc: (documentId: string) => {
                            itemChangeDocIds.push(documentId);
                            return {
                                set: (data: any) =>
                                    new Promise((resolve) => {
                                        savedItemChanges.push(data);
                                        return resolve();
                                    }),
                            };
                        },
                    };
                }
                if (collectionName === "maestro__provider_ids") {
                    return {
                        doc: (documentId: string) => {
                            return {
                                set: (data: any) =>
                                    new Promise((resolve) => {
                                        savedProviderIds.push({
                                            id: documentId,
                                            ...data,
                                        });
                                        return resolve();
                                    }),
                            };
                        },
                    };
                }
                throw Error("Should't get here! Collection: " + collectionName);
            },
            getAll: (...refs: []) => {
                return new Promise((resolve) => {
                    resolve([
                        {
                            id: previousChangeId,
                            exists: true,
                            data: () => {
                                return {
                                    operation: "INSERT",
                                    collection_name: "my_collection",
                                    item_id: itemId,
                                    date_created: creationDate,
                                    provider_id: "provider2",
                                    provider_timestamp: creationDate,
                                    insert_provider_id: "provider2",
                                    insert_provider_timestamp: creationDate,
                                    serialized_item:
                                        '{"entity_name":"my_collection","pk":"7932bf9f-8e45-42da-8e91-43a55ad9dee4","fields":{"hello":"world"}}',
                                    should_ignore: false,
                                    is_applied: true,
                                    vector_clock: [
                                        {
                                            provider_id: "provider2",
                                            timestamp: creationDate,
                                        },
                                    ],
                                };
                            },
                        },
                    ]);
                });
            },
        };

        const itemVersionMetadataConverter = new ItemVersionMetadataConverter();
        const dataStore = new FirestoreDataStore(
            "provider1",
            itemVersionMetadataConverter,
            new ItemChangeMetadataConverter(),
            new FirestoreAppItemSerializer(),
            mockDB as admin.firestore.Firestore
        );
        itemVersionMetadataConverter.dataStore = dataStore;

        await dataStore.commitItemChange(Operation.UPDATE, itemId, item);

        assert.equal(savedItemChanges.length, 1);
        assert.equal(savedItemVersions.length, 1);

        assert.isTrue(
            savedItemChanges[0].date_created instanceof
                admin.firestore.Timestamp
        );
        assert.equal(savedItemChanges[0].operation, "UPDATE");
        assert.equal(savedItemChanges[0].item_id, itemId);
        assert.equal(savedItemChanges[0].collection_name, "my_collection");
        assert.equal(
            savedItemChanges[0].provider_timestamp.toDate().getTime(),
            savedItemChanges[0].date_created.toDate().getTime()
        );
        assert.equal(savedItemChanges[0].provider_id, "provider1");

        assert.equal(
            savedItemChanges[0].insert_provider_timestamp.toDate().getTime(),
            creationDate.toDate().getTime()
        );

        assert.equal(savedItemChanges[0].insert_provider_id, "provider2");
        assert.equal(
            savedItemChanges[0].serialized_item,
            '{"entity_name":"my_collection","pk":"7932bf9f-8e45-42da-8e91-43a55ad9dee4","fields":{"hello":"world"}}'
        );
        assert.isFalse(savedItemChanges[0].should_ignore);
        assert.isTrue(savedItemChanges[0].is_applied);

        assert.equal(savedItemChanges[0].vector_clock.length, 2);
        assert.equal(
            savedItemChanges[0].vector_clock[0].provider_id,
            "provider2"
        );
        assert.equal(
            savedItemChanges[0].vector_clock[1].provider_id,
            "provider1"
        );
        assert.equal(itemVersionDocIds.length, 2);
        assert.equal(itemVersionDocIds[0], itemId);
        assert.equal(itemVersionDocIds[1], itemId);
        assert.equal(itemChangeDocIds.length, 2);
        assert.equal(itemChangeDocIds[0], previousChangeId);
        assert.equal(savedProviderIds.length, 1);

        assert.equal(savedProviderIds[0].id, "provider1");
        assert.isTrue(
            savedProviderIds[0].timestamp instanceof admin.firestore.Timestamp
        );
    }
}
