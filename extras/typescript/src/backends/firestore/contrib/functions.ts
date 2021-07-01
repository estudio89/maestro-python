import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import { QueuedOperation, CollectionType, AppItem } from "../collections";
import { typeToCollection } from "../utils";
import { FirestoreDataStore } from "../store";
import {
    ItemVersionMetadataConverter,
    ItemChangeMetadataConverter,
} from "../converters";
import { FirestoreAppItemSerializer } from "../serializer";
import { Operation } from "../../../core/metadata";

export function setupCommitQueue(
    appItemSerializer: FirestoreAppItemSerializer,
    onChangeCommited: () => {}
): functions.CloudFunction<
    functions.Change<functions.firestore.DocumentSnapshot>
> {
    const db = admin.firestore();

    const queueCollectionName = typeToCollection(CollectionType.COMMIT_QUEUE);
    const itemVersionMetadataConverter = new ItemVersionMetadataConverter();
    const dataStore = new FirestoreDataStore(
        "provider1",
        itemVersionMetadataConverter,
        new ItemChangeMetadataConverter(),
        appItemSerializer,
        db
    );
    itemVersionMetadataConverter.dataStore = dataStore;
    const commitChange = functions.firestore
        .document(queueCollectionName + "/{documentId}/")
        .onWrite(async (snapshot, context) => {
            const data: QueuedOperation =
                snapshot.after.data() as QueuedOperation;
            const doc = await db
                .collection(data.collection_name)
                .doc(data.item_id)
                .get();
            if (doc.exists) {
                let item = dataStore.documentToRawInstance(doc) as AppItem;
                item.collectionName = data.collection_name;
                if (!item.collectionName) {
                    throw Error(
                        "A change was committed to the queue without a 'collection_name' field."
                    );
                }

                if (!item.id) {
                    throw Error(
                        "A change was committed to the queue without an 'item_id' field."
                    );
                }

                if (!(<any>Object).values(Operation).includes(data.operation)) {
                    throw Error(
                        `A change was committed to the queue using an invalid operation: ${data.operation}`
                    );
                }

                await dataStore.commitItemChange(
                    data.operation as Operation,
                    data.item_id,
                    item
                );

                await db
                    .collection(queueCollectionName)
                    .doc(context.params.documentId)
                    .delete();

                onChangeCommited();
            }
        });
    return commitChange;
}
