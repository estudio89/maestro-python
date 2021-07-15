import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import { QueuedOperation, CollectionType } from "../collections";
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
    onChangeCommited: () => void,
    providerId: string = "firestore"
): functions.CloudFunction<functions.firestore.QueryDocumentSnapshot> {

    const queueCollectionName = typeToCollection(CollectionType.COMMIT_QUEUE);

    const commitChange = functions.firestore
        .document(queueCollectionName + "/{documentId}")
        .onCreate(async (snapshot, context) => {
            const db = admin.firestore();
            const itemVersionMetadataConverter =
                new ItemVersionMetadataConverter();
            const dataStore = new FirestoreDataStore(
                providerId,
                itemVersionMetadataConverter,
                new ItemChangeMetadataConverter(),
                appItemSerializer,
                db
            );
            itemVersionMetadataConverter.dataStore = dataStore;
            const queuedOperation: QueuedOperation = snapshot.data() as QueuedOperation;

            console.log(
                "Processing commit operation",
                queuedOperation.operation,
                queuedOperation.item_id
            );
            let item = queuedOperation.data;
            item.id = queuedOperation.item_id;
            item.collectionName = queuedOperation.collection_name;
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

            if (
                !(<any>Object)
                    .values(Operation)
                    .includes(queuedOperation.operation)
            ) {
                throw Error(
                    `A change was committed to the queue using an invalid operation: ${queuedOperation.operation}`
                );
            }

            await dataStore.commitItemChange(
                queuedOperation.operation as Operation,
                queuedOperation.item_id,
                item
            );

            await db
                .collection(queueCollectionName)
                .doc(context.params.documentId)
                .delete();
            console.log("Change was committed");
            await onChangeCommited();
            return true;
        });
    return commitChange;
}
