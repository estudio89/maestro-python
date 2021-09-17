import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {
    QueuedOperation,
    CollectionType,
    QueuedOperationStatus,
} from "../collections";
import { typeToCollection, collectionToEntityName } from "../utils";
import { FirestoreDataStore } from "../store";
import {
    ItemVersionMetadataConverter,
    ItemChangeMetadataConverter,
} from "../converters";
import { FirestoreAppItemSerializer } from "../serializer";
import { Operation } from "../../../core/metadata";

function queuedOperationFromDoc(
    doc:
        | FirebaseFirestore.QueryDocumentSnapshot<FirebaseFirestore.DocumentData>
        | FirebaseFirestore.DocumentSnapshot<FirebaseFirestore.DocumentData>
): QueuedOperation {
    const data = doc.data()!;
    const queuedOperation: QueuedOperation = {
        id: doc.id,
        collection_name: data.collection_name,
        item_id: data.item_id,
        operation: data.operation,
        data: data.data,
        timestamp: data.timestamp,
        status: data.status,
    };
    return queuedOperation;
}
async function processCommit(
    dataStore: FirestoreDataStore,
    db: admin.firestore.Firestore,
    queuedOperation: QueuedOperation
) {
    console.log(
        "Processing commit operation",
        queuedOperation.id,
        queuedOperation.operation,
        queuedOperation.item_id,
        queuedOperation.timestamp?.toDate()
    );
    let item = queuedOperation.data;
    item.id = queuedOperation.item_id;
    if (!queuedOperation.collection_name) {
        throw Error(
            "A change was committed to the queue without a 'collection_name' field."
        );
    }

    if (!item.id) {
        throw Error(
            "A change was committed to the queue without an 'item_id' field."
        );
    }

    if (!(<any>Object).values(Operation).includes(queuedOperation.operation)) {
        throw Error(
            `A change was committed to the queue using an invalid operation: ${queuedOperation.operation}`
        );
    }
    const entityName = collectionToEntityName(queuedOperation.collection_name);
    await dataStore.commitItemChange(
        queuedOperation.operation as Operation,
        entityName,
        queuedOperation.item_id,
        item,
        queuedOperation.timestamp?.toDate(),
        queuedOperation.id
    );
}

export function setupCommitQueue(
    appItemSerializer: FirestoreAppItemSerializer,
    onChangesCommited: () => Promise<any>,
    onError: (queuedOperation: QueuedOperation, error: any) => Promise<any>,
    providerId: string = "firestore",
    db: admin.firestore.Firestore
): functions.CloudFunction<functions.firestore.QueryDocumentSnapshot> {
    const queueCollectionName = typeToCollection(CollectionType.COMMIT_QUEUE);

    const commitChange = functions.firestore
        .document(queueCollectionName + "/{documentId}")
        .onCreate(async (snapshot, context) => {
            const itemVersionMetadataConverter =
                new ItemVersionMetadataConverter();
            const dataStore = new FirestoreDataStore(
                providerId,
                itemVersionMetadataConverter,
                new ItemChangeMetadataConverter(appItemSerializer),
                appItemSerializer,
                db
            );
            itemVersionMetadataConverter.dataStore = dataStore;

            const querySnapshot = await db
                .collection(queueCollectionName)
                .where("status", "==", QueuedOperationStatus.PENDING)
                .orderBy("timestamp")
                .get();
            const docs = querySnapshot.docs;
            for (let doc of docs) {
                const queuedOperation: QueuedOperation =
                    queuedOperationFromDoc(doc);

                try {
                    await db.runTransaction(
                        async (transaction: FirebaseFirestore.Transaction) => {
                            try {
                                dataStore.transaction = transaction;
                                const docRef = db
                                    .collection(queueCollectionName)
                                    .doc(queuedOperation.id);
                                const refreshedDoc = await transaction.get(docRef);
                                const refreshedQueuedOperation =
                                    queuedOperationFromDoc(refreshedDoc);
                                if (
                                    refreshedQueuedOperation.status !==
                                    QueuedOperationStatus.PENDING
                                ) {
                                    console.log(
                                        `Skipping change with id ${queuedOperation.id}. Status = ${refreshedQueuedOperation.status}`
                                    );
                                    return;
                                }
                                await processCommit(dataStore, db, queuedOperation);
                                await transaction.update(docRef, {
                                    status: QueuedOperationStatus.DONE,
                                });
                            } finally {
                                dataStore.transaction = undefined;
                            }
                        }
                    );

                    console.log(
                        `Change with id ${queuedOperation.id} was committed.`
                    );
                } catch (error) {
                    await db
                        .collection(queueCollectionName)
                        .doc(doc.id)
                        .update({ status: QueuedOperationStatus.ERROR });

                    console.error(`Error commiting change with id ${doc.id}.`);
                    await onError(queuedOperation, error);
                }
            }

            await onChangesCommited();
            return true;
        });
    return commitChange;
}
