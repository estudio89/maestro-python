import * as admin from "firebase-admin";
import { BaseItemSerializer } from "../../core/serializer";
import { AppItem } from "./collections";
import { collectionToEntityName } from "./utils";
import { SerializationResult } from "../../core/metadata";

/**
 * Serializes an item to the format expected by the Sync Framework
 */
export class FirestoreAppItemSerializer implements BaseItemSerializer<AppItem> {
    serializeField(
        collectionName: string,
        item: { [key: string]: any },
        key: string
    ): any {
        let value = item[key];
        const isTimestamp = value instanceof admin.firestore.Timestamp;
        if (isTimestamp) {
            value = (value as admin.firestore.Timestamp).toDate();
        }
        const isDate = !!value && typeof value.getMonth === "function";

        if (isDate) {
            value = value.toISOString();
        }

        return value;
    }
    /**
     * Converts item to a string.
     */
    serializeItem(item: AppItem): SerializationResult {
        const pk = item.id;
        const collectionName = item.collectionName;
        const fields: { [key: string]: any } = {};
        for (let key in item) {
            if (["id", "collectionName"].indexOf(key) !== -1) {
                continue;
            }

            const value = this.serializeField(collectionName, item, key);
            fields[key] = value;
        }
        const entityName = collectionToEntityName(collectionName);
        const serializedItem = JSON.stringify(fields);

        return new SerializationResult(pk, entityName, serializedItem);
    }
}
