import * as admin from "firebase-admin";
import { BaseItemSerializer } from "../../core/serializer";
import { AppItem } from "./collections";
import { SerializationResult } from "../../core/metadata";
import { parseDate } from "../../core";
import { entityNameToCollection } from "./utils";

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
    serializeItem(item: AppItem, entityName: string): SerializationResult {
        const pk = item.id;
        const collectionName = item.collectionName;
        const fields: { [key: string]: any } = {};
        for (let key in item) {
            if (["collectionName"].indexOf(key) !== -1) {
                continue;
            }

            const value = this.serializeField(collectionName, item, key);
            fields[key] = value;
        }
        const serializedItem = JSON.stringify(fields);

        return new SerializationResult(pk, entityName, serializedItem);
    }

    deserializeField(
        collectionName: string,
        fields: { [key: string]: any },
        key: string
    ): any {
        const value = fields[key];
        if (!value) {
            return value;
        }

        try {
            const date = parseDate(value);
            return admin.firestore.Timestamp.fromDate(date);
        } catch (e) {}

        return value;
    }

    deserializeItem(serializationResult: SerializationResult): AppItem {
        const collectionName = entityNameToCollection(
            serializationResult.entityName
        );

        const id = serializationResult.itemId;
        const fields = JSON.parse(serializationResult.serializedItem);
        let item: { [key: string]: any } = {
            id: id,
            collectionName: collectionName,
        };
        for (let key in fields) {
            const value = this.deserializeField(collectionName, fields, key);
            item[key] = value;
        }
        return item as AppItem;
    }
}
