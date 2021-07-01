import { BaseMetadataConverter } from "../../core/converter";
import {
    ItemChange,
    ItemVersion,
    VectorClock,
    VectorClockItem,
    Operation,
} from "../../core/metadata";
import {
    ItemChangeRecord,
    ItemVersionRecord,
    VectorClockItemRecord,
} from "./collections";
import { FirestoreDataStore } from "./store";
import { getCollectionName } from "./utils";
import * as admin from "firebase-admin";

abstract class FirestoreConverter<M, R> implements BaseMetadataConverter<M, R> {
    abstract toMetadata(record: R): Promise<M>;
    abstract toRecord(metadataObject: M): Promise<R>;

    private _dataStore?: FirestoreDataStore;
    set dataStore(dataStore: FirestoreDataStore) {
        this._dataStore = dataStore;
    }
    get dataStore(): FirestoreDataStore {
        if (!this._dataStore) {
            throw new Error("DataStore not set!");
        }
        return this._dataStore;
    }
}

export class VectorClockMetadataConverter
    implements BaseMetadataConverter<VectorClock, VectorClockItemRecord[]>
{
    async toMetadata(record: VectorClockItemRecord[]): Promise<VectorClock> {
        const vectorClockItems: VectorClockItem[] = [];
        for (let item of record) {
            vectorClockItems.push(
                new VectorClockItem(item.provider_id, item.timestamp.toDate())
            );
        }
        const vectorClock = new VectorClock(...vectorClockItems);
        return vectorClock;
    }

    async toRecord(
        metadataObject: VectorClock
    ): Promise<VectorClockItemRecord[]> {
        const items: VectorClockItemRecord[] = [];
        for (let vectorClockItem of metadataObject) {
            items.push({
                provider_id: vectorClockItem.providerId,
                timestamp: admin.firestore.Timestamp.fromDate(
                    vectorClockItem.timestamp
                ),
            });
        }
        return items;
    }
}
export class ItemChangeMetadataConverter
    implements BaseMetadataConverter<ItemChange, ItemChangeRecord>
{
    async toMetadata(record: ItemChangeRecord): Promise<ItemChange> {
        const vectorClockConverter = new VectorClockMetadataConverter();
        const vectorClock = await vectorClockConverter.toMetadata(
            record.vector_clock
        );
        const metadataObject = new ItemChange(
            record.id,
            record.operation as Operation,
            record.item_id,
            record.provider_timestamp.toDate(),
            record.provider_id,
            record.insert_provider_id,
            record.insert_provider_timestamp.toDate(),
            record.serialized_item,
            record.should_ignore,
            record.is_applied,
            vectorClock,
            record.date_created.toDate()
        );
        return metadataObject;
    }

    async toRecord(metadataObject: ItemChange): Promise<ItemChangeRecord> {
        const vectorClockConverter = new VectorClockMetadataConverter();
        const vectorClockRecord = await vectorClockConverter.toRecord(
            metadataObject.vectorClock
        );
        const collectionName = getCollectionName(metadataObject.serializedItem);
        return {
            id: metadataObject.id,
            date_created: admin.firestore.Timestamp.fromDate(
                metadataObject.dateCreated
            ),
            operation: metadataObject.operation,
            item_id: metadataObject.itemId,
            collection_name: collectionName,
            provider_timestamp: admin.firestore.Timestamp.fromDate(
                metadataObject.providerTimestamp
            ),
            provider_id: metadataObject.providerId,
            insert_provider_timestamp: admin.firestore.Timestamp.fromDate(
                metadataObject.insertProviderTimestamp
            ),
            insert_provider_id: metadataObject.insertProviderId,
            serialized_item: metadataObject.serializedItem,
            should_ignore: metadataObject.shouldIgnore,
            is_applied: metadataObject.isApplied,
            vector_clock: vectorClockRecord,
        };
    }
}

export class ItemVersionMetadataConverter extends FirestoreConverter<
    ItemVersion,
    ItemVersionRecord
> {
    async toMetadata(record: ItemVersionRecord): Promise<ItemVersion> {
        const itemChanges = await this.dataStore.findItemChanges([
            record.current_item_change_id,
        ]);
        const itemChange = itemChanges[0];
        const vectorClockConverter = new VectorClockMetadataConverter();
        const vectorClock = await vectorClockConverter.toMetadata(
            record.vector_clock
        );
        return new ItemVersion(
            record.id,
            record.date_created.toDate(),
            vectorClock,
            itemChange
        );
    }
    async toRecord(metadataObject: ItemVersion): Promise<ItemVersionRecord> {
        const vectorClockConverter = new VectorClockMetadataConverter();
        const vectorClockRecord = await vectorClockConverter.toRecord(
            metadataObject.vectorClock
        );
        const currentItemChange =
            metadataObject.currentItemChange as ItemChange;
        const collectionName = getCollectionName(
            currentItemChange.serializedItem
        );

        return {
            id: metadataObject.itemId,
            date_created: admin.firestore.Timestamp.fromDate(
                metadataObject.dateCreated
            ),
            current_item_change_id: currentItemChange.id,
            vector_clock: vectorClockRecord,
            collection_name: collectionName,
        };
    }
}
