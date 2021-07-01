import { BaseDataStore } from "../../core/store";
import { ItemVersion, ItemChange } from "../../core/metadata";
import { FirestoreAppItemSerializer } from "./serializer";
import {
    ItemVersionMetadataConverter,
    ItemChangeMetadataConverter,
} from "./converters";
import { BaseMetadataConverter } from "../../core/converter";
import * as admin from "firebase-admin";
import {
    CollectionType,
    AppItem,
    FirestoreItem,
    ItemVersionRecord,
    ItemChangeRecord,
} from "./collections";
import { typeToCollection } from "./utils";

export class FirestoreDataStore extends BaseDataStore<AppItem> {
    constructor(
        protected localProviderId: string,
        protected itemVersionMetadataConverter: ItemVersionMetadataConverter,
        protected itemChangeMetadataConverter: ItemChangeMetadataConverter,
        protected itemSerializer: FirestoreAppItemSerializer,
        private db: admin.firestore.Firestore
    ) {
        super(
            localProviderId,
            itemVersionMetadataConverter as BaseMetadataConverter<
                ItemVersion,
                any
            >,
            itemChangeMetadataConverter as BaseMetadataConverter<
                ItemChange,
                any
            >,
            itemSerializer
        );
    }

    public documentToRawInstance(
        document: admin.firestore.DocumentSnapshot
    ): FirestoreItem {
        return {
            id: document.id,
            ...document.data(),
        };
    }

    private getCollectionQuery(
        collectionType: CollectionType
    ): admin.firestore.CollectionReference {
        const collectionName = typeToCollection(collectionType);
        return this.db.collection(collectionName);
    }

    private async save<T extends FirestoreItem>(
        instance: T,
        collection: string
    ): Promise<void> {
        const documentId = instance.id;
        const ref = this.db.collection(collection).doc(documentId);
        delete instance["id"];
        await ref.set(instance);
    }

    private async saveProviderId(
        providerId: string,
        timestamp: admin.firestore.Timestamp
    ) {
        const collectionName = typeToCollection(CollectionType.PROVIDER_IDS);
        await this.save(
            {
                id: providerId,
                timestamp: timestamp,
            },
            collectionName
        );
    }

    async findItemChanges(ids: string[]): Promise<ItemChange[]> {
        if (ids.length === 0) {
            return [];
        }
        const refs = ids.map((id) =>
            this.getCollectionQuery(CollectionType.ITEM_CHANGES).doc(id)
        );
        const docs = await this.db.getAll(...refs);
        const instances: ItemChangeRecord[] = [];
        for (let doc of docs) {
            if (doc.exists) {
                let instance = this.documentToRawInstance(doc);
                instances.push(instance as ItemChangeRecord);
            }
        }

        const metadataObjects: ItemChange[] = [];
        for (let instance of instances) {
            const metadataObject =
                await this.itemChangeMetadataConverter.toMetadata(instance);
            metadataObjects.push(metadataObject);
        }
        return metadataObjects;
    }

    async getItemVersion(itemId: string): Promise<ItemVersion | undefined> {
        const doc = await this.getCollectionQuery(CollectionType.ITEM_VERSIONS)
            .doc(itemId)
            .get();

        if (doc.exists) {
            const instance = this.documentToRawInstance(
                doc
            ) as ItemVersionRecord;
            const itemVersion =
                await this.itemVersionMetadataConverter.toMetadata(instance);
            return itemVersion;
        } else {
            return undefined;
        }
    }
    async saveItemChange(
        itemChange: ItemChange,
        isCreating: boolean
    ): Promise<void> {
        const collectionName = typeToCollection(CollectionType.ITEM_CHANGES);
        const itemChangeRecord =
            await this.itemChangeMetadataConverter.toRecord(itemChange);
        await this.save(itemChangeRecord, collectionName);

        if (isCreating) {
            this.saveProviderId(
                itemChangeRecord.provider_id,
                itemChangeRecord.provider_timestamp
            );
        }
    }
    async saveItemVersion(itemVersion: ItemVersion): Promise<void> {
        const collectionName = typeToCollection(CollectionType.ITEM_VERSIONS);
        const itemVersionRecord =
            await this.itemVersionMetadataConverter.toRecord(itemVersion);
        await this.save(itemVersionRecord, collectionName);
    }
}
