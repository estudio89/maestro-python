import { Operation, ItemVersion, VectorClock, ItemChange } from "./metadata";
import { ItemNotFoundError } from "./errors";
import { getNowUTC } from "./utils";
import { v4 as uuid } from "uuid";
import { BaseMetadataConverter } from "./converter";
import { BaseItemSerializer } from "./serializer";

export abstract class BaseDataStore<T> {
    constructor(
        protected localProviderId: string,
        protected itemVersionMetadataConverter: BaseMetadataConverter<
            ItemVersion,
            any
        >,
        protected itemChangeMetadataConverter: BaseMetadataConverter<
            ItemChange,
            any
        >,
        protected itemSerializer: BaseItemSerializer<T>
    ) {}

    async commitItemChange(operation: Operation, itemId: string, item: T) {
        let oldVersion = await this.getLocalVersion(itemId);
        let localVectorClock = oldVersion.vectorClock.clone();
        let nowUTC = getNowUTC();

        localVectorClock.updateVectorClockItem(this.localProviderId, nowUTC);

        let itemChange = new ItemChange(
            uuid(),
            operation,
            itemId,
            nowUTC,
            this.localProviderId,
            oldVersion.currentItemChange?.insertProviderId ??
                this.localProviderId,
            oldVersion.currentItemChange?.insertProviderTimestamp ?? nowUTC,
            this.serializeItem(item),
            false,
            true,
            localVectorClock,
            nowUTC
        );

        await this.saveItemChange(itemChange, true);
        const newVersion = new ItemVersion(
            itemId,
            oldVersion.dateCreated,
            undefined,
            itemChange
        );
        await this.saveItemVersion(newVersion);
    }

    async getLocalVersion(itemId: string): Promise<ItemVersion> {
        let localVersion;

        try {
            localVersion = await this.getItemVersion(itemId);
        } catch (error) {
            if (error instanceof ItemNotFoundError) {
                localVersion = undefined;
            } else {
                throw error;
            }
        }

        if (!localVersion) {
            let vectorClock = VectorClock.createEmpty([this.localProviderId]);
            const nowUTC = getNowUTC();
            localVersion = new ItemVersion(
                itemId,
                nowUTC,
                vectorClock,
                undefined
            );
        }

        return localVersion.clone();
    }

    serializeItem(item: T): string {
        return this.itemSerializer.serializeItem(item);
    }
    abstract async getItemVersion(
        itemId: string
    ): Promise<ItemVersion | undefined>;
    abstract async saveItemChange(itemChange: ItemChange, isCreating: boolean): Promise<void>;
    abstract async saveItemVersion(itemVersion: ItemVersion): Promise<void>;
}
