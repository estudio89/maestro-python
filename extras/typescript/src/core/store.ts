import {
    Operation,
    ItemVersion,
    VectorClock,
    ItemChange,
    VectorClockItem,
    SerializationResult,
} from "./metadata";
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

    async commitItemChange(
        operation: Operation,
        entityName: string,
        itemId: string,
        item: T
    ) {
        let oldVersion = await this.getLocalVersion(itemId);
        let localVectorClock = oldVersion.vectorClock.clone();
        let nowUTC = getNowUTC();
        let changeVectorClockItem = new VectorClockItem(
            this.localProviderId,
            nowUTC
        );

        localVectorClock.updateVectorClockItem(this.localProviderId, nowUTC);

        let itemChange = new ItemChange(
            uuid(),
            operation,
            this.serializeItem(item, entityName),
            changeVectorClockItem,
            oldVersion.currentItemChange?.insertVectorClockItem ??
                changeVectorClockItem,
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

    serializeItem(item: T, entityName: string): SerializationResult {
        return this.itemSerializer.serializeItem(item, entityName);
    }
    abstract async getItemVersion(
        itemId: string
    ): Promise<ItemVersion | undefined>;
    abstract async saveItemChange(
        itemChange: ItemChange,
        isCreating: boolean
    ): Promise<void>;
    abstract async saveItemVersion(itemVersion: ItemVersion): Promise<void>;
}
