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
import { entityNameToCollection, collectionToEntityName } from "./utils";
import { FirestoreAppItemSerializer } from "./serializer";

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

export class VectorClockItemMetadataConverter
	implements BaseMetadataConverter<VectorClockItem, VectorClockItemRecord>
{
	toMetadata(record: VectorClockItemRecord): Promise<VectorClockItem> {
		return new Promise((resolve, _) =>
			resolve(new VectorClockItem(record.provider_id, record.timestamp))
		);
	}
	toRecord(metadataObject: VectorClockItem): Promise<VectorClockItemRecord> {
		return new Promise((resolve, _) =>
			resolve({
				provider_id: metadataObject.providerId,
				timestamp: metadataObject.timestamp,
			})
		);
	}
}

export class VectorClockMetadataConverter
	implements BaseMetadataConverter<VectorClock, VectorClockItemRecord[]>
{
	async toMetadata(record: VectorClockItemRecord[]): Promise<VectorClock> {
		const vectorClockItemConverter = new VectorClockItemMetadataConverter();
		const vectorClockItems: VectorClockItem[] = [];
		for (let item of record) {
			vectorClockItems.push(
				await vectorClockItemConverter.toMetadata(item)
			);
		}
		const vectorClock = new VectorClock(...vectorClockItems);
		return vectorClock;
	}

	async toRecord(
		metadataObject: VectorClock
	): Promise<VectorClockItemRecord[]> {
		const vectorClockItemConverter = new VectorClockItemMetadataConverter();
		const items: VectorClockItemRecord[] = [];
		for (let vectorClockItem of metadataObject) {
			items.push(
				await vectorClockItemConverter.toRecord(vectorClockItem)
			);
		}
		return items;
	}
}
export class ItemChangeMetadataConverter
	implements BaseMetadataConverter<ItemChange, ItemChangeRecord>
{
	constructor(public itemSerializer: FirestoreAppItemSerializer) {}
	async toMetadata(record: ItemChangeRecord): Promise<ItemChange> {
		const vectorClockConverter = new VectorClockMetadataConverter();
		const vectorClockItemConverter = new VectorClockItemMetadataConverter();
		const vectorClock = await vectorClockConverter.toMetadata(
			record.vector_clock
		);
		const changeVectorClockItem = await vectorClockItemConverter.toMetadata(
			record.change_vector_clock_item
		);
		const insertVectorClockItem = await vectorClockItemConverter.toMetadata(
			record.insert_vector_clock_item
		);
		const entityName = collectionToEntityName(record.collection_name);
		const serializationResult = this.itemSerializer.serializeItem(
			record.serialized_item,
			entityName
		);
		const metadataObject = new ItemChange(
			record.id,
			record.operation as Operation,
			serializationResult,
			changeVectorClockItem,
			insertVectorClockItem,
			record.should_ignore,
			record.is_applied,
			vectorClock,
			record.date_created
		);
		return metadataObject;
	}

	async toRecord(metadataObject: ItemChange): Promise<ItemChangeRecord> {
		const vectorClockConverter = new VectorClockMetadataConverter();
		const vectorClockRecord = await vectorClockConverter.toRecord(
			metadataObject.vectorClock
		);
		const collectionName = entityNameToCollection(
			metadataObject.serializationResult.entityName
		);
		const deserializedItem = this.itemSerializer.deserializeItem(
			metadataObject.serializationResult
		);
		return {
			id: metadataObject.id,
			date_created: metadataObject.dateCreated,
			operation: metadataObject.operation,
			item_id: metadataObject.serializationResult.itemId,
			collection_name: collectionName,
			change_vector_clock_item: {
				provider_id: metadataObject.changeVectorClockItem.providerId,
				timestamp: metadataObject.changeVectorClockItem.timestamp,
			},
			insert_vector_clock_item: {
				provider_id: metadataObject.insertVectorClockItem.providerId,
				timestamp: metadataObject.insertVectorClockItem.timestamp,
			},
			serialized_item: deserializedItem,
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
			record.date_created,
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
		const collectionName = entityNameToCollection(
			currentItemChange.serializationResult.entityName
		);

		return {
			id: metadataObject.itemId,
			date_created: metadataObject.dateCreated,
			current_item_change_id: currentItemChange.id,
			vector_clock: vectorClockRecord,
			collection_name: collectionName,
		};
	}
}
