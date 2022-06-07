
export enum CollectionType {
    ITEM_CHANGES = "item_changes",
    ITEM_VERSIONS = "item_versions",
    PROVIDER_IDS = "provider_ids",
    COMMIT_QUEUE = "commit_queue",
}

export interface FirestoreItem {
    id: string;
}

export interface AppItem extends FirestoreItem {
    collectionName: string;
}

export interface VectorClockItemRecord {
    provider_id: string;
    timestamp: Date;
}

export interface ItemChangeRecord extends FirestoreItem {
    operation: string;
    collection_name: string;
    item_id: string;
    date_created: Date;
    change_vector_clock_item: VectorClockItemRecord;
    insert_vector_clock_item: VectorClockItemRecord;
    serialized_item: AppItem;
    should_ignore: boolean;
    is_applied: boolean;
    vector_clock: VectorClockItemRecord[];
}

export interface ItemVersionRecord extends FirestoreItem {
    date_created: Date;
    current_item_change_id: string;
    collection_name: string;
    vector_clock: VectorClockItemRecord[];
}

export enum QueuedOperationStatus {
    PENDING = "pending",
    ERROR = "error",
    DONE = "done",
}

export interface QueuedOperation {
    id: string;
    item_id: string;
    collection_name: string;
    operation: string;
    data: any;
    timestamp: Date | undefined;
    status: QueuedOperationStatus
}
