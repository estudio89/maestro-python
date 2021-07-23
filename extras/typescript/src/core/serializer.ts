import { SerializationResult } from "./metadata";

export interface BaseItemSerializer<T> {
    serializeItem(item: T, entityName: string): SerializationResult;
}
