export interface BaseItemSerializer<T> {
    serializeItem(item: T): string;
}