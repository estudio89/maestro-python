import { CollectionType } from "./collections";

export function typeToCollection(collectionType: CollectionType): string {
    return "sync_framework__" + collectionType;
}

export function getCollectionName(serializedItem: string): string {
    const data = JSON.parse(serializedItem);
    const tableName = data.table_name;
    const collection = tableNameToCollection(tableName);
    return collection;
}

export function collectionToTableName(collection: string) {
    return collection;
}

export function tableNameToCollection(tableName: string) {
  return tableName;
}