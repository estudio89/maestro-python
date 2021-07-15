import { CollectionType } from "./collections";

export function typeToCollection(collectionType: CollectionType): string {
    return "maestro__" + collectionType;
}

export function getCollectionName(serializedItem: string): string {
    const data = JSON.parse(serializedItem);
    const entityName = data.entity_name;
    const collection = entityNameToCollection(entityName);
    return collection;
}

export function collectionToEntityName(collection: string) {
    return collection;
}

export function entityNameToCollection(entityName: string) {
  return entityName;
}