import { CollectionType } from "./collections";

export function typeToCollection(collectionType: CollectionType): string {
    return "maestro__" + collectionType;
}

export function collectionToEntityName(collection: string) {
    return collection;
}

export function entityNameToCollection(entityName: string) {
    return entityName;
}
