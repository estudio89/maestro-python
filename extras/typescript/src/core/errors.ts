export class ItemNotFoundError extends Error {
    constructor(itemType: string, id: string) {
        super(`Item of type ${itemType} and ID '${itemType}' not found.`);
    }
}