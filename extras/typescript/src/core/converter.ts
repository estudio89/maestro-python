export interface BaseMetadataConverter<M, R> {
    toMetadata(record: R): Promise<M>;
    toRecord(metadataObject: M): Promise<R>;
}