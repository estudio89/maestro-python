export class VectorClockItem {
  constructor(public providerId: string, public timestamp: Date) {}
  clone(): VectorClockItem {
    return new VectorClockItem(this.providerId, this.timestamp);
  }
  equals(other: VectorClockItem) {
    return (
      this.providerId == other.providerId &&
      this.timestamp.getTime() == other.timestamp.getTime()
    );
  }
  /* istanbul ignore next */
  toString(): string {
    return `VectorClockItem(providerId: ${this.providerId}, timestamp: ${this.timestamp})`;
  }
}

export class VectorClock {
  private vectorClockItemsById: { [key: string]: VectorClockItem };

  constructor(...vectorClockItems: VectorClockItem[]) {
    this.vectorClockItemsById = {};
    for (let vectorClockItem of vectorClockItems) {
      this.vectorClockItemsById[vectorClockItem.providerId] = vectorClockItem;
    }
  }

  [Symbol.iterator]() {
    let pointer = 0;
    let keys = Object.keys(this.vectorClockItemsById);
    let self = this;
    return {
      next(): IteratorResult<VectorClockItem> {
        if (pointer < keys.length) {
          return {
            done: false,
            value: self.vectorClockItemsById[keys[pointer++]],
          };
        } else {
          return {
            done: true,
            value: null,
          };
        }
      },
    };
  }

  /* istanbul ignore next */
  toString(): string {
    return (
      "VectorClock(" + Object.values(this.vectorClockItemsById).join(", ") + ")"
    );
  }

  getVectorClockItem(providerId: string): VectorClockItem {
    let vectorClockItem = this.vectorClockItemsById[providerId];
    if (!vectorClockItem) {
      vectorClockItem = new VectorClockItem(providerId, new Date(0));
      this.vectorClockItemsById[providerId] = vectorClockItem;
    }
    return vectorClockItem;
  }

  updateVectorClockItem(providerId: string, timestamp: Date): VectorClockItem {
    let vectorClockItem = this.getVectorClockItem(providerId);
    if (vectorClockItem.timestamp < timestamp) {
      vectorClockItem.timestamp = timestamp;
    }

    return vectorClockItem;
  }

  static createEmpty(providerIds: string[]): VectorClock {
    let vectorClock = new VectorClock();

    for (let providerId of providerIds) {
      vectorClock.getVectorClockItem(providerId);
    }

    return vectorClock;
  }

  clone(): VectorClock {
    let clonedVectorClockItems: VectorClockItem[] = [];
    for (let item of this) {
      clonedVectorClockItems.push(item.clone());
    }
    return new VectorClock(...clonedVectorClockItems);
  }

  equals(other: VectorClock): boolean {
    for (let vectorClockItem of this) {
      const otherVectorItem = other.getVectorClockItem(
        vectorClockItem.providerId
      );
      if (!otherVectorItem.equals(vectorClockItem)) {
        return false;
      }
    }

    for (let otherVectorItem of other) {
      const vectorClockItem = this.getVectorClockItem(
        otherVectorItem.providerId
      );
      if (!vectorClockItem.equals(otherVectorItem)) {
        return false;
      }
    }
    return true;
  }
}

export enum Operation {
  INSERT = "INSERT",
  UPDATE = "UPDATE",
  DELETE = "DELETE",
}

export class ItemChange {
  constructor(
    public id: string,
    public operation: Operation,
    public itemId: string,
    public providerTimestamp: Date,
    public providerId: string,
    public insertProviderId: string,
    public insertProviderTimestamp: Date,
    public serializedItem: string,
    public shouldIgnore: boolean,
    public isApplied: boolean,
    public vectorClock: VectorClock,
    public dateCreated: Date
  ) {}
  clone(): ItemChange {
    return new ItemChange(
      this.id,
      this.operation,
      this.itemId,
      this.providerTimestamp,
      this.providerId,
      this.insertProviderId,
      this.insertProviderTimestamp,
      this.serializedItem,
      this.shouldIgnore,
      this.isApplied,
      this.vectorClock.clone(),
      this.dateCreated
    );
  }
  equals(other: ItemChange): boolean {
    return (
      this.id === other.id &&
      this.operation === other.operation &&
      this.itemId === other.itemId &&
      this.providerTimestamp.getTime() === other.providerTimestamp.getTime() &&
      this.providerId === other.providerId &&
      this.insertProviderId === other.insertProviderId &&
      this.insertProviderTimestamp.getTime() ===
        other.insertProviderTimestamp.getTime() &&
      this.serializedItem === other.serializedItem &&
      this.shouldIgnore === other.shouldIgnore &&
      this.isApplied === other.isApplied &&
      this.vectorClock.equals(other.vectorClock) &&
      this.dateCreated.getTime() === other.dateCreated.getTime()
    );
  }
}

export class ItemVersion {
  vectorClock: VectorClock;

  constructor(
    public itemId: string,
    public dateCreated: Date,
    vectorClock?: VectorClock,
    public currentItemChange?: ItemChange
  ) {
    if (!vectorClock && !currentItemChange) {
      throw new Error(
        "Either 'vectorClock' or 'currentItemChange' must be provided."
      );
    }
    if (!!vectorClock && !!currentItemChange && !vectorClock.equals(currentItemChange.vectorClock)) {
      throw new Error(
        "The given VectorClock is different from the ItemChange's vector clock."
      );
    }

    if (!!currentItemChange && !vectorClock) {
      this.vectorClock = currentItemChange.vectorClock;
    } else {
      this.vectorClock = vectorClock as VectorClock;
    }
  }
  clone(): ItemVersion {
    return new ItemVersion(
      this.itemId,
      this.dateCreated,
      this.vectorClock.clone(),
      this.currentItemChange?.clone()
    );
  }
  equals(other: ItemVersion): boolean {
    let equalItemChange;
    if (!!this.currentItemChange) {
      if (!other.currentItemChange) {
        equalItemChange = false;
      } else {
        equalItemChange = this.currentItemChange.equals(
          other.currentItemChange
        );
      }
    } else {
      equalItemChange = !other.currentItemChange;
    }

    return (
      this.itemId === other.itemId &&
      this.dateCreated.getTime() === other.dateCreated.getTime() &&
      this.vectorClock.equals(other.vectorClock) &&
      equalItemChange
    );
  }
}
