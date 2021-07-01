import {
    VectorClockItem,
    VectorClock,
    ItemChange,
    Operation,
    ItemVersion,
} from "../src/core/metadata";
import { assert } from "chai";
import { suite, test } from "@testdeck/mocha";
import { v4 as uuid } from "uuid";

@suite
class VectorClockTest {
    private timestamp1: Date = new Date(2021, 6, 27);
    private timestamp2: Date = new Date(2021, 6, 29);
    private timestamp3: Date = new Date(2021, 6, 30);

    private vectorClockItem1: VectorClockItem = new VectorClockItem(
        "provider1",
        this.timestamp1
    );
    private vectorClockItem2: VectorClockItem = new VectorClockItem(
        "provider2",
        this.timestamp2
    );
    private vectorClockItem3: VectorClockItem = new VectorClockItem(
        "provider3",
        this.timestamp3
    );
    private vectorClock: VectorClock = new VectorClock(
        this.vectorClockItem1,
        this.vectorClockItem2,
        this.vectorClockItem3
    );

    @test
    testEquality() {
        let vectorClock = VectorClock.createEmpty(["provider1"]);
        assert.isFalse(this.vectorClock.equals(vectorClock));
        assert.isFalse(vectorClock.equals(this.vectorClock));
        assert.equal(vectorClock.getVectorClockItem("provider1").timestamp.getTime(), new Date(0).getTime());
    }

    @test
    testIteration() {
        let values = [];
        for (let item of this.vectorClock) {
            values.push(item);
        }
        assert.sameOrderedMembers(values, [
            this.vectorClockItem1,
            this.vectorClockItem2,
            this.vectorClockItem3,
        ]);
    }

    @test
    testRetrieval() {
        let item = this.vectorClock.getVectorClockItem("provider1");
        assert.equal(item.providerId, "provider1");
        assert.equal(item.timestamp, this.timestamp1);

        item = this.vectorClock.getVectorClockItem("provider2");
        assert.equal(item.providerId, "provider2");
        assert.equal(item.timestamp, this.timestamp2);

        item = this.vectorClock.getVectorClockItem("provider3");
        assert.equal(item.providerId, "provider3");
        assert.equal(item.timestamp, this.timestamp3);

        item = this.vectorClock.getVectorClockItem("test");
        assert.equal(item.providerId, "test");
        assert.equal(item.timestamp.getTime(), new Date(0).getTime());
    }

    @test
    testUpdate() {
        const newTimestamp = new Date(2021, 6, 28);
        this.vectorClock.updateVectorClockItem("provider1", newTimestamp);
        assert.equal(
            this.vectorClock
                .getVectorClockItem("provider1")
                .timestamp.getTime(),
            newTimestamp.getTime()
        );
        assert.equal(
            this.vectorClock
                .getVectorClockItem("provider2")
                .timestamp.getTime(),
            this.timestamp2.getTime()
        );
        assert.equal(
            this.vectorClock
                .getVectorClockItem("provider3")
                .timestamp.getTime(),
            this.timestamp3.getTime()
        );

        this.vectorClock.updateVectorClockItem("provider2", newTimestamp);
        assert.equal(
            this.vectorClock
                .getVectorClockItem("provider1")
                .timestamp.getTime(),
            newTimestamp.getTime()
        );
        assert.equal(
            this.vectorClock
                .getVectorClockItem("provider2")
                .timestamp.getTime(),
            this.timestamp2.getTime()
        );
        assert.equal(
            this.vectorClock
                .getVectorClockItem("provider3")
                .timestamp.getTime(),
            this.timestamp3.getTime()
        );
    }

    @test
    testCreateEmpty() {
        const vectorClock = VectorClock.createEmpty(["provider"]);
        assert.equal(
            vectorClock.getVectorClockItem("provider").timestamp.getTime(),
            new Date(0).getTime()
        );
    }

    @test
    testClone() {
        const vectorClock = this.vectorClock.clone();
        assert.isTrue(
            vectorClock
                .getVectorClockItem("provider1")
                .equals(this.vectorClock.getVectorClockItem("provider1"))
        );
        assert.isTrue(
            vectorClock
                .getVectorClockItem("provider2")
                .equals(this.vectorClock.getVectorClockItem("provider2"))
        );
        assert.isTrue(
            vectorClock
                .getVectorClockItem("provider3")
                .equals(this.vectorClock.getVectorClockItem("provider3"))
        );
        assert.isTrue(vectorClock.equals(this.vectorClock));
    }
}

@suite
class ItemChangeTest {
    @test
    testClone() {
        const vectorClock = new VectorClock(
            new VectorClockItem("provider2", new Date(2021, 6, 27)),
            new VectorClockItem("provider1", new Date(2021, 6, 28))
        );

        const date = new Date();
        const itemChange1 = new ItemChange(
            uuid(),
            Operation.INSERT,
            "123",
            date,
            "provider1",
            "provider2",
            date,
            '{"table_name":"my_table", "pk":"123", "fields":{"hello":"world"}}',
            false,
            false,
            vectorClock,
            date
        );

        const cloned = itemChange1.clone();
        assert.isTrue(itemChange1.equals(cloned));
        assert.isTrue(cloned.equals(itemChange1));
    }
}

@suite
class ItemVersionTest {
    @test
    testConstructor() {
        const vectorClock1 = new VectorClock(
            new VectorClockItem("provider2", new Date(2021, 6, 27)),
            new VectorClockItem("provider1", new Date(2021, 6, 28))
        );

        const vectorClock2 = new VectorClock(
            new VectorClockItem("provider3", new Date(2021, 6, 27)),
            new VectorClockItem("provider1", new Date(2021, 6, 28))
        );

        const date = new Date();

        const itemChange1 = new ItemChange(
            uuid(),
            Operation.INSERT,
            "123",
            date,
            "provider1",
            "provider2",
            date,
            '{"table_name":"my_table", "pk":"123", "fields":{"hello":"world"}}',
            false,
            false,
            vectorClock1,
            date
        );

        assert.throws(() => {
            new ItemVersion(uuid(), date);
        });

        assert.throws(() => {
            new ItemVersion(uuid(), date, vectorClock2, itemChange1);
        });
    }
    @test
    testClone() {
        const vectorClock = new VectorClock(
            new VectorClockItem("provider2", new Date(2021, 6, 27)),
            new VectorClockItem("provider1", new Date(2021, 6, 28))
        );

        const date = new Date();
        const itemVersion1 = new ItemVersion(uuid(), date, vectorClock);

        const cloned = itemVersion1.clone();
        assert.isTrue(itemVersion1.equals(cloned));
        assert.isTrue(cloned.equals(itemVersion1));

        const itemChange1 = new ItemChange(
            uuid(),
            Operation.INSERT,
            "123",
            date,
            "provider1",
            "provider2",
            date,
            '{"table_name":"my_table", "pk":"123", "fields":{"hello":"world"}}',
            false,
            false,
            vectorClock,
            date
        );

        const itemVersion2 = new ItemVersion(
            uuid(),
            date,
            undefined,
            itemChange1
        );
        const cloned2 = itemVersion2.clone();

        assert.isTrue(itemVersion2.equals(cloned2));
        assert.isTrue(cloned2.equals(itemVersion2));
        assert.isFalse(cloned.equals(cloned2));
        assert.isFalse(cloned2.equals(cloned));
    }
}
