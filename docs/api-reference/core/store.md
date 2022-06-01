#


## BaseDataStore
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/store.py/#L23)
```python 
BaseDataStore(
   local_provider_id: 'str', sync_session_metadata_converter: 'BaseMetadataConverter',
   item_version_metadata_converter: 'BaseMetadataConverter',
   item_change_metadata_converter: 'BaseMetadataConverter',
   conflict_log_metadata_converter: 'BaseMetadataConverter',
   vector_clock_metadata_converter: 'BaseMetadataConverter', item_serializer: 'BaseItemSerializer'
)
```


---
Abstract class that encapsulates the access to the storage system.


**Attributes**

* **conflict_log_metadata_converter** (BaseMetadataConverter) : Instance used to convert ConflictLog objects to data store native records and back.
* **item_change_metadata_converter** (BaseMetadataConverter) : Instance used to convert ItemChange objects to data store native records and back.
* **item_serializer** (BaseItemSerializer) : Instance used to convert serialize data store items to strings.
* **item_version_metadata_converter** (BaseMetadataConverter) : Instance used to convert ItemVersion objects to data store native records and back.
* **local_provider_id** (str) : Unique identifier of the provider that controls this data store.
* **sync_session_metadata_converter** (BaseMetadataConverter) : Instance used to convert SyncSession objects to data store native records and back.
* **vector_clock_metadata_converter** (BaseMetadataConverter) : Instance used to convert VectorClock objects to data store native records and back.



**Methods:**


### .query_items
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/store.py/#L67)
```python
.query_items(
   query: 'Query', vector_clock: 'Optional[VectorClock]'
)
```

---
Returns a list of the item ids that satisfy a query.


**Args**

* **query** (Query) : The query being tested
* **vector_clock** (Optional[VectorClock]) : A VectorClock that if provided, returns the items that would have
matched the query at the time indicated by the clock, enabling time-travel through the data. The items
are returned in the same state they were at the time of the clock.

### .get_local_version
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/store.py/#L80)
```python
.get_local_version(
   item_id: 'str'
)
```

---
Retrieves the current version of the item with the given id.


**Args**

* **item_id** (str) : Primary key of the item whose version we're looking for.


### .get_or_create_item_change
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/store.py/#L105)
```python
.get_or_create_item_change(
   item_change: 'ItemChange', query: 'Optional[Query]'
)
```

---
Looks for the ItemChange in the data store and if it's not found, saves it to the data store.


**Args**

* **item_change** (ItemChange) : The ItemChange saved to the data store.


### .commit_item_change
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/store.py/#L126)
```python
.commit_item_change(
   operation: 'Operation', entity_name: 'str', item_id: 'str', item: 'Any', execute_operation: 'bool' = True
)
```

---
This method will never be called directly by the sync framework but by the application consuming the framework.
   It will perform the operation given as well as record all the metadata necessary for synchronization.


**Args**

* **operation** (Operation) : The operation being performed.
* **item_id** (str) : The item's primary key.
* **item** (Any) : The item that is being changed.


### .serialize_item
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/store.py/#L186)
```python
.serialize_item(
   item: 'Any', entity_name: 'str'
)
```

---
Serializes the given item.


**Args**

* **item** (Any) : Item to be serialized.


### .deserialize_item
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/store.py/#L196)
```python
.deserialize_item(
   serialization_result: 'SerializationResult'
)
```

---
Deserializes an item.


**Args**

* **serialization_result** (SerializationResult) : The result of the item serialization.


### .get_items
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/store.py/#L356)
```python
.get_items()
```

---
Returns a list with all the items in the data store.
DO NOT use in production, used only in tests.

### .get_item_by_id
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/store.py/#L361)
```python
.get_item_by_id(
   id: 'str'
)
```

---
Returns the item with the given primary key. Used only in tests.


**Args**

* **id** (str) : Item's primary key.


**Returns**

* **Any**  : The matching item.


**Raises**

* **ItemNotFoundException**  : If the item is not found.


### .show
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/store.py/#L432)
```python
.show(
   items_only = False
)
```

---
Prints all the data in the store. DO NOT use in production, used only in tests.


**Args**

* **items_only** (bool, optional) : Whether only the items should be printed.

