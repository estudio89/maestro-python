#


## BaseSyncProvider
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/provider.py/#L12)
```python 
BaseSyncProvider(
   provider_id: 'str', data_store: 'BaseDataStore', events_manager: 'EventsManager',
   changes_executor: 'ChangesExecutor', max_num: 'int'
)
```


---
Manages the changes that will be synchronized to a data store.


**Attributes**

* **provider_id** (str) : This provider's unique identifier.
* **data_store** (BaseDataStore) : The data store.
* **events_manager** (EventsManager) : The class that will handle synchronization events.
* **changes_executor** (ChangesExecutor) : The class that will process the changes to be applied to the data store.
* **max_num** (int) : The maximum number of changes that will be processed in each batch of changes.



**Methods:**


### .get_vector_clock
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/provider.py/#L51)
```python
.get_vector_clock(
   query: 'Optional[Query]' = None
)
```

---
Returns the current VectorClock for this provider.


**Returns**

* **VectorClock**  : The current VectorClock for this provider.


### .download_changes
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/provider.py/#L59)
```python
.download_changes(
   vector_clock: 'VectorClock', query: 'Optional[Query]' = None
)
```

---
Retrieves the changes that occurred in the data store linked to this provider after the timestamps defined by the given VectorClock.


**Args**

* **vector_clock** (VectorClock) : VectorClock used for selecting changes.


**Returns**

* **ItemChangeBatch**  : The batch of changes that was selected.


### .upload_changes
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/provider.py/#L76)
```python
.upload_changes(
   item_change_batch: 'ItemChangeBatch', query: 'Optional[Query]'
)
```

---
Applies changes obtained from a remote provider to the data store.


**Args**

* **item_change_batch** (ItemChangeBatch) : The batch of changes to be applied.
* **query** (Optional[Query]) : The query that's being synced.


### .get_deferred_changes
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/provider.py/#L90)
```python
.get_deferred_changes(
   vector_clock: 'VectorClock', query: 'Optional[Query]' = None
)
```

---
Retrieves the changes received previously but that weren't applied in the last session due to an exception having occurred.


**Args**

* **vector_clock** (VectorClock) : VectorClock used to select the changes.


**Returns**

* **ItemChangeBatch**  : The batch of changes that was selected.

