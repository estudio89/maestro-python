#


## NoSQLDataStore
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/store.py/#L17)
```python 
NoSQLDataStore()
```




**Methods:**


### .update_vector_clocks
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/store.py/#L18)
```python
.update_vector_clocks(
   item_change: 'ItemChange'
)
```

---
Updates the cached VectorClocks with the new change.


**Args**

* **item_change** (ItemChange) : ItemChange that was saved to the data store


### .save_item
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/store.py/#L68)
```python
.save_item(
   item: 'Any'
)
```


### .delete_item
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/store.py/#L72)
```python
.delete_item(
   item: 'Any'
)
```


### .save_item_change
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/store.py/#L76)
```python
.save_item_change(
   item_change: 'ItemChange', is_creating: 'bool' = False, query: 'Optional[Query]' = None
)
```


### .save_conflict_log
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/store.py/#L94)
```python
.save_conflict_log(
   conflict_log: 'ConflictLog'
)
```


### .execute_item_change
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/store.py/#L103)
```python
.execute_item_change(
   item_change: 'ItemChange'
)
```


### .save_item_version
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/store.py/#L113)
```python
.save_item_version(
   item_version: 'ItemVersion'
)
```


### .save_sync_session
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/store.py/#L122)
```python
.save_sync_session(
   sync_session: 'SyncSession'
)
```


### .item_to_dict
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/store.py/#L131)
```python
.item_to_dict(
   item: 'Any'
)
```

