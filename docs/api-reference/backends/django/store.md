#


## DjangoDataStore
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L32)
```python 
DjangoDataStore()
```




**Methods:**


### .get_local_vector_clock
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L40)
```python
.get_local_vector_clock(
   query: 'Optional[Query]' = None
)
```


### .get_item_version
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L63)
```python
.get_item_version(
   item_id: 'str'
)
```


### .get_item_change_by_id
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L74)
```python
.get_item_change_by_id(
   id: 'uuid.UUID'
)
```


### .select_changes
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L104)
```python
.select_changes(
   vector_clock: 'VectorClock', max_num: 'int', query: 'Optional[Query]' = None
)
```


### .select_deferred_changes
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L137)
```python
.select_deferred_changes(
   vector_clock: 'VectorClock', max_num: 'int', query: 'Optional[Query]' = None
)
```


### .save_item_change
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L162)
```python
.save_item_change(
   item_change: 'ItemChange', is_creating: 'bool' = False, query: 'Optional[Query]' = None
)
```


### .save_item
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L179)
```python
.save_item(
   item: 'models.Model'
)
```


### .delete_item
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L182)
```python
.delete_item(
   item: 'models.Model'
)
```


### .run_in_transaction
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L185)
```python
.run_in_transaction(
   item_change: 'ItemChange', callback: 'Callable'
)
```


### .save_conflict_log
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L195)
```python
.save_conflict_log(
   conflict_log: 'ConflictLog'
)
```


### .commit_item_change
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L201)
```python
.commit_item_change(
   operation: 'Operation', entity_name: 'str', item_id: 'str', item: 'Any', execute_operation: 'bool' = True
)
```


### .execute_item_change
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L230)
```python
.execute_item_change(
   item_change: 'ItemChange'
)
```


### .save_item_version
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L256)
```python
.save_item_version(
   item_version: 'ItemVersion'
)
```


### .get_deferred_conflict_logs
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L262)
```python
.get_deferred_conflict_logs(
   item_change_loser: 'ItemChange'
)
```


### .save_sync_session
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L281)
```python
.save_sync_session(
   sync_session: 'SyncSession'
)
```


### .get_item_changes
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L290)
```python
.get_item_changes()
```


### .get_item_versions
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L300)
```python
.get_item_versions()
```


### .get_sync_sessions
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L310)
```python
.get_sync_sessions()
```


### .get_conflict_logs
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L320)
```python
.get_conflict_logs()
```


### .item_to_dict
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/store.py/#L330)
```python
.item_to_dict(
   item: 'Any'
)
```

