#


## EventsManager
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/events.py/#L20)
```python 
EventsManager(
   data_store: 'BaseDataStore'
)
```


---
Handles the events that happen during a sync session.


**Methods:**


### .on_start_sync_session
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/events.py/#L30)
```python
.on_start_sync_session(
   source_provider_id: 'str', target_provider_id: 'str', query: 'Optional[Query]'
)
```

---
This is called at the start of a sync session. It creates a sync session and saves it to the data store.


**Args**

* **source_provider_id** (str) : Source provider id.
* **target_provider_id** (str) : Target provider id.


### .on_conflict_resolved
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/events.py/#L56)
```python
.on_conflict_resolved(
   conflict_type: 'ConflictType', item_change_winner: 'ItemChange', item_change_loser: 'ItemChange'
)
```

---
Called after a conflict is resolved. It creates a ConflictLog and saves it to the data store.


**Args**

* **conflict_type** (ConflictType) : Type of conflict
* **item_change_winner** (ItemChange) : ItemChange that won the conflict
* **item_change_loser** (ItemChange) : ItemChange that lost the conflict


### .on_exception
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/events.py/#L89)
```python
.on_exception(
   remote_item_change: 'ItemChange', exception: 'Exception', query: 'Optional[Query]'
)
```

---
Called when an exception is raised when trying to execute an ItemChange. It creates a ConflictLog and saves it to the data store.


**Args**

* **remote_item_change** (ItemChange) : ItemChange that was being executed when the exception was raised.
* **exception** (Exception) : Exception that was raised.
* **query** (Optional[Query]) : The query that was being synced


**Returns**

* **TYPE**  : Description


### .on_item_change_processed
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/events.py/#L132)
```python
.on_item_change_processed(
   item_change: 'ItemChange'
)
```

---
Called after an ItemChange is saved to the data store but before it is executed. It adds the ItemChange to the running sync session.


**Args**

* **item_change** (ItemChange) : ItemChange saved to the data store


### .on_item_change_applied
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/events.py/#L140)
```python
.on_item_change_applied(
   item_change: 'ItemChange'
)
```

---
Called after an ItemChange was executed successfully.


**Args**

* **item_change** (ItemChange) : The change that was executed


### .on_item_changes_sent
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/events.py/#L158)
```python
.on_item_changes_sent(
   item_changes: 'List[ItemChange]'
)
```

---
Called after a list of changes is sent to another provider.


**Args**

* **item_changes** (List[ItemChange]) : The changes that were sent.


### .on_end_sync_session
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/events.py/#L166)
```python
.on_end_sync_session()
```

---
Called at the end of the sync session.

### .on_failed_sync_session
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/events.py/#L176)
```python
.on_failed_sync_session(
   exception: 'Exception'
)
```

---
Called if an exception is raised while running the sync session. Note that this would be called if there was a
failure in the framework itself, not in the execution of an ItemChange.
