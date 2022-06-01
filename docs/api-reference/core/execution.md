#


## ConflictCheckResult
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/execution.py/#L15)
```python 
ConflictCheckResult()
```


---
Stores the result of a conflict check.


**Attributes**

* **has_conflict** (bool) : True if there is a conflict, False otherwise.
* **conflict_type** (ConflictType) : The type of conflict that was detected.
* **local_item_change** (Optional[ItemChange]) : The change that was made locally.
* **remote_item_change** (Optional[ItemChange]) : The change that was made remotely.
* **local_version** (ItemVersion) : The version of the local item.


----


## ConflictResolution
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/execution.py/#L33)
```python 
ConflictResolution()
```


---
Stores information about which change won and which lost a conflict.


**Attributes**

* **item_change_loser** (ItemChange) : The change that lost the conflict.
* **item_change_winner** (ItemChange) : The change that won the conflict.


----


## ConflictResolver
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/execution.py/#L45)
```python 
ConflictResolver()
```


---
Determines which change won a conflict.


**Methods:**


### .resolve
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/execution.py/#L48)
```python
.resolve(
   conflict_type: 'ConflictType', local_item_change: 'ItemChange', remote_item_change: 'ItemChange'
)
```

---
Selects the winning change between two conflicting changes. It applies the following resolution rules for each type of conflict:

ConflictType.LOCAL_UPDATE_REMOTE_UPDATE, ConflictType.LOCAL_DELETE_REMOTE_DELETE, ConflictType.LOCAL_INSERT_REMOTE_UPDATE,
ConflictType.LOCAL_UPDATE_REMOTE_INSERT - Most recent change wins

ConflictType.LOCAL_UPDATE_REMOTE_DELETE - Deletion wins

ConflictType.LOCAL_DELETE_REMOTE_UPDATE - Deletion wins


**Args**

* **conflict_type** (ConflictType) : Type of conflict
* **local_item_change** (ItemChange) : Local change
* **remote_item_change** (ItemChange) : Remote change


----


## ChangesExecutor
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/execution.py/#L95)
```python 
ChangesExecutor(
   data_store: 'BaseDataStore', events_manager: 'EventsManager', conflict_resolver: 'ConflictResolver'
)
```


---
Processes and applies each change received from a remote provider.


**Methods:**


### .run
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/execution.py/#L112)
```python
.run(
   item_changes: 'List[ItemChange]', query: 'Optional[Query]'
)
```

---
Iterates the changes and applies each one.


**Args**

* **item_changes** (List[ItemChange]) : list of changes to be processed.


### .process_remote_change
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/execution.py/#L121)
```python
.process_remote_change(
   item_change: 'ItemChange', query: 'Optional[Query]'
)
```

---
Processes a change received from a remote provider. Processing means:

- Checking if it needs to be applied
- Checking to see if the change causes a conflict
- Handling conflicts
- Executing the change
- Posting events


**Args**

* **item_change** (ItemChange) : The change to be processed
* **query** (Optional[Query]) : The query that is being synced


### .check_conflict
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/execution.py/#L179)
```python
.check_conflict(
   remote_item_change: 'ItemChange'
)
```

---
Checks if the remote change causes a conflict.
A conflict occurs any time the remote provider wasn't aware of the change
currently linked to the local version of the item being changed.


**Args**

* **remote_item_change** (ItemChange) : The change received from the remote provider.


**Returns**

* **ConflictCheckResult**  : The result of the analysis


### .handle_conflict
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/execution.py/#L266)
```python
.handle_conflict(
   conflict_type: 'ConflictType', local_item_change: 'ItemChange', remote_item_change: 'ItemChange',
   local_version: 'ItemVersion'
)
```

---
Called whenever a conflict is detected.


**Args**

* **conflict_type** (ConflictType) : Type of conflict
* **local_item_change** (ItemChange) : The local change applied to the item previously
* **remote_item_change** (ItemChange) : The remote change that caused the conflict


**Returns**

* A function that is to be called at the end of the transaction


### .handle_exception
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/execution.py/#L310)
```python
.handle_exception(
   remote_item_change: 'ItemChange', exception: 'Exception', query: 'Optional[Query]'
)
```

---
Called whenever an exception is raised while trying to apply a change to an item.


**Args**

* **remote_item_change** (ItemChange) : The change that was being applied when the exception was raised.
* **exception** (Exception) : The exception that was raised.
* **query** (Optional[Query]) : The query that was being synced


### .apply_item_change
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/execution.py/#L325)
```python
.apply_item_change(
   item_change: 'ItemChange', old_version: 'ItemVersion'
)
```

---
Applies a change. This consists of:
- Executing the change
- Marking the change as applied
- Saving the change to the data store
- Updating the version of the item referenced by the change


**Args**

* **item_change** (ItemChange) : The change to be applied.
* **old_version** (ItemVersion) : The current local version of the item.

