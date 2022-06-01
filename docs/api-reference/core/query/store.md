#


## TrackQueriesStoreMixin
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/query/store.py/#L11)
```python 
TrackQueriesStoreMixin()
```


---
A mixin that allows a data store to track queries.


**Methods:**


### .commit_item_change
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/query/store.py/#L17)
```python
.commit_item_change(
   operation: 'Operation', entity_name: 'str', item_id: 'str', item: 'Any', execute_operation: 'bool' = True
)
```


### .get_item_ids_for_query
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/query/store.py/#L45)
```python
.get_item_ids_for_query(
   query: 'Query', vector_clock = 'VectorClock'
)
```


### .start_tracking_query
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/query/store.py/#L92)
```python
.start_tracking_query(
   query: 'Query'
)
```


### .update_query_vector_clock
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/query/store.py/#L100)
```python
.update_query_vector_clock(
   tracked_query: 'TrackedQuery', item_change: 'ItemChange'
)
```

---
Updates the vector clock for the given query.


**Args**

* **tracked_query** (TrackedQuery) : The query being tracked
* **item_change_record** (ItemChangeRecord) : The record that caused the update


### .check_impacts_query
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/query/store.py/#L117)
```python
.check_impacts_query(
   item_change: 'ItemChange', query: 'Query', vector_clock: 'Optional[VectorClock]'
)
```

---
Checks whether an ItemChange is part of a query.


**Args**

* **item** (Any) : The item being checked
* **query** (Query) : The query
* **vector_clock** (Optional[VectorClock]) : The VectorClock that should
be considered when evaluating the query

### .check_tracked_query_vector_clocks
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/query/store.py/#L152)
```python
.check_tracked_query_vector_clocks(
   new_item_change: 'ItemChange', old_item_change: 'Optional[ItemChange]' = None,
   ignore_old_change_if_none: 'bool' = False
)
```

---
Checks whether the change impacts any of the queries being tracked and updates
their VectorClocks accordingly.


**Args**

* **new_item_change** (ItemChange) : The new change
* **old_item_change** (ItemChange) : The previous change associated with the same item

