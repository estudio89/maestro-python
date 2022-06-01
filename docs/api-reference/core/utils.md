#


## BaseSyncLock
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/utils.py/#L10)
```python 
BaseSyncLock()
```


---
Prevents two synchronization sessions from running at the same time.

----


## BaseMetadataConverter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/utils.py/#L25)
```python 
BaseMetadataConverter()
```


---
Abstract class to be used for converting metadata objects used by the sync framework to records that can be saved to the data store and back.

----


## SyncTimer
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/utils.py/#L50)
```python 
SyncTimer(
   timeout_seconds
)
```


---
Times the sync session to make sure it finishes before the timeout.


**Attributes**

* **start_time** (float) : Moment when the timer was started (seconds since epoch).
* **timeout_seconds** (float) : Maximum number of seconds for the timer.



**Methods:**


### .tick
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/utils.py/#L66)
```python
.tick()
```

---
Performs a check to see if the timer has finished.


**Raises**

* **SyncTimeoutException**  : If more time than the timeout has elapsed.


----


### get_now_utc
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/utils.py/#L80)
```python
.get_now_utc()
```

---
Current time in UTC.


**Returns**

* **datetime**  : Current time in UTC.


----


### parse_datetime
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/utils.py/#L91)
```python
.parse_datetime(
   value: 'str'
)
```

---
Converts a string to a dt.datetime object.


**Args**

* **value** (str) : An ISO-8601 string.


**Returns**

* **datetime**  : The parsed dt.datetime


----


### is_iterable
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/utils.py/#L112)
```python
.is_iterable(
   value: 'Any'
)
```

---
Checks if a value is an iterable

----


### make_hashable
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/utils.py/#L121)
```python
.make_hashable(
   value
)
```

---
Attempts to make a value hashable.
If the value is a dictionary, it will be converted to a tuple (key, val) and the dict values will be made hashable recursively.
If the value is a non-hashable iterable, it will be converted to a tuble and its values will be made hashable recursively.
