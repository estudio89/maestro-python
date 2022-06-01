#


## SyncSessionMetadataConverter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/converters.py/#L30)
```python 
SyncSessionMetadataConverter()
```




**Methods:**


### .to_metadata
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/converters.py/#L31)
```python
.to_metadata(
   record: 'SyncSessionRecord'
)
```


### .to_record
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/converters.py/#L50)
```python
.to_record(
   metadata_object: 'SyncSession'
)
```


----


## ItemVersionMetadataConverter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/converters.py/#L70)
```python 
ItemVersionMetadataConverter()
```




**Methods:**


### .to_metadata
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/converters.py/#L71)
```python
.to_metadata(
   record: 'ItemVersionRecord'
)
```


### .to_record
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/converters.py/#L85)
```python
.to_record(
   metadata_object: 'ItemVersion'
)
```


----


## ItemChangeMetadataConverter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/converters.py/#L104)
```python 
ItemChangeMetadataConverter()
```




**Methods:**


### .to_metadata
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/converters.py/#L105)
```python
.to_metadata(
   record: 'ItemChangeRecord'
)
```


### .to_record
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/converters.py/#L134)
```python
.to_record(
   metadata_object: 'ItemChange'
)
```


----


## ConflictLogMetadataConverter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/converters.py/#L160)
```python 
ConflictLogMetadataConverter()
```




**Methods:**


### .to_metadata
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/converters.py/#L161)
```python
.to_metadata(
   record: 'ConflictLogRecord'
)
```


### .to_record
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/converters.py/#L187)
```python
.to_record(
   metadata_object: 'ConflictLog'
)
```


----


## VectorClockMetadataConverter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/converters.py/#L203)
```python 
VectorClockMetadataConverter()
```




**Methods:**


### .to_metadata
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/converters.py/#L204)
```python
.to_metadata(
   record: 'List[Dict]'
)
```


### .to_record
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/django/converters.py/#L217)
```python
.to_record(
   metadata_object: 'VectorClock'
)
```

