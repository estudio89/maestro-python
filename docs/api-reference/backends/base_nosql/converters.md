#


## DateConverter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L50)
```python 
DateConverter()
```




**Methods:**


### .serialize_date
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L51)
```python
.serialize_date(
   value: 'Optional[dt.datetime]'
)
```


### .deserialize_date
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L54)
```python
.deserialize_date(
   value: 'Optional[Any]'
)
```


----


## NoSQLConverter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L58)
```python 

```



----


## DataStoreAccessConverter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L66)
```python 
DataStoreAccessConverter()
```



----


## SyncSessionMetadataConverter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L70)
```python 
SyncSessionMetadataConverter()
```




**Methods:**


### .to_metadata
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L71)
```python
.to_metadata(
   record: 'SyncSessionRecord'
)
```


### .to_record
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L87)
```python
.to_record(
   metadata_object: 'SyncSession'
)
```


----


## ConflictLogMetadataConverter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L106)
```python 
ConflictLogMetadataConverter()
```




**Methods:**


### .to_metadata
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L107)
```python
.to_metadata(
   record: 'ConflictLogRecord'
)
```


### .to_record
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L138)
```python
.to_record(
   metadata_object: 'ConflictLog'
)
```


----


## VectorClockItemMetadataConverter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L156)
```python 
VectorClockItemMetadataConverter()
```




**Methods:**


### .to_metadata
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L157)
```python
.to_metadata(
   record: 'VectorClockItemRecord'
)
```


### .to_record
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L167)
```python
.to_record(
   metadata_object: 'VectorClockItem'
)
```


----


## VectorClockMetadataConverter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L176)
```python 
VectorClockMetadataConverter(
   vector_clock_item_converter: 'VectorClockItemMetadataConverter' = VectorClockItemMetadataConverter()
)
```




**Methods:**


### .to_metadata
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L185)
```python
.to_metadata(
   record: 'List[VectorClockItemRecord]'
)
```


### .to_record
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L196)
```python
.to_record(
   metadata_object: 'VectorClock'
)
```


----


## ItemVersionMetadataConverter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L208)
```python 
ItemVersionMetadataConverter(
   vector_clock_converter: 'VectorClockMetadataConverter' = VectorClockMetadataConverter()
)
```




**Methods:**


### .to_metadata
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L218)
```python
.to_metadata(
   record: 'ItemVersionRecord'
)
```


### .to_record
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L236)
```python
.to_record(
   metadata_object: 'ItemVersion'
)
```


----


## ItemChangeMetadataConverter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L257)
```python 
ItemChangeMetadataConverter(
   item_serializer: 'NoSQLItemSerializer',
   vector_clock_item_converter: 'VectorClockItemMetadataConverter' = VectorClockItemMetadataConverter(),
   vector_clock_converter: 'VectorClockMetadataConverter' = VectorClockMetadataConverter()
)
```




**Methods:**


### .to_metadata
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L274)
```python
.to_metadata(
   record: 'ItemChangeRecord'
)
```


### .to_record
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L304)
```python
.to_record(
   metadata_object: 'ItemChange'
)
```


----


## SortOrderConverter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L337)
```python 
SortOrderConverter()
```




**Methods:**


### .to_metadata
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L338)
```python
.to_metadata(
   record: 'SortOrderRecord'
)
```


### .to_record
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L343)
```python
.to_record(
   metadata_object: 'SortOrder'
)
```


----


## ComparisonMetadataConverter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L349)
```python 
ComparisonMetadataConverter()
```




**Methods:**


### .to_metadata
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L350)
```python
.to_metadata(
   record: 'ComparisonRecord'
)
```


### .to_record
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L357)
```python
.to_record(
   metadata_object: 'Comparison'
)
```


----


## FilterMetadataConverter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L366)
```python 
FilterMetadataConverter(
   comparison_converter: 'ComparisonMetadataConverter' = ComparisonMetadataConverter()
)
```




**Methods:**


### .to_metadata
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L373)
```python
.to_metadata(
   record: 'FilterRecord'
)
```


### .to_record
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L390)
```python
.to_record(
   metadata_object: 'Filter'
)
```


----


## QueryMetadataConverter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L408)
```python 
QueryMetadataConverter(
   filter_converter: 'FilterMetadataConverter' = FilterMetadataConverter(),
   sort_order_converter: 'SortOrderConverter' = SortOrderConverter()
)
```




**Methods:**


### .to_metadata
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L417)
```python
.to_metadata(
   record: 'QueryRecord'
)
```


### .to_record
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L436)
```python
.to_record(
   metadata_object: 'Query'
)
```


----


## TrackedQueryMetadataConverter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L455)
```python 
TrackedQueryMetadataConverter(
   vector_clock_converter: 'VectorClockMetadataConverter' = VectorClockMetadataConverter(),
   query_converter: 'QueryMetadataConverter' = QueryMetadataConverter()
)
```




**Methods:**


### .to_metadata
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L464)
```python
.to_metadata(
   record: 'TrackedQueryRecord'
)
```


### .to_record
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/converters.py/#L472)
```python
.to_record(
   metadata_object: 'TrackedQuery'
)
```

