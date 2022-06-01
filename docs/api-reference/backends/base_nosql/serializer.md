#


## NoSQLItemSerializer
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/serializer.py/#L12)
```python 
NoSQLItemSerializer()
```




**Methods:**


### .get_skip_fields
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/serializer.py/#L13)
```python
.get_skip_fields()
```


### .serialize_field_value
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/serializer.py/#L16)
```python
.serialize_field_value(
   collection_name: 'str', item: 'Dict[str, Any]', key = 'str'
)
```


### .serialize_item
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/serializer.py/#L24)
```python
.serialize_item(
   item: 'Dict[str, Any]', entity_name: 'str'
)
```


### .deserialize_field_value
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/serializer.py/#L45)
```python
.deserialize_field_value(
   collection_name: 'str', fields: 'Dict[str, Any]', key: 'str'
)
```


### .deserialize_item
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/backends/base_nosql/serializer.py/#L68)
```python
.deserialize_item(
   serialization_result: 'SerializationResult'
)
```

