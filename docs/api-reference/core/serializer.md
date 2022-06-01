#


## BaseItemSerializer
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/serializer.py/#L16)
```python 
BaseItemSerializer()
```


---
Abstract class that serializes items to string and back.

----


## BaseMetadataSerializer
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/serializer.py/#L37)
```python 
BaseMetadataSerializer()
```


---
Abstract class that serializes metadata objects to dictionaries of primitive types and back.


----


## MetadataSerializer
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/serializer.py/#L51)
```python 
MetadataSerializer()
```


---
Concrete implementation of a BaseMetadataSerializer.



**Methods:**


### .serialize
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/serializer.py/#L100)
```python
.serialize(
   metadata_object: 'Any'
)
```

---
Converts the metadata object to a dictionary of primitive types.


**Args**

* **metadata_object** (Any) : Metadata object to be serialized.


**Returns**

* **Dict**  : A dictionary of primitive types.


----


## RawDataStoreJSONSerializer
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/serializer.py/#L112)
```python 
RawDataStoreJSONSerializer(
   metadata_serializer: 'BaseMetadataSerializer', indent: 'int'
)
```


---
Serializes the contents of a data store to JSON string. This class is only ever used in tests,
it shouldn't be used in production as it would read ALL the data from the data store.


**Attributes**

* **metadata_serializer** (BaseMetadataSerializer) : The serializer that should be used for converting the metadata objects in the data store to primitive dictionaries.
* **indent** (int) : Indentation to be used when the JSON string is generated.



**Methods:**


### .serialize
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/serializer.py/#L133)
```python
.serialize(
   data_store: 'BaseDataStore'
)
```

---
Converts the contents of the data store to a JSON string. This is used only for testing, DO NOT use in production.


**Args**

* **data_store** (BaseDataStore) : The data store being serialized.


**Returns**

* **str**  : JSON string.

