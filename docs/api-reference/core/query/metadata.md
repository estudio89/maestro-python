#


## Comparator
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/query/metadata.py/#L7)
```python 
Comparator()
```


---
Represents a comparison operation that can be performed in a field.

----


## Comparison
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/query/metadata.py/#L19)
```python 
Comparison(
   field_name: 'str', comparator: 'Comparator', value: 'Any'
)
```


---
Stores a comparison that can be done to a field, such as field1 > 2, field1 == 3, etc.


**Attributes**

* **field_name** (str) : The name of the field to compare.
* **comparator** (Comparator) : The comparison operation to perform.
* **value** (Any) : The value to compare against.


----


## Connector
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/query/metadata.py/#L53)
```python 
Connector()
```


---
Represents the connection between filters when they are combined.

----


## Filter
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/query/metadata.py/#L60)
```python 
Filter(
   children: 'List[Union[Filter, Comparison]]', connector: 'Connector' = Connector.AND
)
```


---
Represents a filtering operation. The combined filters are stored in a tree where the leaves are always Comparison objects.


**Attributes**

* **children** (List[Union[Filter, Comparison]]) : These are the nested filters that were combined into this one. If this is a single
* **connector** (TYPE) : The connection between this filter's children. If this is a single non-combined filter, its operator will be
non-combined filter, it will contain a single Comparison instance.
equal to Connector.AND.


**Methods:**


### .add
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/query/metadata.py/#L81)
```python
.add(
   child: 'Filter'
)
```

---
Add another child to this filter.


**Args**

* **child** (Filter) : the filter being added.


### .combine
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/query/metadata.py/#L96)
```python
.combine(
   other: 'Filter', connector: 'Connector'
)
```

---
Combines two filters using the given connector.


**Args**

* **other** (Filter) : The filter being combined into this one
* **connector** (Connector) : The connector to be used.


**Returns**

* **Filter**  : The combined filter


**Raises**

* **TypeError**  : If the instance passed is not a Filter


----


## SortOrder
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/query/metadata.py/#L161)
```python 
SortOrder(
   field_name: 'str', descending: 'bool' = False
)
```


---
Stores an ordering instruction for a particular field.


**Attributes**

* **field_name** (str) : The name of the field to order by.
* **descending** (bool) : Whether or not to order in descending order.


----


## Query
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/query/metadata.py/#L185)
```python 
Query(
   entity_name: 'str', filter: 'Filter', ordering: 'List[SortOrder]', limit: 'Optional[Any]',
   offset: 'Optional[Any]'
)
```


---
Represents a query with an optional filter and an ordering.


**Attributes**

* **filter** (Filter) : The filter that needs to be applied
* **ordering** (List[SortOrder]) : The sort order that should be applied



**Methods:**


### .get_id
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/query/metadata.py/#L235)
```python
.get_id()
```

---
Returns a unique identifier for this query.

----


## TrackedQuery
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/query/metadata.py/#L241)
```python 
TrackedQuery(
   query: 'Query', vector_clock: 'VectorClock'
)
```


---
Represents a query being tracked by a provider node.


**Attributes**

* **query** (Query) : The query being tracked
* **vector_clock** (VectorClock) : The vector clock for this query

