#


## SyncOrchestrator
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/orchestrator.py/#L10)
```python 
SyncOrchestrator(
   sync_lock: 'BaseSyncLock', providers: 'List[BaseSyncProvider]', maximum_duration_seconds: 'int'
)
```


---
Synchronizes data between two providers.


**Attributes**

* **sync_lock** (BaseSyncLock) : Lock used to make sure multiple synchronizations don't happen in parallel.
* **maximum_duration_seconds** (int) : The maximum duration in seconds that the sync session can last. If the session doesn't end by that time, an exception of type SyncTimeoutException is raised.



**Methods:**


### .synchronize_providers
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/orchestrator.py/#L42)
```python
.synchronize_providers(
   source_provider_id: 'str', target_provider_id: 'str', query: 'Optional[Query]' = None
)
```

---
Retrieves data from the source provider and sends them to the target provider.


**Args**

* **source_provider_id** (str) : Source provider's identifier.
* **target_provider_id** (str) : Target provider's identifier.


### .run
[source](https://github.com/estudio89/estudio89/maestro-python/blob/master/maestro/core/orchestrator.py/#L143)
```python
.run(
   initial_source_provider_id: 'str'
)
```

---
Runs two synchronization sessions:
1) initial_source_provider_id => other_provider
2) other_provider => initial_source_provider_id


**Args**

* **initial_source_provider_id** (str) : The identifier of the provider that will first send data.

