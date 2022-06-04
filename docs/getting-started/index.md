# Getting started

## Installation

Install using `pip`:

```
pip install maestro-python[django,mongo,firestore]
```

The command above will install all dependencies for all the supported backends.

## Setting up Django backend

The Django backend contains a Django app that integrates with your Django application. To add it to your project, you first need to set some things up in your `settings.py` file.

Add `"maestro.backends.django"` to your `INSTALLED_APPS` setting.

```python
INSTALLED_APPS = [
    ...
    "maestro.backends.django",
]
```

Add `"maestro.backends.django.contrib.middleware.SyncQueueMiddleware"` to you `MIDDLEWARE` setting.

```python
MIDDLEWARE = [
    ...
    "maestro.backends.django.contrib.middleware.SyncQueueMiddleware",
]
```

Then, define some basic settings to instruct maestro about what needs to be synchronized:

```python
MAESTRO = {
    # This indicates which models will be sinchronized
    "MODELS": [
        "myapp.mymodel",
        "myapp.myothermodel",
    ],

    # This is the path to a function that will be called
    # at the end of a request whenever
    # changes are made to any of the models listed
    "CHANGES_COMMITTED_CALLBACK": "path.to.callback.function"
}
```

Lastly, you have to define the callback function that will be called whenever changes are made to the models being synchronized. This callback should initiate a synchronization session, so that the data is replicated to the other node.

You can see below a sample implementation of this callback. It will spin up a synchronization session in a separate thread so as not to block the request. In this sample, the data is being synchronized between a Django application and Firestore.

```python

from maestro.backends.django.contrib.factory import create_django_provider
from maestro.backends.firestore.contrib.factory import create_firestore_provider
import maestro.backends.firestore
import maestro.backends.django
from maestro.core.orchestrator import SyncOrchestrator
from firebase_admin import firestore
import threading
from typing import TYPE_CHECKING


def on_changes_commited():
    thread = threading.Thread(target=start_sync, args=["django"])
    thread.start()

def start_sync(initial_source_provider_id: "str"):

    # Django
    django_provider = create_django_provider()

    # Firestore
    firestore_provider = create_firestore_provider()

    # Sync lock
    sync_lock = maestro.backends.django.DjangoSyncLock()

    # Orchestrator
    orchestrator = SyncOrchestrator(
        sync_lock=sync_lock,
        providers=[django_provider, firestore_provider],
        maximum_duration_seconds=10 * 60,
    )
    orchestrator.run(initial_source_provider_id=initial_source_provider_id)

```

To finish things off, run Django's `migrate` command so that Maestro creates the necessary tables:

    ./manage.py migrate

## Setting up Firestore backend

## Setting up MongoDB backend

After installation, all you need to do is notify maestro whenever one of the collections that you'd like to keep in sync is modified. For that, you'll need to call the `commit_item_change` method of the `MongoDataStore` class.

First, create an instance of the store:

```python
from maestro.backends.mongo.contrib.factory import create_mongo_store
from pymongo import MongoClient

client = MongoClient("mongodb://host:port/")
maestro_store = create_mongo_store(
    client=client, database_name="example-db"
)
```

Then, call the `commit_item_change` method whenever changes are made:

- When a new document is **created**:

```python
from maestro.core.metadata import Operation

maestro_store.commit_item_change(
    operation=Operation.INSERT,
    entity_name="collection_name",
    item_id="item_id",
    item={
        "id":"item_id",
        "collection_name": "collection_name",
        "some_property": "some_property"
    }
)
```

An important thing to notice here is that when the method is called, a dictionary is expected for the `item` parameter and it should at least have the properties `id` (the item's primary key) and `collection_name` (the collection where the document is stored).

- When a document is **updated** (pretty much the same, but with a different `Operation`):

```python
from maestro.core.metadata import Operation

maestro_store.commit_item_change(
    operation=Operation.UPDATE,
    entity_name="collection_name",
    item_id="item_id",
    item={
        "id":"item_id",
        "collection_name": "collection_name",
        "some_property": "some_property"
    }
)
```

- When a document is **deleted** (pretty much the same, but with a different `Operation`):

```python
from maestro.core.metadata import Operation

maestro_store.commit_item_change(
    operation=Operation.DELETE,
    entity_name="collection_name",
    item_id="item_id",
    item={
        "id":"item_id",
        "collection_name": "collection_name",
        "some_property": "some_property"
    }
)
```