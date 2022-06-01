# Welcome to Maestro's documentation

Maestro is a framework for synchronizing data accross distributed systems.

## What is it?

**Maestro** is a framework for two-way data synchronization accross distributed data sources. In essence, it lets you keep multiple databases in sync.

## How does it work?

**Maestro** uses a concept called [vector clocks](https://en.wikipedia.org/wiki/Vector_clock) to keep the data in sync. Each node that participates in the synchronization keeps a log of all changes made to it. This log is then used to propagate the changes across the nodes.

## What is it for?

This library was created for a particular purpose: being able to synchronize data between a Cloud Firestore database and a Django application. The purpose was to leverage Firebase's offline sync capabilities on mobile while still being able to use Django as a backend.

It currently is able to synchronize data between Django (and all databases it supports), Firestore and MongoDB.
