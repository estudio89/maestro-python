#!/bin/bash

script_full_path=$(cd "$(dirname "$0")"; pwd -P)

docker run -d --name maestro-mongo -it -v $script_full_path/mongo-keyfile:/mongo-keyfile -e MONGO_INITDB_ROOT_USERNAME=maestro -e MONGO_INITDB_ROOT_PASSWORD=maestro -p 27000:27017 mongo:5.0.0 --keyFile /mongo-keyfile --replSet maestro-set

sleep 5

docker exec -it maestro-mongo mongo "mongodb://maestro:maestro@localhost" --eval "rs.initiate();"