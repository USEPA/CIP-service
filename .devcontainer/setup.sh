#!/bin/bash

cd config
chmod +x config-compose.sh
./config-compose.sh engine default
./config-compose.sh admin default
./config-compose.sh demo default

cd ../engine
docker compose -p engine build
docker compose -p engine up --detach
docker ps

cd ../admin
docker compose -p admin build
docker compose -p admin up --detach

cd ../demo
docker compose -p demo build
docker compose -p demo up --detach