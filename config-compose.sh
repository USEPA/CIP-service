#!/bin/sh

CURRENT_UID=$(id -u):$(id -g) COMPOSE_PROJECT_NAME=config-compose COMPOSE_PROFILE=${1} PROJDIR=${2} docker-compose -f ./config-compose.yml up
