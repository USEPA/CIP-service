#!/bin/sh

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

CURRENT_UID=$(id -u):$(id -g)                \
COMPOSE_PROJECT_NAME=config                  \
SCRIPT_PATH=${SCRIPTPATH}                    \
BUNDLE=${1}                                  \
BPROFILE=${2}                                \
docker-compose -f ${SCRIPTPATH}/config-compose.yml up
