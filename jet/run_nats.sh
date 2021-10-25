#!/usr/bin/env bash

mkdir /tmp/nats
docker stop nats-main || true
docker run -d --rm --name nats-main -p 4222:4222 -p 6222:6222 -p 8222:8222 -v /tmp/nats/:/tmp/nats/ nats -js -sd=/tmp/nats

docker stop redis || true
docker run --rm -d --name redis -p 6379:6379 -d redis