#!/bin/bash

# Create table
# localhostじゃないことに注意。
docker-compose exec awscli aws dynamodb create-table \
    --endpoint-url=http://host.docker.internal:4566 \
    --table-name journal \
    --attribute-definitions \
        AttributeName=actorName,AttributeType=S \
        AttributeName=eventIndex,AttributeType=N \
    --key-schema \
        AttributeName=actorName,KeyType=HASH \
        AttributeName=eventIndex,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

docker-compose exec awscli aws dynamodb create-table \
    --endpoint-url=http://host.docker.internal:4566 \
    --table-name snapshot \
    --attribute-definitions \
        AttributeName=actorName,AttributeType=S \
        AttributeName=eventIndex,AttributeType=N \
    --key-schema \
        AttributeName=actorName,KeyType=HASH \
        AttributeName=eventIndex,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1