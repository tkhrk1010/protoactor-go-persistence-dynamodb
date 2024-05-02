#!/bin/bash

# Create table
# localhostじゃないことに注意。
docker-compose exec awscli aws dynamodb create-table \
    --endpoint-url=http://host.docker.internal:4566 \
    --table-name MyTable \
    --attribute-definitions AttributeName=ID,AttributeType=S \
    --key-schema AttributeName=ID,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1