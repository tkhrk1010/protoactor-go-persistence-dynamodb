#!/bin/sh

# Thanks to:
# https://github.com/tkhrk1010/cqrs-es-example-go?tab=readme-ov-file

#
# Usage:
#  sh create-tables.sh -e dev
#

# shellcheck disable=SC2046
cd $(dirname "$0") && pwd

# shellcheck disable=SC2039
if [ $# == 0 ]; then
  echo "Parameters are empty."
  exit 1
fi

while getopts e: OPT; do
  # shellcheck disable=SC2220
  case ${OPT} in
  "e") ENV_NAME="$OPTARG" ;;
  esac
done

export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-us-west-1}
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-x}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-x}
export AWS_PAGER=""

ENDPOINT_URL_OPTION=""
DYNAMODB_ENDPOINT=${DYNAMODB_ENDPOINT:-host.docker.internal:4566}

# 変更している
JOURNAL_TABLE_NAME=${JOURNAL_TABLE_NAME:-"journal"}
JOURNAL_GSI_NAME=${JOURNAL_GSI_NAME:-"journal-aid-index"}
SNAPSHOT_TABLE_NAME=${SNAPSHOT_TABLE_NAME:-"snapshot"}
SNAPSHOT_GSI_NAME=${SNAPSHOT_GSI_NAME:-"snapshot-aid-index"}

echo "DYNAMODB_ENDPOINT = ${DYNAMODB_ENDPOINT}"
echo "JOURNAL_TABLE_NAME = ${JOURNAL_TABLE_NAME}"
echo "JOURNAL_GSI_NAME = ${JOURNAL_GSI_NAME}"
echo "SNAPSHOT_TABLE_NAME = ${SNAPSHOT_TABLE_NAME}"
echo "SNAPSHOT_GSI_NAME = ${SNAPSHOT_GSI_NAME}"

if [ "${ENV_NAME}" = "dev" ]; then
  # shellcheck disable=SC2034
  ENDPOINT_URL_OPTION=" --endpoint-url http://${DYNAMODB_ENDPOINT} "
fi

echo "ENDPOINT_URL_OPTION = ${ENDPOINT_URL_OPTION}"

echo "delete existing tables and create new tables"
# Delete existing tables if they exist
docker-compose exec awscli aws dynamodb delete-table --table-name "${JOURNAL_TABLE_NAME}" ${ENDPOINT_URL_OPTION} 2>/dev/null
docker-compose exec awscli aws dynamodb delete-table --table-name "${SNAPSHOT_TABLE_NAME}" ${ENDPOINT_URL_OPTION} 2>/dev/null

echo "create new tables"
docker-compose exec awscli aws dynamodb create-table \
  ${ENDPOINT_URL_OPTION} \
  --table-name "${JOURNAL_TABLE_NAME}" \
  --attribute-definitions \
    AttributeName=pkey,AttributeType=S \
    AttributeName=skey,AttributeType=S \
    AttributeName=aid,AttributeType=S \
    AttributeName=seq_nr,AttributeType=N \
  --key-schema \
    AttributeName=pkey,KeyType=HASH \
    AttributeName=skey,KeyType=RANGE \
  --provisioned-throughput \
    ReadCapacityUnits=10,WriteCapacityUnits=10 \
  --global-secondary-indexes \
  "[
    {
      \"IndexName\": \"${JOURNAL_GSI_NAME}\",
      \"KeySchema\": [{\"AttributeName\":\"aid\",\"KeyType\":\"HASH\"},
                      {\"AttributeName\":\"seq_nr\",\"KeyType\":\"RANGE\"}],
      \"Projection\":{
        \"ProjectionType\":\"ALL\"
      },
      \"ProvisionedThroughput\": {
        \"ReadCapacityUnits\": 10,
        \"WriteCapacityUnits\": 10
      }
    }
  ]" \
  --stream-specification "StreamEnabled=true,StreamViewType=NEW_IMAGE"

# shellcheck disable=SC2086
docker-compose exec awscli aws dynamodb create-table \
  ${ENDPOINT_URL_OPTION} \
  --table-name "${SNAPSHOT_TABLE_NAME}" \
  --attribute-definitions \
    AttributeName=pkey,AttributeType=S \
    AttributeName=skey,AttributeType=S \
    AttributeName=aid,AttributeType=S \
    AttributeName=seq_nr,AttributeType=N \
  --key-schema \
    AttributeName=pkey,KeyType=HASH \
    AttributeName=skey,KeyType=RANGE \
  --provisioned-throughput \
    ReadCapacityUnits=10,WriteCapacityUnits=10 \
  --global-secondary-indexes \
  "[
    {
      \"IndexName\": \"${SNAPSHOT_GSI_NAME}\",
      \"KeySchema\": [{\"AttributeName\":\"aid\",\"KeyType\":\"HASH\"},
                      {\"AttributeName\":\"seq_nr\",\"KeyType\":\"RANGE\"}],
      \"Projection\":{
        \"ProjectionType\":\"ALL\"
      },
      \"ProvisionedThroughput\": {
        \"ReadCapacityUnits\": 10,
        \"WriteCapacityUnits\": 10
      }
    }
  ]"
