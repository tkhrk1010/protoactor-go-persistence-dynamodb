https://docs.localstack.cloud/getting-started/installation/
https://docs.localstack.cloud/user-guide/aws/dynamodb/#getting-started

## Getting Started

### run
```
make up
```

### create sample table
```
docker-compose exec localstack \
awslocal dynamodb create-table \
    --table-name global01 \
    --key-schema AttributeName=id,KeyType=HASH \
    --attribute-definitions AttributeName=id,AttributeType=S \
    --billing-mode PAY_PER_REQUEST \
    --region ap-south-1
```

expected
```
{
    "TableDescription": {
        "AttributeDefinitions": [
            {
                "AttributeName": "id",
                "AttributeType": "S"
            }
        ],
        "TableName": "global01",
        "KeySchema": [
            {
                "AttributeName": "id",
                "KeyType": "HASH"
            }
        ],
        "TableStatus": "ACTIVE",
        "CreationDateTime": 1712371115.842,
        "ProvisionedThroughput": {
            "LastIncreaseDateTime": 0.0,
            "LastDecreaseDateTime": 0.0,
            "NumberOfDecreasesToday": 0,
            "ReadCapacityUnits": 0,
            "WriteCapacityUnits": 0
        },
        "TableSizeBytes": 0,
        "ItemCount": 0,
        "TableArn": "arn:aws:dynamodb:ap-south-1:000000000000:table/global01",
        "TableId": "bba65d7f-3f0c-4995-b181-9e4a450b68d9",
        "BillingModeSummary": {
            "BillingMode": "PAY_PER_REQUEST",
            "LastUpdateToPayPerRequestDateTime": 1712371115.842
        },
        "DeletionProtectionEnabled": false
    }
}
```