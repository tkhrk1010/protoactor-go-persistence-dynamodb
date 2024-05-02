package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	p "github.com/tkhrk1010/go-samples/actor-model/persistence/dynamodb/persistence"
	"google.golang.org/protobuf/proto"
)

const (
	batchSize         = 10
	journalTable      = "journal"
	snapshotTable     = "snapshot"
	journalReadTable  = "journal_readable"
	snapshotReadTable = "snapshot_readable"
)

type TableSchema struct {
	AttributeDefinitions []types.AttributeDefinition
	KeySchema            []types.KeySchemaElement
}

type KeyNames struct {
	ActorName  string
	EventIndex string
	Payload    string
}

func tableSchema() TableSchema {
	return TableSchema{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("actorName"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("eventIndex"),
				AttributeType: types.ScalarAttributeTypeN,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("actorName"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("eventIndex"),
				KeyType:       types.KeyTypeRange,
			},
		},
	}
}

func main() {
	log.Println("start")

	client := initializeDynamoDBClient()

	// テーブルスキーマの定義
	tableSchema := tableSchema()

	err := createTableIfNotExists(client, journalReadTable, tableSchema)
	if err != nil {
		log.Fatalf("Failed to create journal_readable table: %v", err)
	}

	err = createTableIfNotExists(client, snapshotReadTable, tableSchema)
	if err != nil {
		log.Fatalf("Failed to create snapshot_readable table: %v", err)
	}

	// キー名の定義
	keyNames := KeyNames{
		ActorName:  "actorName",
		EventIndex: "eventIndex",
		Payload:    "payload",
	}

	err = processBatchData(client, journalTable, journalReadTable, batchSize, keyNames)
	if err != nil {
		log.Fatalf("Failed to process journal data: %v", err)
	}

	err = processBatchData(client, snapshotTable, snapshotReadTable, batchSize, keyNames)
	if err != nil {
		log.Fatalf("Failed to process snapshot data: %v", err)
	}

	log.Println("ETL process completed successfully")
}

func scanTableWithLimit(client *dynamodb.Client, tableName string, startKey map[string]types.AttributeValue, limit int) ([]map[string]types.AttributeValue, map[string]types.AttributeValue, error) {
	input := &dynamodb.ScanInput{
		TableName: aws.String(tableName),
		Limit:     aws.Int32(int32(limit)),
	}
	if startKey != nil {
		input.ExclusiveStartKey = startKey
	}

	output, err := client.Scan(context.TODO(), input)
	if err != nil {
		return nil, nil, err
	}

	return output.Items, output.LastEvaluatedKey, nil
}

func processBatchData(client *dynamodb.Client, srcTableName, dstTableName string, batchSize int, keyNames KeyNames) error {
	var startKey map[string]types.AttributeValue

	for {
		// 一定サイズのデータを読み込む
		data, lastEvaluatedKey, err := scanTableWithLimit(client, srcTableName, startKey, batchSize)
		if err != nil {
			return fmt.Errorf("Failed to scan %s table: %v", srcTableName, err)
		}

		// 読み込んだデータを処理し、保存する
		err = processAndSaveData(client, data, dstTableName, batchSize, keyNames)
		if err != nil {
			return fmt.Errorf("Failed to process and save %s data: %v", srcTableName, err)
		}

		if lastEvaluatedKey == nil {
			break
		}
		startKey = lastEvaluatedKey
	}

	return nil
}

func processAndSaveData(client *dynamodb.Client, data []map[string]types.AttributeValue, tableName string, batchSize int, keyNames KeyNames) error {
	var writeReqs []types.WriteRequest

	for _, item := range data {
		payload, ok := item[keyNames.Payload].(*types.AttributeValueMemberB)
		if !ok {
			return fmt.Errorf("payload is not a binary type")
		}
		processedPayload, err := processPayload(payload.Value)
		if err != nil {
			return err
		}

		actorName := item[keyNames.ActorName].(*types.AttributeValueMemberS).Value
		eventIndex := item[keyNames.EventIndex].(*types.AttributeValueMemberN).Value

		newItem := map[string]types.AttributeValue{
			keyNames.ActorName:  &types.AttributeValueMemberS{Value: actorName},
			keyNames.EventIndex: &types.AttributeValueMemberN{Value: eventIndex},
			keyNames.Payload:    &types.AttributeValueMemberS{Value: processedPayload},
		}

		writeReqs = append(writeReqs, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: newItem,
			},
		})

		// バッチサイズに達したら書き込む
		if len(writeReqs) == batchSize {
			err := batchWriteItems(client, tableName, writeReqs)
			if err != nil {
				return err
			}
			writeReqs = []types.WriteRequest{}
		}
	}

	// 残りのリクエストを書き込む
	if len(writeReqs) > 0 {
		err := batchWriteItems(client, tableName, writeReqs)
		if err != nil {
			return err
		}
	}

	return nil
}

func batchWriteItems(client *dynamodb.Client, tableName string, writeReqs []types.WriteRequest) error {
	_, err := client.BatchWriteItem(context.TODO(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			tableName: writeReqs,
		},
	})
	return err
}

func initializeDynamoDBClient() *dynamodb.Client {
	ctx := context.TODO()

	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if service == dynamodb.ServiceID {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           "http://localhost:4566",
				SigningRegion: "us-east-1",
			}, nil
		}
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(aws.AnonymousCredentials{}),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	if err != nil {
		panic(fmt.Sprintf("unable to load SDK config, %v", err))
	}

	return dynamodb.NewFromConfig(cfg)
}

func createTableIfNotExists(client *dynamodb.Client, tableName string, schema TableSchema) error {
	_, err := client.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		var notFoundErr *types.ResourceNotFoundException
		if errors.As(err, &notFoundErr) {
			// テーブルが存在しない場合は作成する
			_, err := client.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
				AttributeDefinitions: schema.AttributeDefinitions,
				KeySchema:            schema.KeySchema,
				TableName:            aws.String(tableName),
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(3),
					WriteCapacityUnits: aws.Int64(3),
				},
			})
			if err != nil {
				return err
			}
			// テーブルが作成されるまで待つ
			err = waitUntilTableExists(client, tableName)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	return nil
}

func waitUntilTableExists(client *dynamodb.Client, tableName string) error {
	waiter := dynamodb.NewTableExistsWaiter(client)
	err := waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}, 5*time.Minute)
	if err != nil {
		return err
	}
	return nil
}

// byte列のpayloadをjson stringにする
func processPayload(payload []byte) (string, error) {
	event := &p.Event{}
	err := proto.Unmarshal(payload, event)
	if err != nil {
		return "", err
	}

	jsonBytes, err := json.Marshal(event)
	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}