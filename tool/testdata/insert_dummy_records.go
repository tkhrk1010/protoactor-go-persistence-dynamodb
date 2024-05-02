package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	p "github.com/tkhrk1010/go-samples/actor-model/persistence/dynamodb/persistence"
	"google.golang.org/protobuf/proto"
)

func main() {
	// DynamoDBクライアントの初期化
	client := initializeDynamoDBClient()

	// テストレコードを件作成
	for i := 0; i < 35; i++ {
		// テストデータの作成
		actorName := fmt.Sprintf("actor_%d", i)
		eventIndex := fmt.Sprintf("%d", i)
		event := &p.Event{
			Data: fmt.Sprintf("test_message_%d", i),
		}

		// イベントをバイナリ形式にシリアライズ
		payload, err := proto.Marshal(event)
		if err != nil {
			log.Fatalf("Failed to marshal event: %v", err)
		}

		// レコードを追加
		_, err = client.PutItem(context.TODO(), &dynamodb.PutItemInput{
			TableName: aws.String("journal"),
			Item: map[string]types.AttributeValue{
				"actorName":  &types.AttributeValueMemberS{Value: actorName},
				"eventIndex": &types.AttributeValueMemberN{Value: eventIndex},
				"payload":    &types.AttributeValueMemberB{Value: payload},
			},
		})
		if err != nil {
			log.Fatalf("Failed to put item: %v", err)
		}
		_, err = client.PutItem(context.TODO(), &dynamodb.PutItemInput{
			TableName: aws.String("snapshot"),
			Item: map[string]types.AttributeValue{
				"actorName":  &types.AttributeValueMemberS{Value: actorName},
				"eventIndex": &types.AttributeValueMemberN{Value: eventIndex},
				"payload":    &types.AttributeValueMemberB{Value: payload},
			},
		})
		if err != nil {
			log.Fatalf("Failed to put item: %v", err)
		}
	}

	log.Println("テストレコードを作成しました。")
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