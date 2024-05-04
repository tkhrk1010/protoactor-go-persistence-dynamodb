package persistence_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	p "github.com/tkhrk1010/protoactor-go-persistence-dynamodb/persistence"
	"google.golang.org/protobuf/proto"
)

func TestSnapshotStore_GetSnapshot(t *testing.T) {
	tableName := "testSnapshotTable"

	client := InitializeDynamoDBClient()
	snapshotStore := p.NewSnapshotStore(client, tableName)

	actorName := "testActor"
	eventIndex := 1
	snapshotData := &p.Snapshot{Data: "testSnapshot"}

	// スナップショットを保存
	snapshotBytes, err := proto.Marshal(snapshotData)
	assert.NoError(t, err)
	item := map[string]types.AttributeValue{
		"actorName":  &types.AttributeValueMemberS{Value: actorName},
		"eventIndex": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", eventIndex)},
		"payload":    &types.AttributeValueMemberB{Value: snapshotBytes},
	}
	putInput := &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item,
	}
	_, err = client.PutItem(context.Background(), putInput)
	assert.NoError(t, err)

	// GetSnapshotを呼び出して、保存したスナップショットを取得
	retrievedSnapshot, retrievedEventIndex, ok := snapshotStore.GetSnapshot(actorName)
	assert.True(t, ok)
	assert.Equal(t, eventIndex, retrievedEventIndex)

	snapshot, ok := retrievedSnapshot.(*p.Snapshot)
	assert.True(t, ok)
	assert.Equal(t, snapshotData.Data, snapshot.Data)

	// 存在しないスナップショットを取得
	_, _, ok = snapshotStore.GetSnapshot("nonexistentActor")
	assert.False(t, ok)

	// クリーンアップ
	key, err := attributevalue.MarshalMap(map[string]interface{}{
		"actorName":  actorName,
		"eventIndex": eventIndex,
	})
	assert.NoError(t, err)

	deleteInput := &dynamodb.DeleteItemInput{
		Key:       key,
		TableName: aws.String(tableName),
	}
	_, err = client.DeleteItem(context.Background(), deleteInput)
	assert.NoError(t, err)
}

func TestSnapshotStore_PersistSnapshot(t *testing.T) {
	tableName := "testSnapshotTable"

	client := InitializeDynamoDBClient()
	snapshotStore := p.NewSnapshotStore(client, tableName)

	actorName := "testActor"
	eventIndex := 1
	snapshotData := &p.Snapshot{Data: "testSnapshot"}

	snapshotStore.PersistSnapshot(actorName, eventIndex, snapshotData)

	// 保存されたスナップショットを取得して検証
	retrievedSnapshot, retrievedEventIndex, ok := snapshotStore.GetSnapshot(actorName)
	assert.True(t, ok)
	assert.Equal(t, eventIndex, retrievedEventIndex)

	snapshot, ok := retrievedSnapshot.(*p.Snapshot)
	assert.True(t, ok)

	assert.Equal(t, snapshotData.Data, snapshot.Data)

	// クリーンアップ
	key, err := attributevalue.MarshalMap(map[string]interface{}{
		"actorName":  actorName,
		"eventIndex": eventIndex,
	})
	assert.NoError(t, err)

	deleteInput := &dynamodb.DeleteItemInput{
		Key:       key,
		TableName: aws.String(tableName),
	}
	_, err = client.DeleteItem(context.Background(), deleteInput)
	assert.NoError(t, err)
}
