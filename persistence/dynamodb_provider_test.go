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
	"google.golang.org/protobuf/proto"

	p "github.com/tkhrk1010/protoactor-go-persistence-dynamodb/persistence"
)

func TestNewProviderState(t *testing.T) {
	client := InitializeDynamoDBClient()
	ps := p.NewProviderState(client)
	if ps == nil {
		t.Error("NewProviderState returned nil")
	}
}

func TestGetState(t *testing.T) {
	client := InitializeDynamoDBClient()
	ps := p.NewProviderState(client)
	state := ps.GetState()
	if state != ps {
		t.Error("GetState should return the same instance")
	}
}

// 呼び出せることだけ確認
func TestRestart(t *testing.T) {
	client := InitializeDynamoDBClient()
	ps := p.NewProviderState(client)
	ps.Restart()
}

func TestGetSnapshotInterval(t *testing.T) {
	client := InitializeDynamoDBClient()
	ps := p.NewProviderState(client)
	interval := ps.GetSnapshotInterval()
	if interval != 3 {
		t.Errorf("GetSnapshotInterval should return 3, got: %d", interval)
	}
}

func TestGetSnapshot(t *testing.T) {
	tableName := "snapshot"
	client := InitializeDynamoDBClient()
	ps := p.NewProviderState(client)
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
	retrievedSnapshot, retrievedEventIndex, ok := ps.GetSnapshot(actorName)
	assert.True(t, ok)
	assert.Equal(t, eventIndex, retrievedEventIndex)

	snapshot, ok := retrievedSnapshot.(*p.Snapshot)
	assert.True(t, ok)
	assert.Equal(t, snapshotData.Data, snapshot.Data)

	// 存在しないスナップショットを取得
	_, _, ok = ps.GetSnapshot("nonexistentActor")
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

func TestPersistSnapshot(t *testing.T) {
	tableName := "snapshot"
	client := InitializeDynamoDBClient()
	ps := p.NewProviderState(client)

	actorName := "testActor"
	eventIndex := 1
	snapshotData := &p.Snapshot{Data: "testSnapshot"}

	ps.PersistSnapshot(actorName, eventIndex, snapshotData)

	// 保存されたスナップショットを取得して検証
	retrievedSnapshot, retrievedEventIndex, ok := ps.GetSnapshot(actorName)
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

// 呼び出せることだけ確認
func TestDeleteSnapshots(t *testing.T) {
	client := InitializeDynamoDBClient()
	ps := p.NewProviderState(client)
	ps.DeleteSnapshots("testActor", 1)
}

func TestGetEvents(t *testing.T) {
	tableName := "journal"

	client := InitializeDynamoDBClient()
	ps := p.NewProviderState(client)

	// シードデータの準備
	seedData := []map[string]interface{}{
		{
			"actorName":  "testActor",
			"eventIndex": 1,
			"payload":    encodeEvent(&p.Event{Data: "event1"}),
		},
		{
			"actorName":  "testActor",
			"eventIndex": 2,
			"payload":    encodeEvent(&p.Event{Data: "event2"}),
		},
		{
			"actorName":  "testActor",
			"eventIndex": 3,
			"payload":    encodeEvent(&p.Event{Data: "event3"}),
		},
	}
	for _, item := range seedData {
		av, err := attributevalue.MarshalMap(item)
		assert.NoError(t, err)
		input := &dynamodb.PutItemInput{
			Item:      av,
			TableName: aws.String(tableName),
		}
		_, err = client.PutItem(context.Background(), input)
		assert.NoError(t, err)
	}

	actorName := "testActor"
	eventIndexStart := 1
	eventIndexEnd := 2

	var actualEvents []interface{}
	callback := func(e interface{}) {
		actualEvents = append(actualEvents, e)
	}
	ps.GetEvents(actorName, eventIndexStart, eventIndexEnd, callback)

	expectedEvents := []*p.Event{
		{Data: "event1"},
		{Data: "event2"},
	}
	assert.Equal(t, len(expectedEvents), len(actualEvents))
	for i, expected := range expectedEvents {
		actual := actualEvents[i].(*p.Event)
		assert.True(t, proto.Equal(expected, actual))
	}

	// クリーンアップ
	for _, item := range seedData {
		key, err := attributevalue.MarshalMap(map[string]interface{}{
			"actorName":  item["actorName"],
			"eventIndex": item["eventIndex"],
		})
		assert.NoError(t, err)
		input := &dynamodb.DeleteItemInput{
			Key:       key,
			TableName: aws.String(tableName),
		}
		_, err = client.DeleteItem(context.Background(), input)
		assert.NoError(t, err)
	}
}

func TestPersistEvent(t *testing.T) {
	tableName := "journal"

	client := InitializeDynamoDBClient()
	ps := p.NewProviderState(client)

	actorName := "testActor"
	eventIndex := 1
	eventData := &p.Event{Data: "testEvent"}

	ps.PersistEvent(actorName, eventIndex, eventData)

	// 保存したイベントの検証
	key, err := attributevalue.MarshalMap(map[string]interface{}{
		"actorName":  actorName,
		"eventIndex": eventIndex,
	})
	assert.NoError(t, err)
	input := &dynamodb.GetItemInput{
		Key:       key,
		TableName: aws.String(tableName),
	}
	result, err := client.GetItem(context.Background(), input)
	assert.NoError(t, err)

	var persistedEvent map[string]interface{}
	err = attributevalue.UnmarshalMap(result.Item, &persistedEvent)
	assert.NoError(t, err)

	assert.Equal(t, actorName, persistedEvent["actorName"])
	// Goでは、UnmarshalMapすると数値はfloat64になるため、intに変換する
	assert.Equal(t, eventIndex, int(persistedEvent["eventIndex"].(float64)))

	var persistedEventData p.Event
	err = proto.Unmarshal(persistedEvent["payload"].([]byte), &persistedEventData)
	assert.NoError(t, err)
	assert.Equal(t, eventData.Data, persistedEventData.Data)

	// クリーンアップ
	deleteInput := &dynamodb.DeleteItemInput{
		Key:       key,
		TableName: aws.String(tableName),
	}
	_, err = client.DeleteItem(context.Background(), deleteInput)
	assert.NoError(t, err)
}

// 呼び出せることだけ確認
func TestDeleteEvents(t *testing.T) {
	client := InitializeDynamoDBClient()
	ps := p.NewProviderState(client)
	ps.DeleteEvents("testActor", 1)
}
