package persistence

import (
	"fmt"
	"strconv"

	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)


type SnapshotStore struct {
	client    *dynamodb.Client
	table     string
}

func NewSnapshotStore(client *dynamodb.Client, table string) *SnapshotStore {
	return &SnapshotStore{
		client:    client,
		table:     table,
	}
}

func (s *SnapshotStore) GetSnapshot(actorName string) (snapshot interface{}, eventIndex int, ok bool) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(s.table),
		KeyConditionExpression: aws.String("actorName = :actorName"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":actorName": &types.AttributeValueMemberS{Value: actorName},
		},
		ScanIndexForward: aws.Bool(false), // 逆順にソート
		Limit:            aws.Int32(1),    // 最新の1レコードのみ取得
	}

	result, err := s.client.Query(context.Background(), input)
	if err != nil {
		return nil, 0, false
	}

	if len(result.Items) == 0 {
		return nil, 0, false
	}

	item := result.Items[0]

	var snapshotData map[string]interface{}
	err = attributevalue.UnmarshalMap(item, &snapshotData)
	if err != nil {
		return nil, 0, false
	}

	// Goでは、UnmarshalMapすると数値はfloat64になるが、取得できているかを型assertionで確認する
	eventIndexStr := fmt.Sprintf("%.0f", snapshotData["eventIndex"].(float64))
	eventIndex, err = strconv.Atoi(eventIndexStr)
	if err != nil {
		return nil, 0, false
	}

	snapshotBytes, ok := snapshotData["payload"].([]byte)
	if !ok {
		if snapshotData["payload"] == nil {
			// log something
			return nil, 0, false
		}
		return nil, 0, false
	}

	snapshot = &Snapshot{}
	err = proto.Unmarshal(snapshotBytes, snapshot.(*Snapshot))
	if err != nil {
		return nil, 0, false
	}

	return snapshot, eventIndex, true
}

func (s *SnapshotStore) PersistSnapshot(actorName string, eventIndex int, snapshot protoreflect.ProtoMessage) {
	snapshotBytes, err := proto.Marshal(snapshot)
	if err != nil {
		// TODO: error handling
		panic(err)
	}

	item := map[string]types.AttributeValue{
		"actorName": &types.AttributeValueMemberS{Value: actorName},
		"eventIndex": &types.AttributeValueMemberN{Value: strconv.Itoa(eventIndex)},
		"payload": &types.AttributeValueMemberB{Value: snapshotBytes},
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item:      item,
	}

	_, err = s.client.PutItem(context.Background(), input)
	if err != nil {
		// TODO: error handling
		panic(err)
	}
}

func (s *SnapshotStore) DeleteSnapshots(actorName string, inclusiveToIndex int) {}
