package main

import (
	"context"
	"errors"
	"log"
	"time"

	console "github.com/asynkron/goconsole"
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/persistence"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"google.golang.org/protobuf/types/known/timestamppb"
	"github.com/oklog/ulid/v2"
	a "github.com/tkhrk1010/protoactor-go-persistence-dynamodb/actor"
	p "github.com/tkhrk1010/protoactor-go-persistence-dynamodb/persistence"
)

func main() {
	log.Printf("start")

	//
	// 基本設定
	system := actor.NewActorSystem()
	client := p.InitializeDynamoDBClient()
	provider := p.NewProviderState(client)
	props := actor.PropsFromProducer(a.NewUserAccount, actor.WithReceiverMiddleware(persistence.Using(provider)))

	//
	// 通常ケース
	log.Printf("--- normal case ---")
	user1 := spawnUserAccount(system, props, "1")
	getEmail(system, user1)
	// ちゃんとするなら、直接Eventを渡さず、別の型で渡すべきか
	system.Root.Send(user1, &p.Event{Id: ulid.Make().String(), Type: "CreateUserAccount", Data: "event1", OccurredAt: timestamppb.Now()})
	system.Root.Send(user1, &p.Event{Id: ulid.Make().String(), Type: "CreateUserAccount", Data: "event2", OccurredAt: timestamppb.Now()})
	system.Root.Send(user1, &p.Event{Id: ulid.Make().String(), Type: "CreateUserAccount", Data: "event3", OccurredAt: timestamppb.Now()})
	system.Root.Send(user1, &p.Event{Id: ulid.Make().String(), Type: "CreateUserAccount", Data: "event4", OccurredAt: timestamppb.Now()})
	time.Sleep(3 * time.Second)

	//
	// userAccount1を一度停止し、復活させたときの挙動を見る
	log.Printf("--- stop and restart ---")
	getEmail(system, user1)
	system.Root.Stop(user1)
	time.Sleep(2 * time.Second)
	// event3までがsnapshotから復元される
	// その後replayされて、event4が追加される
	reUser1 := spawnUserAccount(system, props, "1")
	// 実際、最新のevent4が返ってくる
	getEmail(system, reUser1)
	time.Sleep(3 * time.Second)

	//
	// 同じactorNameのactorが生まれたらerrorになることを確認
	log.Printf("--- same actorName ---")
	sameUserAccount1 := spawnUserAccount(system, props, "1")
	// errorになっても、指定したpidは返ってくるらしい。既存のactorにmessageがいく。
	// 本番ではちゃんとrequest処理を失敗させないとaccount乗っ取りになりかねない
	system.Root.Send(sameUserAccount1, &p.Event{Data: "sameUser event1"})
	time.Sleep(3 * time.Second)

	_, _ = console.ReadLine()

	// DynamoDBのrecordを削除
	log.Print("deleting DynamoDB records...")
	deleteDynamoDBRecords(client, "journal", "userAccountActor-1")
	deleteDynamoDBRecords(client, "snapshot", "userAccountActor-1")
	log.Print("done")
}

func getEmail(system *actor.ActorSystem, pid *actor.PID) {
	res := system.Root.RequestFuture(pid, &a.GetEmailRequest{}, 5*time.Second)
	result, err := res.Result()
	if err != nil {
		log.Printf("main failed GetEmailRequest: %s", err.Error())
	}
	log.Printf("GetEmailRequest Response: %v", result)
}

func spawnUserAccount(system *actor.ActorSystem, props *actor.Props, id string) *actor.PID {
	pid, err := system.Root.SpawnNamed(props, "userAccountActor-"+id)
	// 登録ユーザーのメールアドレスが既に存在する場合はエラーを返す
	// メッセージ送信時に現在のバージョンを送信することで、永続化されたデータとの競合を防ぐことができるらしい
	// 詳しくはprotobufを参照してください
	// TODO: protobufを見て勉強する
	// Ref: github.com/ytake/protoactor-go-cqrs-example/internal/registration/create_user.go
	if errors.Is(err, actor.ErrNameExists) {
		log.Printf("user %s already exists", pid)
	}
	if err != nil {
		log.Printf("failed to spawn userAccountActor: %s", err.Error())
	}
	return pid
}

func deleteDynamoDBRecords(client *dynamodb.Client, tableName string, actorName string) error {
	// Scan operation parameters
	params := &dynamodb.ScanInput{
		TableName:        aws.String(tableName),
		FilterExpression: aws.String("actorName = :actorName"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":actorName": &types.AttributeValueMemberS{Value: actorName},
		},
	}

	// Scan the table to get all records with the specified actorName
	result, err := client.Scan(context.TODO(), params)
	if err != nil {
		return err
	}

	// Iterate over the scanned records and delete them one by one
	for _, item := range result.Items {
		deleteParams := &dynamodb.DeleteItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"actorName":  item["actorName"],
				"eventIndex": item["eventIndex"],
			},
		}

		_, err := client.DeleteItem(context.TODO(), deleteParams)
		if err != nil {
			return err
		}
	}

	return nil
}
