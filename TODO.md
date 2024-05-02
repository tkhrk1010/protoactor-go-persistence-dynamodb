## 実装するもの
persistence/persistence_provider.go
```go
// Provider is the abstraction used for persistence
type Provider interface {
	GetState() ProviderState
}

// ProviderState is an object containing the implementation for the provider
type ProviderState interface {
	SnapshotStore
	EventStore

	Restart()
	GetSnapshotInterval() int
}

type SnapshotStore interface {
	GetSnapshot(actorName string) (snapshot interface{}, eventIndex int, ok bool)
	PersistSnapshot(actorName string, snapshotIndex int, snapshot proto.Message)
	DeleteSnapshots(actorName string, inclusiveToIndex int)
}

type EventStore interface {
	GetEvents(actorName string, eventIndexStart int, eventIndexEnd int, callback func(e interface{}))
	PersistEvent(actorName string, eventIndex int, event proto.Message)
	DeleteEvents(actorName string, inclusiveToIndex int)
}
```

## 設計
- [x] methodの洗い出し

## 実装
- [x] ProviderState(DynamoDBProvider)の実装
  - [x] ProviderState構造体の実装
  - [x] GetStateの実装
  - [x] NewProviderStateの実装
- [x] SnapshotStoreの実装
  - [x] SnapshotStore構造体(entry)の実装
  - [x] GetSnapshotの実装
  - [x] PersistSnapshotの実装
  - [x] DeleteSnapshotsの実装
- [x] EventStoreの実装
  - [x] EventStore構造体(entry)の実装
  - [x] GetEventsの実装
  - [x] PersistEventの実装
  - [x] DeleteEventsの実装
- [x] Restartの実装(空method)
- [x] GetSnapshotIntervalの実装
- [x] sample codeとしてのactorの実装

## 追加機能、修正
- [ ] DynamoDBに記録されたrecordをdeserializeして他のtableに移すscriptがほしい
- [ ] Event, Snapshotをちゃんとしたkey valueに変更
- [ ] table名を指定できるように
- [ ] snapshotIntervalを変数化
- [ ] DynamoDB clientの設定を変数化
- [ ] 