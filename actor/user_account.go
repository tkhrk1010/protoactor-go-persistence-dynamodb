package actor

import (
	"log"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/persistence"
	"github.com/oklog/ulid/v2"
	p "github.com/tkhrk1010/protoactor-go-persistence-dynamodb/persistence"
)

// Nameというfieldを持ってしまうと、MixinのNameと競合してしまい、エラーになるので注意
type UserAccount struct {
	persistence.Mixin
	id    string
	email string
}

type GetEmailRequest struct{}

type CreateUserRequest struct {
	email string
}

func (u *UserAccount) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *actor.Started:
		log.Printf("actor started: %v", ctx.Self())
		// actor.Context.Self().GetId()は、SpawnNamedで指定した名前が返却される
		// ここでは、userAccountActorはemailでuniqueなものとする
		u.email = ctx.Self().GetId()
	case *p.Event:
		log.Printf("Event message: %v", msg)
		// Persist all events received outside of recovery
		if !u.Recovering() {
			u.PersistReceive(msg)
		}
		ctx.Send(ctx.Self(), &CreateUserRequest{email: msg.Data})
	case *p.Snapshot:
		log.Printf("Snapshot message: %v", msg)
		u.email = msg.Data
	case *GetEmailRequest:
		log.Printf("GetEmailRequest message: %v", msg)
		ctx.Respond(u.getEmail())
	case *CreateUserRequest:
		log.Printf("CreateUserRequest message: %v", msg)
		// Set state to whatever message says
		// domain logicとして別にまとめられたらきれい
		u.id = ulid.Make().String()
		u.email = msg.email
	case *persistence.RequestSnapshot:
		log.Printf("RequestSnapshot message: %v", msg)
		u.PersistSnapshot(newSnapshot(u.email))
	case *persistence.ReplayComplete:
		log.Printf("ReplayComplete message: %v", msg)
	default:
		log.Printf("Unknown message: %v, message type: %T", msg, msg)
	}
}

// ここでIDを渡したい。が、actor.Actorのerrorになってしまう。返却値がinterfaceだから。
// あとでIDを設定すればいいのか？
func NewUserAccount() actor.Actor {
	return &UserAccount{}
}

func newSnapshot(data string) *p.Snapshot {
	return &p.Snapshot{Data: data}
}

func (ua *UserAccount) getEmail() string {
	return ua.email
}
