package example

import (
	"context"
	"github.com/corverroos/replay"
	"github.com/corverroos/replay/client"
	"github.com/corverroos/replay/db"
	"github.com/golang/protobuf/proto"
	"github.com/luno/jettison/jtest"
	"github.com/luno/jettison/log"
	"github.com/luno/reflex/rpatterns"
	"testing"
	"time"
)

//go:generate protoc --go_out=plugins=grpc:. ./example.proto

func TestExample(t *testing.T) {
	dbc := db.ConnectForTesting(t)
	cl := client.New(dbc)

	var b Backends
	replay.RegisterActivity(cl, rpatterns.MemCursorStore(), b, EnrichGreeting)
	replay.RegisterActivity(cl, rpatterns.MemCursorStore(), b, PrintGreeting)
	replay.RegisterWorkflow(cl, rpatterns.MemCursorStore(), GreetingWorkflow)

	err := cl.CreateRun(context.Background(), "GreetingWorkflow", &String{Value: "World"})
	jtest.RequireNil(t, err)

	time.Sleep(time.Minute)
}

type Backends struct{}

func GreetingWorkflow(ctx replay.RunContext, args proto.Message) error {
	for i := 0; i < 5; i++ {
		var err error
		args, err = ctx.ExecuteActivity(EnrichGreeting, args)
		if err != nil {
			return err
		}
	}

	_, err := ctx.ExecuteActivity(PrintGreeting, args)
	return err
}

func EnrichGreeting(ctx context.Context, b Backends, msg proto.Message) (proto.Message, error) {
	return &String{Value: "[" + msg.(*String).Value + "]"}, nil
}

func PrintGreeting(ctx context.Context, b Backends, msg proto.Message) (proto.Message, error) {
	log.Info(ctx, "Hello "+msg.(*String).Value)
	return &Empty{}, nil
}
