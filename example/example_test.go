package example

import (
	"context"
	"github.com/corverroos/replay"
	"github.com/corverroos/replay/client"
	"github.com/corverroos/replay/db"
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

func GreetingWorkflow(ctx replay.RunContext, name *String) {
	for i := 0; i < 5; i++ {
		name = ctx.ExecActivity(EnrichGreeting, name).(*String)
	}

	ctx.ExecActivity(PrintGreeting, name)
}

func EnrichGreeting(ctx context.Context, b Backends, msg *String) (*String, error) {
	return &String{Value: "[" + msg.Value + "]"}, nil
}

func PrintGreeting(ctx context.Context, b Backends, msg *String) (*Empty, error) {
	log.Info(ctx, "Hello "+msg.Value)
	return &Empty{}, nil
}
