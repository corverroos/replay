package example

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/client/logical"
	"github.com/corverroos/replay/internal"
	"github.com/corverroos/replay/test"
	"github.com/golang/protobuf/proto"
	"github.com/luno/jettison/jtest"
	"github.com/luno/jettison/log"
)

//go:generate protoc --go_out=plugins=grpc:. ./example.proto

func TestExample(t *testing.T) {
	dbc := test.ConnectDB(t)
	cl := logical.New(dbc)
	cstore := new(test.MemCursorStore)
	completeChan := make(chan string)
	tcl := &testClient{
		Client:       cl,
		completeChan: completeChan,
	}
	var b Backends

	replay.RegisterActivity(testCtx(t), cl, cstore, b, EnrichGreeting)
	replay.RegisterActivity(testCtx(t), cl, cstore, b, PrintGreeting)
	replay.RegisterWorkflow(testCtx(t), tcl, cstore, GreetingWorkflow)

	err := cl.RunWorkflow(context.Background(), "GreetingWorkflow", t.Name(), &String{Value: "World"})
	jtest.RequireNil(t, err)

	<-completeChan
}

func TestExampleSleep(t *testing.T) {
	dbc := test.ConnectDB(t)
	cl := logical.New(dbc)
	ctx := context.Background()
	cstore := new(test.MemCursorStore)
	completeChan := make(chan string)
	tcl := &testClient{
		Client:       cl,
		completeChan: completeChan,
	}

	b := Backends{Replay: cl}
	test.RegisterNoopSleeps(ctx, cl, cstore, dbc)
	replay.RegisterActivity(testCtx(t), cl, cstore, b, PrintGreeting)
	replay.RegisterWorkflow(testCtx(t), tcl, cstore, SleepWorkflow)

	err := cl.RunWorkflow(ctx, "SleepWorkflow", t.Name(), new(Empty))
	jtest.RequireNil(t, err)

	<-completeChan
}

func TestExampleSignal(t *testing.T) {
	dbc := test.ConnectDB(t)
	cl := logical.New(dbc)
	ctx := context.Background()
	cstore := new(test.MemCursorStore)
	completeChan := make(chan string)
	tcl := &testClient{
		Client:       cl,
		completeChan: completeChan,
	}

	b := Backends{Replay: cl}

	test.RegisterNoSleepSignals(ctx, cl, cstore, dbc)
	replay.RegisterActivity(testCtx(t), cl, cstore, b, MaybeSignal)
	replay.RegisterActivity(testCtx(t), cl, cstore, b, PrintGreeting)
	replay.RegisterWorkflow(testCtx(t), tcl, cstore, SignalWorkflow)

	err := cl.RunWorkflow(ctx, "SignalWorkflow", "test", new(Empty))
	jtest.RequireNil(t, err)

	<-completeChan
}

func TestExampleGRPC(t *testing.T) {
	cl, _ := test.SetupForTesting(t)
	cstore := new(test.MemCursorStore)
	completeChan := make(chan string)
	tcl := &testClient{
		Client:       cl,
		completeChan: completeChan,
	}
	var b Backends

	replay.RegisterActivity(testCtx(t), cl, cstore, b, EnrichGreeting)
	replay.RegisterActivity(testCtx(t), cl, cstore, b, PrintGreeting)
	replay.RegisterWorkflow(testCtx(t), tcl, cstore, GreetingWorkflow)

	err := cl.RunWorkflow(context.Background(), "GreetingWorkflow", t.Name(), &String{Value: "World"})
	jtest.RequireNil(t, err)

	<-completeChan
}

type Backends struct {
	Replay replay.Client
}

func GreetingWorkflow(ctx replay.RunContext, name *String) {
	for i := 0; i < 5; i++ {
		name = ctx.ExecActivity(EnrichGreeting, name).(*String)
	}

	ctx.ExecActivity(PrintGreeting, name)
}

func SleepWorkflow(ctx replay.RunContext, _ *Empty) {
	for i := 0; i < 10; i++ {
		ctx.Sleep(time.Hour * 24 * 365)
	}

	ctx.ExecActivity(PrintGreeting, &String{Value: "sleepy head"})
}

func SignalWorkflow(ctx replay.RunContext, _ *Empty) {
	var sum int
	for i := 0; i < 10; i++ {
		ctx.ExecActivity(MaybeSignal, &Int{Value: int64(i)})
		res, ok := ctx.AwaitSignal(testsig{}, time.Second)
		if ok {
			sum += int(res.(*Int).Value)
		}
	}

	ctx.ExecActivity(PrintGreeting, &String{Value: fmt.Sprintf("sum %d", sum)})
}

func EnrichGreeting(ctx context.Context, b Backends, msg *String) (*String, error) {
	return &String{Value: "[" + msg.Value + "]"}, nil
}

func MaybeSignal(ctx context.Context, b Backends, i *Int) (*Empty, error) {
	if i.Value > 3 {
		return &Empty{}, nil
	}
	i.Value += 100

	err := b.Replay.SignalRun(ctx, "SignalWorkflow", "test", testsig{}, i, fmt.Sprint(i.Value))
	return &Empty{}, err
}

func PrintGreeting(ctx context.Context, b Backends, msg *String) (*Empty, error) {
	log.Info(ctx, "Hello "+msg.Value)
	return &Empty{}, nil
}

type testsig struct{}

func (s testsig) SignalType() int {
	return 0
}

func (s testsig) MessageType() proto.Message {
	return &Int{}
}

type testClient struct {
	replay.Client
	completeChan chan string
	errsChan     chan string
	activityErrs map[string]error
}

func (c *testClient) CompleteRun(ctx context.Context, workflow, run string) error {
	defer func() { c.completeChan <- run }()

	return c.Client.CompleteRun(ctx, workflow, run)
}

func (c *testClient) RequestActivity(ctx context.Context, key string, args proto.Message) error {
	k, err := internal.DecodeKey(key)
	if err != nil {
		return err
	}

	if err, ok := c.activityErrs[k.Activity]; ok {
		defer func() {
			c.errsChan <- k.Activity
		}()
		return err
	}

	return c.Client.RequestActivity(ctx, key, args)
}

func testCtx(t *testing.T) func() context.Context {
	var n int
	return func() context.Context {
		if n > 0 {
			time.Sleep(time.Hour)
		}
		n++
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(func() {
			cancel()
		})
		return ctx
	}
}
