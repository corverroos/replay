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
	"github.com/luno/fate"
	"github.com/luno/jettison/jtest"
	"github.com/luno/jettison/log"
	"github.com/luno/reflex"
)

//go:generate protoc --go_out=plugins=grpc:. ./example.proto

func TestExample(t *testing.T) {
	dbc := test.ConnectDB(t)
	cl := logical.New(dbc)
	cstore := new(test.MemCursorStore)
	completeChan := make(chan string)
	tcl := &testClient{
		cl:           cl,
		Client:       cl.Internal(),
		completeChan: completeChan,
	}
	var b Backends

	replay.RegisterActivity(testCtx(t), cl, cstore, b, "ns", EnrichGreeting)
	replay.RegisterActivity(testCtx(t), cl, cstore, b, "ns", PrintGreeting)
	replay.RegisterWorkflow(testCtx(t), tcl, cstore, "ns", GreetingWorkflow)

	err := cl.RunWorkflow(context.Background(), "ns", "GreetingWorkflow", t.Name(), &String{Value: "World"})
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
		cl:           cl,
		Client:       cl.Internal(),
		completeChan: completeChan,
	}

	b := Backends{Replay: cl}
	test.RegisterNoopSleeps(ctx, cl, cstore, dbc)
	replay.RegisterActivity(testCtx(t), cl, cstore, b, "ns", PrintGreeting)
	replay.RegisterWorkflow(testCtx(t), tcl, cstore, "ns", SleepWorkflow)

	err := cl.RunWorkflow(ctx, "ns", "SleepWorkflow", t.Name(), new(Empty))
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
		cl:           cl,
		Client:       cl.Internal(),
		completeChan: completeChan,
	}

	b := Backends{Replay: cl}

	test.RegisterNoSleepSignals(ctx, cl, cstore, dbc)
	replay.RegisterActivity(testCtx(t), cl, cstore, b, "ns", MaybeSignal)
	replay.RegisterActivity(testCtx(t), cl, cstore, b, "ns", PrintGreeting)
	replay.RegisterWorkflow(testCtx(t), tcl, cstore, "ns", SignalWorkflow)

	err := cl.RunWorkflow(ctx, "ns", "SignalWorkflow", "test", new(Empty))
	jtest.RequireNil(t, err)

	<-completeChan
}

func TestExampleGRPC(t *testing.T) {
	cl, _ := test.SetupForTesting(t)
	cstore := new(test.MemCursorStore)
	completeChan := make(chan string)
	tcl := &testClient{
		cl:           cl,
		Client:       cl.Internal(),
		completeChan: completeChan,
	}
	var b Backends

	replay.RegisterActivity(testCtx(t), cl, cstore, b, "ns", EnrichGreeting)
	replay.RegisterActivity(testCtx(t), cl, cstore, b, "ns", PrintGreeting)
	replay.RegisterWorkflow(testCtx(t), tcl, cstore, "ns", GreetingWorkflow)

	err := cl.RunWorkflow(context.Background(), "ns", "GreetingWorkflow", t.Name(), &String{Value: "World"})
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

func EnrichGreeting(ctx context.Context, b Backends, f fate.Fate, msg *String) (*String, error) {
	return &String{Value: "[" + msg.Value + "]"}, nil
}

func MaybeSignal(ctx context.Context, b Backends, f fate.Fate, i *Int) (*Empty, error) {
	if i.Value > 3 {
		return &Empty{}, nil
	}
	i.Value += 100

	err := b.Replay.SignalRun(ctx, "ns", "SignalWorkflow", "ns", testsig{}, i, fmt.Sprint(i.Value))
	return &Empty{}, err
}

func PrintGreeting(ctx context.Context, b Backends, f fate.Fate, msg *String) (*Empty, error) {
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
	internal.Client
	cl           replay.Client
	completeChan chan string
	errsChan     chan string
	activityErrs map[string]error
}

func (c *testClient) RunWorkflow(ctx context.Context, namespace, workflow, run string, message proto.Message) error {
	return c.cl.RunWorkflow(ctx, namespace, workflow, run, message)
}

func (c *testClient) SignalRun(ctx context.Context, namespace, workflow, run string, s replay.Signal, message proto.Message, extID string) error {
	return c.cl.SignalRun(ctx, namespace, workflow, run, s, message, extID)
}

func (c *testClient) Stream(namespace string) reflex.StreamFunc {
	return c.cl.Stream(namespace)
}

func (c *testClient) Internal() internal.Client {
	return c
}

func (c *testClient) CompleteRun(ctx context.Context, namespace, workflow, run string, iter int) error {
	defer func() { c.completeChan <- run }()

	return c.Client.CompleteRun(ctx, namespace, workflow, run, iter)
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
