package example

import (
	"context"
	"github.com/corverroos/replay"
	"github.com/corverroos/replay/client"
	"github.com/corverroos/replay/db"
	"github.com/golang/protobuf/proto"
	"github.com/luno/jettison/jtest"
	"github.com/luno/jettison/log"
	"github.com/stretchr/testify/require"
	"io"
	"sync"
	"testing"
)

//go:generate protoc --go_out=plugins=grpc:. ./example.proto

func TestExample(t *testing.T) {
	dbc := db.ConnectForTesting(t)
	cl := client.New(dbc)
	ctx := context.Background()
	cstore := new(memCursorStore)
	completeChan := make(chan string)
	tcl := &testClient{
		Client:       cl,
		completeChan: completeChan,
	}
	var b Backends

	replay.RegisterActivity(ctx, cl, cstore, b, EnrichGreeting)
	replay.RegisterActivity(ctx, cl, cstore, b, PrintGreeting)
	replay.RegisterWorkflow(ctx, tcl, cstore, GreetingWorkflow)

	err := cl.CreateRun(context.Background(), "GreetingWorkflow", &String{Value: "World"})
	jtest.RequireNil(t, err)

	<-completeChan
}

func TestExampleReplay(t *testing.T) {
	dbc := db.ConnectForTesting(t)
	db.FillGaps(dbc)
	cl := client.New(dbc)
	ctx := context.Background()
	cstore := new(memCursorStore)
    errsChan:= make(chan string)
	tcl1 := &testClient{
		Client:       cl,
		activityErrs: map[string]error{"PrintGreeting":io.EOF},
		completeChan: make(chan string),
		errsChan: errsChan,
	}

	var b Backends
	replay.RegisterActivity(ctx,cl, cstore, b, EnrichGreeting)
	replay.RegisterActivity(ctx,cl, cstore,  b, PrintGreeting)
	replay.RegisterWorkflow(ctx,tcl1, cstore, GreetingWorkflow) // This workflow will block right before ctx.ExecActivity(PrintGreeting, name)

	err := cl.CreateRun(context.Background(), "GreetingWorkflow", &String{Value: "World"})
	jtest.RequireNil(t, err)

	activity := <- errsChan
	require.Equal(t, activity, "PrintGreeting")

	completeChan:= make(chan string)
	tcl2 := &testClient{
		Client:       cl,
		completeChan: completeChan,
	}
	replay.RegisterWorkflow(ctx,tcl2, cstore, GreetingWorkflow) // This workflow will bootstrap and continue after ctx.ExecActivity(PrintGreeting, name)
	run := <-completeChan
	
	el, err := cl.ListBootstrapEvents(ctx, "GreetingWorkflow", run)
	jtest.RequireNil(t, err)
	require.Len(t, el, 7)
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

type testClient struct {
	replay.Client
	completeChan chan string
	errsChan chan string
	activityErrs map[string]error
}

func (c *testClient) CompleteRun(ctx context.Context, workflow, run string) error {
	defer func() {c.completeChan<-run}()

	return c.Client.CompleteRun(ctx, workflow, run)
}

func (c *testClient) RequestActivity(ctx context.Context, workflow, run string, activity string, index int, args proto.Message) error {
	if err, ok := c.activityErrs[activity]; ok {
		defer func() {
			c.errsChan <- activity
		}()
		return err
	}

	return c.Client.RequestActivity(ctx, workflow, run, activity, index, args)
}


type memCursorStore struct {
	sync.Mutex
	cursors map[string]string
}

func (m *memCursorStore) GetCursor(_ context.Context, consumerName string) (string, error) {
	m.Lock()
	defer m.Unlock()
	return m.cursors[consumerName], nil
}

func (m *memCursorStore) SetCursor(_ context.Context, consumerName string, cursor string) error {
	m.Lock()
	defer m.Unlock()
	if m.cursors == nil {
		m.cursors = make(map[string]string)
	}
	m.cursors[consumerName] = cursor
	return nil
}

func (m *memCursorStore) Flush(_ context.Context) error { return nil }
