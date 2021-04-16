package example

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/client/logical"
	"github.com/corverroos/replay/internal"
	"github.com/corverroos/replay/test"
	"github.com/luno/fate"
	"github.com/luno/jettison/jtest"
	"github.com/luno/reflex"
	"github.com/stretchr/testify/require"
)

func TestNoopWorkflow(t *testing.T) {
	dbc := test.ConnectDB(t)
	ctx := context.Background()
	cl := logical.New(dbc)
	cstore := new(test.MemCursorStore)

	name := "noop"
	noop := func(ctx replay.RunContext, _ *Empty) {}

	completeChan := make(chan string)
	tcl := &testClient{
		cl:           cl,
		Client:       cl.Internal(),
		completeChan: completeChan,
	}
	replay.RegisterWorkflow(testCtx(t), tcl, cstore, "ns", noop, replay.WithName(name))

	for i := 0; i < 5; i++ {
		run := fmt.Sprint(i)
		err := cl.RunWorkflow(ctx, "ns", name, run, new(Empty))
		jtest.RequireNil(t, err)
		require.Equal(t, internal.MinKey("ns", name, run, 0), <-completeChan)
	}
}

func TestRestart(t *testing.T) {
	dbc := test.ConnectDB(t)
	ctx := context.Background()
	cl := logical.New(dbc)
	cstore := new(test.MemCursorStore)

	name := "restart"
	restart := func(ctx replay.RunContext, msg *Int) {
		// Note this just tests restart.
		key, _ := internal.DecodeKey(ctx.CreateEvent().ForeignID)
		if key.Iteration < 5 && msg.Value == int64(key.Iteration) {
			msg.Value++
			ctx.Restart(msg)
		}
	}

	completeChan := make(chan string)
	tcl := &testClient{
		cl:           cl,
		Client:       cl.Internal(),
		completeChan: completeChan,
	}
	replay.RegisterWorkflow(testCtx(t), tcl, cstore, "ns", restart, replay.WithName(name))

	err := cl.RunWorkflow(ctx, "ns", name, t.Name(), new(Int))
	jtest.RequireNil(t, err)

	require.Equal(t, internal.MinKey("ns", name, t.Name(), 5), <-completeChan) // Restarts doesn't call Complete on client, only last exit does.

	for i := 0; i < 5; i++ {
		el, err := cl.Internal().ListBootstrapEvents(ctx, internal.MinKey("ns", name, t.Name(), i))
		jtest.RequireNil(t, err)
		require.Len(t, el, 2)
	}

	el, err := cl.Internal().ListBootstrapEvents(ctx, internal.MinKey("ns", name, t.Name(), 6))
	jtest.RequireNil(t, err)
	require.Len(t, el, 0)
}

func TestActivityFunc(t *testing.T) {
	require.PanicsWithError(t,
		"invalid activity function, input parameters not "+
			"context.Context, interface{}, fate.Fate, proto.Message: "+
			"func(context.Context, fate.Fate, example.Backends, *example.Empty) (*example.Empty, error)",
		func() {
			replay.RegisterActivity(nil, nil, nil, nil, "",
				func(context.Context, fate.Fate, Backends, *Empty) (*Empty, error) {
					return nil, nil
				})
		})

	require.PanicsWithError(t,
		"invalid activity function, output parameters not "+
			"proto.Message, error: "+
			"func(context.Context, example.Backends, fate.Fate, *example.Empty) (example.Backends, error)",
		func() {
			replay.RegisterActivity(nil, nil, nil, nil, "",
				func(context.Context, Backends, fate.Fate, *Empty) (Backends, error) {
					return Backends{}, nil
				})
		})
}

func TestWorkflowFunc(t *testing.T) {
	require.PanicsWithError(t,
		"invalid workflow function, input parameters not "+
			"replay.RunContext, proto.Message: "+
			"func(context.Context, *example.Empty)",
		func() {
			replay.RegisterWorkflow(nil, nil, nil, "",
				func(context.Context, *Empty) {})
		})

	require.PanicsWithError(t,
		"invalid workflow function, output parameters not empty: "+
			"func(replay.RunContext, *example.Empty) error",
		func() {
			replay.RegisterWorkflow(nil, nil, nil, "",
				func(replay.RunContext, *Empty) error {
					return nil
				})
		})
}

func TestActivityErr(t *testing.T) {
	dbc := test.ConnectDB(t)
	ctx := context.Background()
	cl := logical.New(dbc)
	cstore := new(test.MemCursorStore)

	var i int
	activity := func(context.Context, Backends, fate.Fate, *Empty) (*Empty, error) {
		i++
		if i > 1 {
			return new(Empty), nil
		}
		return nil, fate.ErrTempt
	}
	workflow := func(ctx replay.RunContext, _ *Empty) {
		ctx.ExecActivity(activity, new(Empty), replay.WithName("act"))
	}
	name := "test"

	completeChan := make(chan string)
	tcl := &testClient{
		cl:           cl,
		Client:       cl.Internal(),
		completeChan: completeChan,
	}
	replay.RegisterWorkflow(testCtx(t), tcl, cstore, "ns", workflow, replay.WithName(name))
	replay.RegisterActivity(func() context.Context { return context.Background() },
		tcl, cstore, Backends{}, "ns", activity, replay.WithName("act"))

	err := cl.RunWorkflow(ctx, "ns", name, t.Name(), new(Empty))
	jtest.RequireNil(t, err)
	require.Equal(t, internal.MinKey("ns", name, t.Name(), 0), <-completeChan)
	require.Equal(t, 2, i)
}

func TestIdenticalReplay(t *testing.T) {
	dbc := test.ConnectDB(t)
	cl := logical.New(dbc)
	ctx := context.Background()
	cstore := new(test.MemCursorStore)
	errsChan := make(chan string)
	tcl1 := &testClient{
		cl:           cl,
		Client:       cl.Internal(),
		activityErrs: map[string]error{"PrintGreeting": io.EOF},
		completeChan: make(chan string),
		errsChan:     errsChan,
	}

	workflow := makeWorkflow(5)
	name := "test_workflow"

	var b Backends
	replay.RegisterActivity(testCtx(t), cl, cstore, b, "ns", EnrichGreeting)
	replay.RegisterActivity(testCtx(t), cl, cstore, b, "ns", PrintGreeting)
	replay.RegisterWorkflow(testCtx(t), tcl1, cstore, "ns", workflow, replay.WithName(name)) // This workflow will block right before ctx.ExecActivity(PrintGreeting, name)

	err := cl.RunWorkflow(context.Background(), "ns", name, t.Name(), &String{Value: "World"})
	jtest.RequireNil(t, err)

	activity := <-errsChan
	require.Equal(t, "PrintGreeting", activity)

	el, err := cl.Internal().ListBootstrapEvents(ctx, internal.MinKey("ns", name, t.Name(), 0))
	jtest.RequireNil(t, err)
	require.Len(t, el, 6)

	completeChan := make(chan string)
	tcl2 := &testClient{
		cl:           cl,
		Client:       cl.Internal(),
		completeChan: completeChan,
	}
	replay.RegisterWorkflow(testCtx(t), tcl2, cstore, "ns", workflow, replay.WithName(name)) // This workflow will bootstrap and continue after ctx.ExecActivity(PrintGreeting, name)
	run := <-completeChan
	require.Equal(t, internal.MinKey("ns", name, t.Name(), 0), run)

	el, err = cl.Internal().ListBootstrapEvents(ctx, internal.MinKey("ns", name, t.Name(), 0))
	jtest.RequireNil(t, err)
	require.Len(t, el, 8)
}

func TestEarlyCompleteReplay(t *testing.T) {
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

	name := "name"

	returnCh := make(chan struct{})
	calledCh := make(chan struct{})
	activity := func(context.Context, Backends, fate.Fate, *Empty) (*Empty, error) {
		calledCh <- struct{}{}
		<-returnCh
		return new(Empty), nil
	}

	workflow1 := func(ctx replay.RunContext, e *Empty) {
		ctx.ExecActivity(activity, e, replay.WithName(name))
	}

	var b Backends
	replay.RegisterActivity(testCtx(t), cl, cstore, b, "ns", activity, replay.WithName(name))

	// This workflow will block waiting for activity to respond and will then  be cancelled.
	ctx, cancel := context.WithCancel(ctx)
	replay.RegisterWorkflow(func() context.Context { return ctx }, tcl, cstore, "ns", workflow1, replay.WithName(name))

	err := cl.RunWorkflow(context.Background(), "ns", name, t.Name(), new(Empty))
	jtest.RequireNil(t, err)

	<-calledCh
	cancel()

	// Ensure only 1 event, CreateRun
	el, err := cl.Internal().ListBootstrapEvents(context.Background(), internal.MinKey("ns", name, t.Name(), 0))
	jtest.RequireNil(t, err)
	require.Len(t, el, 1)

	// This workflow will replay the above run and just complete it immediately.
	noop := func(ctx replay.RunContext, e *Empty) {}
	replay.RegisterWorkflow(testCtx(t), tcl, new(test.MemCursorStore), "ns", noop, replay.WithName(name))
	require.Equal(t, internal.MinKey("ns", name, t.Name(), 0), <-completeChan)

	// Trigger above activity response (after new completion)
	returnCh <- struct{}{}

	// Wait for 3 events: CreateRun, Complete, Response
	require.Eventually(t, func() bool {
		el, err = cl.Internal().ListBootstrapEvents(context.Background(), internal.MinKey("ns", name, t.Name(), 0))
		jtest.RequireNil(t, err)
		if len(el) < 3 {
			return false
		}
		require.Len(t, el, 3)
		require.True(t, reflex.IsType(el[0].Type, internal.CreateRun))
		require.True(t, reflex.IsType(el[1].Type, internal.CompleteRun))
		require.True(t, reflex.IsType(el[2].Type, internal.ActivityResponse))
		return true
	}, time.Second, time.Millisecond*10)

	// Do another noop run, ensure it completes even though above had response after the complete.
	err = cl.RunWorkflow(context.Background(), "ns", name, "flush", new(Empty))
	jtest.RequireNil(t, err)
	require.Equal(t, internal.MinKey("ns", name, "flush", 0), <-completeChan)

	el, err = cl.Internal().ListBootstrapEvents(context.Background(), internal.MinKey("ns", name, "flush", 0))
	jtest.RequireNil(t, err)
	require.Len(t, el, 2)
}

func TestBootstrapComplete(t *testing.T) {
	dbc := test.ConnectDB(t)
	cl := logical.New(dbc)
	ctx := context.Background()
	cstore := new(test.MemCursorStore)
	errsChan := make(chan string)
	tcl1 := &testClient{
		cl:           cl,
		Client:       cl.Internal(),
		activityErrs: map[string]error{"PrintGreeting": io.EOF},
		completeChan: make(chan string),
		errsChan:     errsChan,
	}

	workflow := makeWorkflow(5)
	name := "test_workflow"

	var b Backends
	replay.RegisterActivity(testCtx(t), cl, cstore, b, "ns", EnrichGreeting)
	replay.RegisterActivity(testCtx(t), cl, cstore, b, "ns", PrintGreeting)
	// This workflow will block right before ctx.ExecActivity(PrintGreeting, name)
	replay.RegisterWorkflow(testCtx(t), tcl1, cstore, "ns", workflow, replay.WithName(name))

	err := cl.RunWorkflow(context.Background(), "ns", name, t.Name(), &String{Value: "World"})
	jtest.RequireNil(t, err)

	activity := <-errsChan
	require.Equal(t, activity, "PrintGreeting")

	noop := func(ctx replay.RunContext, _ *String) {}
	completeChan := make(chan string)
	tcl2 := &testClient{
		cl:           cl,
		Client:       cl.Internal(),
		completeChan: completeChan,
	}
	// This workflow will complete during bootstrap
	replay.RegisterWorkflow(testCtx(t), tcl2, cstore, "ns", noop, replay.WithName(name))
	run := <-completeChan
	require.Equal(t, internal.MinKey("ns", name, t.Name(), 0), run)

	el, err := cl.Internal().ListBootstrapEvents(ctx, internal.MinKey("ns", name, t.Name(), 0))
	jtest.RequireNil(t, err)
	require.Len(t, el, 7) // No PrintGreeting response
	require.True(t, reflex.IsType(el[0].Type, internal.CreateRun))
	require.True(t, reflex.IsType(el[1].Type, internal.ActivityResponse))
	require.True(t, reflex.IsType(el[5].Type, internal.ActivityResponse))
	require.True(t, reflex.IsType(el[6].Type, internal.CompleteRun))
}

func TestOutOfOrderResponses(t *testing.T) {
	dbc := test.ConnectDB(t)
	cl := logical.New(dbc)
	ctx := context.Background()
	cstore := new(test.MemCursorStore)
	errsChan := make(chan string)
	tcl1 := &testClient{
		cl:           cl,
		Client:       cl.Internal(),
		activityErrs: map[string]error{"PrintGreeting": io.EOF},
		completeChan: make(chan string),
		errsChan:     errsChan,
	}

	name := "test_workflow"

	var b Backends
	replay.RegisterActivity(testCtx(t), cl, cstore, b, "ns", EnrichGreeting)
	replay.RegisterActivity(testCtx(t), cl, cstore, b, "ns", PrintGreeting)

	// This workflow will block right before ctx.ExecActivity(PrintGreeting, name)
	replay.RegisterWorkflow(testCtx(t), tcl1, cstore, "ns", makeWorkflow(5), replay.WithName(name))

	err := cl.RunWorkflow(context.Background(), "ns", name, t.Name(), &String{Value: "World"})
	jtest.RequireNil(t, err)

	activity := <-errsChan
	require.Equal(t, activity, "PrintGreeting")

	completeChan := make(chan string)
	tcl2 := &testClient{
		cl:           cl,
		Client:       cl.Internal(),
		completeChan: completeChan,
	}

	// This workflow will error during bootstrap since the activity order changed
	errChan := make(chan struct{})
	replay.RegisterWorkflow(testCtx(t), tcl2, cstore, "ns", makeWorkflow(1), replay.WithName(name),
		replay.WithWorkflowMetrics(func(_, _ string) replay.Metrics {
			return replay.Metrics{
				IncErrors: func() {
					errChan <- struct{}{}
				},
				IncStart:    func() {},
				IncComplete: func(time.Duration) {},
			}
		}))
	<-errChan

	el, err := cl.Internal().ListBootstrapEvents(ctx, internal.MinKey("ns", name, t.Name(), 0))
	jtest.RequireNil(t, err)
	require.Len(t, el, 6)
	require.True(t, reflex.IsType(el[0].Type, internal.CreateRun))
	require.True(t, reflex.IsType(el[5].Type, internal.ActivityResponse))
}

func TestDuplicateSignals(t *testing.T) {
	dbc := test.ConnectDB(t)
	cl := logical.New(dbc)
	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 100; i++ {
		extID := fmt.Sprint(rand.Int63())
		for i := 0; i < 2; i++ {
			err := cl.SignalRun(ctx, "ns", "workflow", "run", testsig{}, new(Int), extID)
			if i == 0 {
				jtest.RequireNil(t, err)
			} else {
				jtest.Require(t, replay.ErrDuplicate, err)
			}
		}
	}
}

func makeWorkflow(n int) func(ctx replay.RunContext, name *String) {
	return func(ctx replay.RunContext, name *String) {
		for i := 0; i < n; i++ {
			name = ctx.ExecActivity(EnrichGreeting, name).(*String)
		}

		ctx.ExecActivity(PrintGreeting, name)
	}
}
