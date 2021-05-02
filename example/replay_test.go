package example

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/luno/fate"
	"github.com/luno/jettison/jtest"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rsql"
	"github.com/stretchr/testify/require"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/internal"
	"github.com/corverroos/replay/test"
)

const (
	ns = "namespace"
	w  = "workflow"
	r  = "run"
)

func TestNoopWorkflow(t *testing.T) {
	ctx, cl, _, cstore := setup(t)

	noopFunc := func(ctx replay.RunContext, _ *Empty) {}

	replay.RegisterWorkflow(oneCtx(t), cl, cstore, ns, noopFunc, replay.WithName(w))

	for i := 0; i < 5; i++ {
		run := fmt.Sprint(i)
		ok, err := cl.RunWorkflow(ctx, ns, w, run, new(Empty))
		jtest.RequireNil(t, err)
		require.True(t, ok)
		require.Equal(t, runkey(run), <-cl.completeChan)
	}
}

func TestRestart(t *testing.T) {
	ctx, cl, _, cstore := setup(t)

	restart := func(ctx replay.RunContext, msg *Int) {
		// Note this just tests restart.
		key, _ := internal.DecodeKey(ctx.CreateEvent().ForeignID)
		if key.Iteration < 5 && msg.Value == int64(key.Iteration) {
			msg.Value++
			ctx.Restart(msg)
		}
	}

	replay.RegisterWorkflow(oneCtx(t), cl, cstore, ns, restart, replay.WithName(w))

	ok, err := cl.RunWorkflow(ctx, ns, w, r, new(Int))
	jtest.RequireNil(t, err)
	require.True(t, ok)

	require.Equal(t, internal.MinKey(ns, w, r, 5), <-cl.completeChan) // Restarts doesn't call Complete on client, only last exit does.

	for i := 0; i < 5; i++ {
		el, err := cl.Internal().ListBootstrapEvents(ctx, internal.MinKey(ns, w, r, i))
		jtest.RequireNil(t, err)
		require.Len(t, el, 2)
	}

	el, err := cl.Internal().ListBootstrapEvents(ctx, internal.MinKey(ns, w, r, 6))
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
	ctx, cl, _, cstore := setup(t)

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

	replay.RegisterWorkflow(oneCtx(t), cl, cstore, ns, workflow, replay.WithName(w))
	replay.RegisterActivity(func() context.Context { return context.Background() },
		cl, cstore, Backends{}, ns, activity, replay.WithName("act"))

	ok, err := cl.RunWorkflow(ctx, ns, w, r, new(Empty))
	jtest.RequireNil(t, err)
	require.True(t, ok)
	require.Equal(t, runkey(r), <-cl.completeChan)
	require.Equal(t, 2, i)

	ok, err = cl.RunWorkflow(ctx, ns, w, r, new(Empty))
	jtest.RequireNil(t, err)
	require.False(t, ok)
}

func TestIdenticalReplay(t *testing.T) {
	ctx, cl, _, cstore := setup(t)
	cl.blockActivity["PrintGreeting"] = io.EOF

	workflow := makeWorkflow(5)

	var b Backends
	replay.RegisterActivity(oneCtx(t), cl, cstore, b, ns, EnrichGreeting)
	replay.RegisterActivity(oneCtx(t), cl, cstore, b, ns, PrintGreeting)

	// This workflow will block right before ctx.ExecActivity(PrintGreeting, name)
	replay.RegisterWorkflow(oneCtx(t), cl, cstore, ns, workflow, replay.WithName(w))

	_, err := cl.RunWorkflow(context.Background(), ns, w, r, &String{Value: "World"})
	jtest.RequireNil(t, err)

	require.Equal(t, "PrintGreeting", <-cl.blockedChan)

	el, err := cl.Internal().ListBootstrapEvents(ctx, runkey(r))
	jtest.RequireNil(t, err)
	require.Len(t, el, 6)

	cl2 := cl.Clone()
	// This workflow will bootstrap and continue after ctx.ExecActivity(PrintGreeting, name)
	replay.RegisterWorkflow(oneCtx(t), cl2, cstore, ns, workflow, replay.WithName(w))
	require.Equal(t, runkey(r), <-cl2.completeChan)

	el, err = cl.Internal().ListBootstrapEvents(ctx, runkey(r))
	jtest.RequireNil(t, err)
	require.Len(t, el, 8)
}

func TestEarlyCompleteReplay(t *testing.T) {
	ctx, cl, _, cstore := setup(t)

	returnCh := make(chan struct{})
	calledCh := make(chan struct{})
	activity := func(context.Context, Backends, fate.Fate, *Empty) (*Empty, error) {
		calledCh <- struct{}{}
		<-returnCh
		return new(Empty), nil
	}

	workflow1 := func(ctx replay.RunContext, e *Empty) {
		ctx.ExecActivity(activity, e, replay.WithName(w))
	}

	var b Backends
	replay.RegisterActivity(oneCtx(t), cl, cstore, b, ns, activity, replay.WithName(w))

	// This workflow will block waiting for activity to respond and will then  be cancelled.
	ctx, cancel := context.WithCancel(ctx)
	replay.RegisterWorkflow(func() context.Context { return ctx }, cl, cstore, ns, workflow1, replay.WithName(w))

	_, err := cl.RunWorkflow(context.Background(), ns, w, r, new(Empty))
	jtest.RequireNil(t, err)

	<-calledCh
	cancel()

	// Ensure only 1 event, CreateRun
	el, err := cl.Internal().ListBootstrapEvents(context.Background(), runkey(r))
	jtest.RequireNil(t, err)
	require.Len(t, el, 1)

	// This workflow will replay the above run and just complete it immediately.
	noop := func(ctx replay.RunContext, e *Empty) {}
	replay.RegisterWorkflow(oneCtx(t), cl, new(test.MemCursorStore), ns, noop, replay.WithName(w))
	require.Equal(t, runkey(r), <-cl.completeChan)

	// Trigger above activity response (after new completion)
	returnCh <- struct{}{}

	// Wait for 3 events: CreateRun, Complete, Response
	require.Eventually(t, func() bool {
		el, err = cl.Internal().ListBootstrapEvents(context.Background(), runkey(r))
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
	_, err = cl.RunWorkflow(context.Background(), ns, w, "flush", new(Empty))
	jtest.RequireNil(t, err)
	require.Equal(t, runkey("flush"), <-cl.completeChan)

	el, err = cl.Internal().ListBootstrapEvents(context.Background(), runkey("flush"))
	jtest.RequireNil(t, err)
	require.Len(t, el, 2)
}

func TestBootstrapComplete(t *testing.T) {
	ctx, cl, _, cstore := setup(t)
	cl.blockActivity["PrintGreeting"] = io.EOF

	workflow := makeWorkflow(5)

	var b Backends
	replay.RegisterActivity(oneCtx(t), cl, cstore, b, ns, EnrichGreeting)
	replay.RegisterActivity(oneCtx(t), cl, cstore, b, ns, PrintGreeting)
	// This workflow will block right before ctx.ExecActivity(PrintGreeting, name)
	replay.RegisterWorkflow(oneCtx(t), cl, cstore, ns, workflow, replay.WithName(w))

	_, err := cl.RunWorkflow(context.Background(), ns, w, r, &String{Value: "World"})
	jtest.RequireNil(t, err)

	require.Equal(t, "PrintGreeting", <-cl.blockedChan)

	noop := func(ctx replay.RunContext, _ *String) {}
	cl2 := cl.Clone()
	// This workflow will complete during bootstrap
	replay.RegisterWorkflow(oneCtx(t), cl2, cstore, ns, noop, replay.WithName(w))
	require.Equal(t, runkey(r), <-cl2.completeChan)

	el, err := cl.Internal().ListBootstrapEvents(ctx, runkey(r))
	jtest.RequireNil(t, err)
	require.Len(t, el, 7) // No PrintGreeting response
	require.True(t, reflex.IsType(el[0].Type, internal.CreateRun))
	require.True(t, reflex.IsType(el[1].Type, internal.ActivityResponse))
	require.True(t, reflex.IsType(el[5].Type, internal.ActivityResponse))
	require.True(t, reflex.IsType(el[6].Type, internal.CompleteRun))
}

func TestOutOfOrderResponses(t *testing.T) {
	ctx, cl, _, cstore := setup(t)
	cl.blockActivity["PrintGreeting"] = io.EOF

	var b Backends
	replay.RegisterActivity(oneCtx(t), cl, cstore, b, ns, EnrichGreeting)

	// This workflow will block right before ctx.ExecActivity(PrintGreeting, name)
	replay.RegisterWorkflow(oneCtx(t), cl, cstore, ns, makeWorkflow(5), replay.WithName(w))

	_, err := cl.RunWorkflow(context.Background(), ns, w, r, &String{Value: "World"})
	jtest.RequireNil(t, err)

	require.Equal(t, "PrintGreeting", <-cl.blockedChan)

	cl2 := cl.Clone()

	// This workflow will error during bootstrap since the activity order changed
	errChan := make(chan struct{})
	replay.RegisterWorkflow(oneCtx(t), cl2, cstore, ns, makeWorkflow(1), replay.WithName(w),
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

	el, err := cl.Internal().ListBootstrapEvents(ctx, runkey(r))
	jtest.RequireNil(t, err)
	require.Len(t, el, 6)
	require.True(t, reflex.IsType(el[0].Type, internal.CreateRun))
	require.True(t, reflex.IsType(el[5].Type, internal.ActivityResponse))
}

func TestDuplicateSignals(t *testing.T) {
	ctx, cl, _, _ := setup(t)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 100; i++ {
		extID := fmt.Sprint(rand.Int63())
		for i := 0; i < 2; i++ {
			ok, err := cl.SignalRun(ctx, ns, w, r, testsig{}, new(Int), extID)
			jtest.RequireNil(t, err)
			require.Equal(t, i == 0, ok)
		}
	}
}

func TestCancelCtxBootstrap(t *testing.T) {
	ctx, cl, _, cstore := setup(t)
	run1 := "run1"
	run2 := "run2"

	var b Backends
	// Only register EnrichGreeting
	replay.RegisterActivity(oneCtx(t), cl, cstore, b, ns, EnrichGreeting)

	// Run 2 workflows
	ok, err := cl.RunWorkflow(context.Background(), ns, w, run1, &String{Value: "World"})
	jtest.RequireNil(t, err)
	require.True(t, ok)
	ok, err = cl.RunWorkflow(context.Background(), ns, w, run2, &String{Value: "World"})
	jtest.RequireNil(t, err)
	require.True(t, ok)

	// Start workflows, since PrintGreeting activity consumer not running, the workflows will block
	// awaiting PrintGreeting responses.
	tick := tickCtx{nextChan: make(chan struct{})}
	replay.RegisterWorkflow(tick.Get, cl, cstore, ns, makeWorkflow(1), replay.WithName(w))
	tick.Next()

	// Wait until PrintGreeting requests inserted
	sc, err := cl.Stream(ns)(ctx, "")
	jtest.RequireNil(t, err)
	for i := 0; i < 6; i++ {
		_, err := sc.Recv()
		jtest.RequireNil(t, err)
	}

	// Cancel the context, forcing reflex consumer to restart.
	tick.Next()

	// Now start PrintGreeting activity consumer to complete the runs.
	replay.RegisterActivity(oneCtx(t), cl, cstore, b, ns, PrintGreeting)
	require.Equal(t, runkey(run1), <-cl.completeChan)
	require.Equal(t, runkey(run2), <-cl.completeChan)

	el, err := cl.Internal().ListBootstrapEvents(ctx, runkey(run1))
	jtest.RequireNil(t, err)
	require.Len(t, el, 4)
	require.True(t, reflex.IsType(el[0].Type, internal.CreateRun))
	require.True(t, reflex.IsType(el[1].Type, internal.ActivityResponse))
	require.True(t, reflex.IsType(el[2].Type, internal.ActivityResponse))
	require.True(t, reflex.IsType(el[3].Type, internal.CompleteRun))
}

func TestAwaitTimeout(t *testing.T) {
	ctx, cl, _, cstore := setup(t)

	workflow := makeWorkflow(0)
	runs := 5

	// Run the workflows
	for i := 0; i < runs; i++ {
		ok, err := cl.RunWorkflow(context.Background(), ns, w, fmt.Sprintf("run%d", i), &String{Value: "World"})
		jtest.RequireNil(t, err)
		require.True(t, ok)
	}

	// We did not start activity consumers, the run goroutines should timeout when awaiting.
	replay.RegisterWorkflow(oneCtx(t), cl, cstore, ns, workflow, replay.WithName(w), replay.WithAwaitTimeout(time.Millisecond))

	// Wait until PrintGreeting requests inserted
	sc, err := cl.Stream(ns)(ctx, "")
	jtest.RequireNil(t, err)
	for i := 0; i < runs*2; i++ {
		_, err := sc.Recv()
		jtest.RequireNil(t, err)
	}

	// Now start PrintGreeting activity consumer to complete the runs.
	replay.RegisterActivity(oneCtx(t), cl, cstore, Backends{}, ns, PrintGreeting)
	for i := 0; i < runs; i++ {
		require.Equal(t, runkey(fmt.Sprintf("run%d", i)), <-cl.completeChan)
	}

	for i := 0; i < runs; i++ {
		el, err := cl.Internal().ListBootstrapEvents(ctx, runkey(fmt.Sprintf("run%d", i)))
		jtest.RequireNil(t, err)
		require.Len(t, el, 3)
		require.True(t, reflex.IsType(el[0].Type, internal.CreateRun))
		require.True(t, reflex.IsType(el[1].Type, internal.ActivityResponse))
		require.True(t, reflex.IsType(el[2].Type, internal.CompleteRun))
	}
}

func TestCtxCancel(t *testing.T) {
	ctx, cl, _, cstore := setup(t, rsql.WithEventsBackoff(time.Millisecond*10))

	workflow := makeWorkflow(10)
	runs := 10
	timeout := time.Millisecond * 100

	// Create the runs
	for i := 0; i < runs; i++ {
		ok, err := cl.RunWorkflow(ctx, ns, w, fmt.Sprint(i), &String{Value: "Hello"})
		jtest.RequireNil(t, err)
		require.True(t, ok)
	}

	// Start the activity consumers
	replay.RegisterActivity(oneCtx(t), cl, cstore, Backends{}, ns, EnrichGreeting)
	replay.RegisterActivity(oneCtx(t), cl, cstore, Backends{}, ns, PrintGreeting)

	//  fickleCtx will constantly cancel
	fickleCtx := func() context.Context {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		t.Cleanup(cancel)
		return ctx
	}

	replay.RegisterWorkflow(fickleCtx, cl, cstore, ns, workflow, replay.WithName(w))

	// Wait for all runs to complete
	for i := 0; i < runs; i++ {
		<-cl.completeChan
	}
}

func TestShards(t *testing.T) {
	ctx, cl, _, cstore := setup(t)

	workflow := makeWorkflow(2)
	runs := 20
	shards := 5

	// Create the runs
	for i := 0; i < runs; i++ {
		ok, err := cl.RunWorkflow(ctx, ns, w, fmt.Sprint(i), &String{Value: "Hello"})
		jtest.RequireNil(t, err)
		require.True(t, ok)
	}

	// Start the activity and workflow consumers
	for i := 0; i < shards; i++ {
		replay.RegisterActivity(oneCtx(t), cl, cstore, Backends{}, ns, EnrichGreeting, replay.WithShard(i, shards))
		replay.RegisterActivity(oneCtx(t), cl, cstore, Backends{}, ns, PrintGreeting, replay.WithShard(i, shards))
		replay.RegisterWorkflow(oneCtx(t), cl, cstore, ns, workflow, replay.WithName(w), replay.WithShard(i, shards))
	}

	// Wait for all runs to complete
	for i := 0; i < runs; i++ {
		<-cl.completeChan
	}
}

func TestReShard(t *testing.T) {
	ctx, cl, _, cstore := setup(t)

	workflow := makeWorkflow(2)
	runs := 10

	for j, shards := range []int{1, 4, 16} {
		t.Run(fmt.Sprint(shards), func(t *testing.T) {
			// Create all 10 runs
			for i := 0; i < runs; i++ {
				ok, err := cl.RunWorkflow(ctx, ns, w, fmt.Sprintf("%d_%d", shards, i), &String{Value: "Hello"})
				jtest.RequireNil(t, err)
				require.True(t, ok)
			}

			// Start the activity and workflow consumers (these will block/cancel at the end of the sub-test.
			for i := 0; i < shards; i++ {
				replay.RegisterActivity(oneCtx(t), cl, cstore, Backends{}, ns, EnrichGreeting, replay.WithShard(i, shards))
				replay.RegisterActivity(oneCtx(t), cl, cstore, Backends{}, ns, PrintGreeting, replay.WithShard(i, shards))
				replay.RegisterWorkflow(oneCtx(t), cl, cstore, ns, workflow, replay.WithName(w), replay.WithShard(i, shards))
			}

			// Wait for all runs to complete (also previous subtest runs since those runs are reprocessed)
			for i := 0; i < runs*(j+1); i++ {
				<-cl.completeChan
			}
		})
	}
}

func TestCustomShards(t *testing.T) {
	ctx, cl, _, cstore := setup(t)

	workflow := makeWorkflow(2)
	runs := 20
	shards := 5

	// Just mod runs over the total shards
	shardFunc := func(n int, run string) int {
		m, _ := strconv.Atoi(run)
		return m % n
	}

	// Create the runs
	for i := 0; i < runs; i++ {
		ok, err := cl.RunWorkflow(ctx, ns, w, fmt.Sprint(i), &String{Value: "Hello"})
		jtest.RequireNil(t, err)
		require.True(t, ok)
	}

	// Start only 1 of 5 shards
	replay.RegisterActivity(oneCtx(t), cl, cstore, Backends{}, ns, EnrichGreeting, replay.WithShard(0, shards), replay.WithShardFunc(shardFunc))
	replay.RegisterActivity(oneCtx(t), cl, cstore, Backends{}, ns, PrintGreeting, replay.WithShard(0, shards), replay.WithShardFunc(shardFunc))
	replay.RegisterWorkflow(oneCtx(t), cl, cstore, ns, workflow, replay.WithName(w), replay.WithShard(0, shards), replay.WithShardFunc(shardFunc))

	// Wait for 4 (5/20) runs to complete
	for i := 0; i < runs/shards; i++ {
		require.Equal(t, runkey(fmt.Sprint(i*shards)), <-cl.completeChan)
	}
}

func setup(t *testing.T, opts ...rsql.EventsOption) (context.Context, *testClient, *sql.DB, *test.MemCursorStore) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cl, dbc := test.Setup(t, opts...)

	tcl := &testClient{
		Client:        cl.Internal(),
		cl:            cl,
		completeChan:  make(chan string),
		blockedChan:   make(chan string),
		blockActivity: make(map[string]error),
	}

	return ctx, tcl, dbc, new(test.MemCursorStore)
}

// makeWorkflow returns a workflow for testing.
// It does n EnrichGreeting activities before calling PrintGreeting
func makeWorkflow(n int) func(ctx replay.RunContext, name *String) {
	return func(ctx replay.RunContext, name *String) {
		for i := 0; i < n; i++ {
			name = ctx.ExecActivity(EnrichGreeting, name).(*String)
		}
		ctx.ExecActivity(PrintGreeting, name)
	}
}

type tickCtx struct {
	prev     context.CancelFunc
	nextChan chan struct{}
}

func (t *tickCtx) Get() context.Context {
	<-t.nextChan
	var ctx context.Context
	ctx, t.prev = context.WithCancel(context.Background())
	return ctx
}

func (t *tickCtx) Next() {
	if t.prev != nil {
		t.prev()
	}
	t.nextChan <- struct{}{}
}

// runkey returns a common default run key used in testing.
func runkey(run string) string {
	return internal.MinKey(ns, w, run, 0)
}
