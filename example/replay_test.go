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

	"github.com/golang/protobuf/proto"
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
		awaitComplete(t, cl, ns, w, run)
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
	awaitCompletes(t, cl, ns, w, r, 5)

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
	awaitComplete(t, cl, ns, w, r)
	require.Equal(t, 2, i)

	ok, err = cl.RunWorkflow(ctx, ns, w, r, new(Empty))
	jtest.RequireNil(t, err)
	require.False(t, ok)
}

func TestIdenticalReplay(t *testing.T) {
	ctx, cl, _, cstore := setup(t)

	workflow := makeWorkflow(5)

	var b Backends
	replay.RegisterActivity(oneCtx(t), cl, cstore, b, ns, EnrichGreeting)
	replay.RegisterActivity(oneCtx(t), cl, cstore, b, ns, PrintGreeting)

	_, err := cl.RunWorkflow(context.Background(), ns, w, r, &String{Value: "World"})
	jtest.RequireNil(t, err)

	// This workflow will block right before ctx.ExecActivity(PrintGreeting, name)
	bcl := newBlockingClient(cl, "PrintGreeting")
	replay.RegisterWorkflow(oneCtx(t), bcl, cstore, ns, workflow, replay.WithName(w))
	require.Equal(t, "PrintGreeting", <-bcl.blockedChan)

	el, err := cl.Internal().ListBootstrapEvents(ctx, runkey(r))
	jtest.RequireNil(t, err)
	require.Len(t, el, 6)

	// This workflow will bootstrap and continue after ctx.ExecActivity(PrintGreeting, name)
	replay.RegisterWorkflow(oneCtx(t), cl, cstore, ns, workflow, replay.WithName(w))
	awaitComplete(t, cl, ns, w, r)

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

	// Ensure only 1 event, RunCreated
	el, err := cl.Internal().ListBootstrapEvents(context.Background(), runkey(r))
	jtest.RequireNil(t, err)
	require.Len(t, el, 1)

	// This workflow will replay the above run and just complete it immediately.
	noop := func(ctx replay.RunContext, e *Empty) {}
	replay.RegisterWorkflow(oneCtx(t), cl, new(test.MemCursorStore), ns, noop, replay.WithName(w))
	awaitComplete(t, cl, ns, w, r)

	// Trigger above activity response (after new completion)
	returnCh <- struct{}{}

	// Wait for 3 events: RunCreated, Complete, Response
	require.Eventually(t, func() bool {
		el, err = cl.Internal().ListBootstrapEvents(context.Background(), runkey(r))
		jtest.RequireNil(t, err)
		if len(el) < 3 {
			return false
		}
		require.Len(t, el, 3)
		require.True(t, reflex.IsType(el[0].Type, internal.RunCreated))
		require.True(t, reflex.IsType(el[1].Type, internal.RunCompleted))
		require.True(t, reflex.IsType(el[2].Type, internal.ActivityResponse))
		return true
	}, time.Second, time.Millisecond*10)

	// Do another noop run, ensure it completes even though above had response after the complete.
	_, err = cl.RunWorkflow(context.Background(), ns, w, "flush", new(Empty))
	jtest.RequireNil(t, err)
	awaitComplete(t, cl, ns, w, "flush")

	el, err = cl.Internal().ListBootstrapEvents(context.Background(), runkey("flush"))
	jtest.RequireNil(t, err)
	require.Len(t, el, 2)
}

func TestBootstrapComplete(t *testing.T) {
	ctx, cl, _, cstore := setup(t)
	bcl := newBlockingClient(cl, "PrintGreeting")

	workflow := makeWorkflow(5)

	var b Backends
	replay.RegisterActivity(oneCtx(t), bcl, cstore, b, ns, EnrichGreeting)
	replay.RegisterActivity(oneCtx(t), bcl, cstore, b, ns, PrintGreeting)
	// This workflow will block right before ctx.ExecActivity(PrintGreeting, name)
	replay.RegisterWorkflow(oneCtx(t), bcl, cstore, ns, workflow, replay.WithName(w))

	_, err := cl.RunWorkflow(context.Background(), ns, w, r, &String{Value: "World"})
	jtest.RequireNil(t, err)

	require.Equal(t, "PrintGreeting", <-bcl.blockedChan)

	noop := func(ctx replay.RunContext, _ *String) {}
	// This workflow will complete during bootstrap
	replay.RegisterWorkflow(oneCtx(t), cl, cstore, ns, noop, replay.WithName(w))
	awaitComplete(t, cl, ns, w, r)

	el, err := cl.Internal().ListBootstrapEvents(ctx, runkey(r))
	jtest.RequireNil(t, err)
	require.Len(t, el, 7) // No PrintGreeting response
	require.True(t, reflex.IsType(el[0].Type, internal.RunCreated))
	require.True(t, reflex.IsType(el[1].Type, internal.ActivityResponse))
	require.True(t, reflex.IsType(el[5].Type, internal.ActivityResponse))
	require.True(t, reflex.IsType(el[6].Type, internal.RunCompleted))
}

func TestOutOfOrderResponses(t *testing.T) {
	ctx, cl, _, cstore := setup(t)
	bcl := newBlockingClient(cl, "PrintGreeting")

	var b Backends
	replay.RegisterActivity(oneCtx(t), cl, cstore, b, ns, EnrichGreeting)

	// This workflow will block right before ctx.ExecActivity(PrintGreeting, name)
	replay.RegisterWorkflow(oneCtx(t), bcl, cstore, ns, makeWorkflow(5), replay.WithName(w))

	_, err := cl.RunWorkflow(context.Background(), ns, w, r, &String{Value: "World"})
	jtest.RequireNil(t, err)

	require.Equal(t, "PrintGreeting", <-bcl.blockedChan)

	// This workflow will error during bootstrap since the activity order changed
	errChan := make(chan struct{})
	replay.RegisterWorkflow(oneCtx(t), cl, cstore, ns, makeWorkflow(1), replay.WithName(w),
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
	require.True(t, reflex.IsType(el[0].Type, internal.RunCreated))
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

type testsig2 struct{}

func (s testsig2) SignalType() int {
	return 2
}

func (s testsig2) MessageType() proto.Message {
	return &Int{}
}

func TestUniqSignals(t *testing.T) {
	ctx, cl, _, _ := setup(t)

	ok, err := cl.SignalRun(ctx, ns, w, r, testsig{}, new(Int), "")
	jtest.RequireNil(t, err)
	require.True(t, ok)

	ok, err = cl.SignalRun(ctx, ns, w, r, testsig2{}, new(Int), "")
	jtest.RequireNil(t, err)
	require.True(t, ok)
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

	// Start run, since PrintGreeting activity consumer not running, the runs cannot
	// complete yet.
	tick := tickCtx{nextChan: make(chan struct{})}
	replay.RegisterWorkflow(tick.Get, cl, cstore, ns, makeWorkflow(1), replay.WithName(w))
	tick.Next()

	// Wait until runs active; until few activity requests inserted
	sc, err := cl.Stream(ns, w, "")(ctx, "")
	jtest.RequireNil(t, err)
	for i := 0; i < 4; i++ {
		_, err := sc.Recv()
		jtest.RequireNil(t, err)
	}

	// Cancel the context, forcing reflex consumer to restart.
	tick.Next()

	// Now start PrintGreeting activity consumer and wait for runs to complete.
	replay.RegisterActivity(oneCtx(t), cl, cstore, b, ns, PrintGreeting)
	awaitComplete(t, cl, ns, w, run1)
	awaitComplete(t, cl, ns, w, run2)

	el, err := cl.Internal().ListBootstrapEvents(ctx, runkey(run1))
	jtest.RequireNil(t, err)
	require.Len(t, el, 4)
	require.True(t, reflex.IsType(el[0].Type, internal.RunCreated))
	require.True(t, reflex.IsType(el[1].Type, internal.ActivityResponse))
	require.True(t, reflex.IsType(el[2].Type, internal.ActivityResponse))
	require.True(t, reflex.IsType(el[3].Type, internal.RunCompleted))
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
	// TODO(corver): This test flaps when the timeout below happens during RespondActivity
	//  which causes the workflow consumer to error and the test to timeout. This is expected
	//  behaviour and not a problem. The test is just not robust.
	replay.RegisterWorkflow(oneCtx(t), cl, cstore, ns, workflow, replay.WithName(w), replay.WithAwaitTimeout(10*time.Millisecond))

	// Wait until PrintGreeting requests inserted
	sc, err := cl.Stream(ns, "", "")(ctx, "")
	jtest.RequireNil(t, err)
	for i := 0; i < runs*2; i++ {
		_, err := sc.Recv()
		jtest.RequireNil(t, err)
	}

	// Now start PrintGreeting activity consumer to complete the runs.
	replay.RegisterActivity(oneCtx(t), cl, cstore, Backends{}, ns, PrintGreeting)
	for i := 0; i < runs; i++ {
		awaitComplete(t, cl, ns, w, fmt.Sprintf("run%d", i))
	}

	for i := 0; i < runs; i++ {
		el, err := cl.Internal().ListBootstrapEvents(ctx, runkey(fmt.Sprintf("run%d", i)))
		jtest.RequireNil(t, err)
		require.Len(t, el, 3)
		require.True(t, reflex.IsType(el[0].Type, internal.RunCreated))
		require.True(t, reflex.IsType(el[1].Type, internal.ActivityResponse))
		require.True(t, reflex.IsType(el[2].Type, internal.RunCompleted))
	}
}

func TestCtxCancel(t *testing.T) {
	ctx, cl, _, cstore := setup(t, rsql.WithEventsBackoff(time.Millisecond*10))

	workflow := makeWorkflow(10)
	runs := 5
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
	awaitCompletes(t, cl, ns, w, "", runs)
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
	awaitCompletes(t, cl, ns, w, "", runs)
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
			awaitCompletes(t, cl, ns, w, "", runs*(j+1))
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
	awaitCompletes(t, cl, ns, w, "", runs/shards)
}

func TestStream(t *testing.T) {
	ctx, cl, _, cstore := setup(t)

	// ns1
	replay.RegisterActivity(oneCtx(t), cl, cstore, Backends{}, "ns1", EnrichGreeting)
	replay.RegisterActivity(oneCtx(t), cl, cstore, Backends{}, "ns1", PrintGreeting)
	replay.RegisterWorkflow(oneCtx(t), cl, cstore, "ns1", makeWorkflow(1), replay.WithName("w1"))

	ok, err := cl.RunWorkflow(ctx, "ns1", "w1", "run1", &String{Value: "Hello"})
	jtest.RequireNil(t, err)
	require.True(t, ok)
	awaitCompletes(t, cl, "", "", "", 1)

	// ns2
	replay.RegisterActivity(oneCtx(t), cl, cstore, Backends{}, "ns2", PrintGreeting)
	replay.RegisterWorkflow(oneCtx(t), cl, cstore, "ns2", makeWorkflow(0), replay.WithName("w2"))

	ok, err = cl.RunWorkflow(ctx, "ns2", "w2", "run2", &String{Value: "Hello"})
	jtest.RequireNil(t, err)
	require.True(t, ok)
	awaitCompletes(t, cl, "", "", "", 2)

	cre := internal.RunCreated.String()
	com := internal.RunCompleted.String()
	out := internal.RunOutput.String()
	req := internal.ActivityRequest.String()
	res := internal.ActivityResponse.String()

	testStream := func(t *testing.T, namespace, workflow, run string, types ...string) {
		t.Helper()
		sc, err := cl.Stream(namespace, workflow, run)(ctx, "", reflex.WithStreamToHead())
		jtest.RequireNil(t, err)

		checkArgs := func(n, w, r string) {
			if namespace != "" {
				require.Equal(t, namespace, n)
			}
			if workflow != "" {
				require.Equal(t, workflow, w)
			}
			if run != "" {
				require.Equal(t, run, r)
			}
		}

		for i, typ := range types {
			e, err := sc.Recv()
			jtest.RequireNil(t, err)

			err = replay.Handle(e,
				replay.HandleRunCreated(func(n, w, r string, msg proto.Message) error {
					require.Equal(t, "Hello", msg.(*String).Value)
					checkArgs(n, w, r)
					require.Equal(t, typ, cre, i)
					return nil
				}),
				replay.HandleRunCompleted(func(n, w, r string) error {
					checkArgs(n, w, r)
					require.Equal(t, typ, com, i)
					return nil
				}),
				replay.HandleOutput(func(n, w, r, o string, msg proto.Message) error {
					require.Equal(t, "output", o)
					require.Equal(t, "output", msg.(*String).Value)
					checkArgs(n, w, r)
					require.Equal(t, typ, out, i)
					return nil
				}),
				replay.HandleActivityRequest(func(n, w, r, a string, msg proto.Message) error {
					_, ok := msg.(*String)
					require.True(t, ok, i)
					checkArgs(n, w, r)
					require.Equal(t, typ, req, i)
					return nil
				}),
				replay.HandleActivityResponse(func(n, w, r, a string, msg proto.Message) error {
					if a == "EnrichGreeting" {
						_, ok := msg.(*String)
						require.True(t, ok, i)
					} else /* a == "PrintGreeting" */ {
						_, ok := msg.(*Empty)
						require.True(t, ok, i)
					}
					checkArgs(n, w, r)
					require.Equal(t, typ, res, i)
					return nil
				}),
			)
			jtest.RequireNil(t, err)
		}

		_, err = sc.Recv()
		require.Equal(t, err, reflex.ErrHeadReached)
	}

	tl1 := []string{cre, req, res, req, res, out, com}
	testStream(t, "ns1", "w1", "run1", tl1...)
	testStream(t, "ns1", "w1", "", tl1...)
	testStream(t, "ns1", "", "", tl1...)

	tl2 := []string{cre, req, res, out, com}
	testStream(t, "ns2", "w2", "run2", tl2...)

	testStream(t, "", "", "", append(tl1, tl2...)...)
}

func awaitComplete(t *testing.T, cl replay.Client, namespace, workflow, run string) {
	awaitCompletes(t, cl, namespace, workflow, run, 1)
}

func awaitCompletes(t *testing.T, cl replay.Client, namespace, workflow, run string, count int) {
	sc, err := cl.Stream(namespace, workflow, run)(context.Background(), "")
	jtest.RequireNil(t, err)

	for {
		e, err := sc.Recv()
		if err == context.DeadlineExceeded {
			require.Fail(t, "timeout: complete(s) not received", "remaining=%d", count)
		}
		jtest.RequireNil(t, err)

		err = replay.Handle(e, replay.HandleRunCompleted(func(n, w, r string) error {
			if namespace != "" && namespace != n {
				require.Fail(t, "unexpected namespace")
			}
			if workflow != "" && workflow != w {
				require.Fail(t, "unexpected workflow")
			}
			if run != "" && run != r {
				require.Fail(t, "unexpected run")
			}

			if count--; count == 0 {
				return io.EOF
			}

			return nil
		}))
		if err == io.EOF {
			return
		}
		jtest.RequireNil(t, err)
	}
}

func setup(t *testing.T, opts ...rsql.EventsOption) (context.Context, replay.Client, *sql.DB, *test.MemCursorStore) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cl, dbc := test.Setup(t, opts...)

	return ctx, cl, dbc, new(test.MemCursorStore)
}

// makeWorkflow returns a workflow for testing.
// It does n EnrichGreeting activities before calling PrintGreeting
func makeWorkflow(n int) func(ctx replay.RunContext, name *String) {
	return func(ctx replay.RunContext, name *String) {
		for i := 0; i < n; i++ {
			name = ctx.ExecActivity(EnrichGreeting, name).(*String)
		}
		ctx.ExecActivity(PrintGreeting, name)
		ctx.EmitOutput("output", &String{Value: "output"})
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

func newBlockingClient(cl replay.Client, activities ...string) *blockingClient {
	m := make(map[string]bool)
	for _, activity := range activities {
		m[activity] = true
	}
	return &blockingClient{
		Client:        cl.Internal(),
		cl:            cl,
		blockedChan:   make(chan string),
		blockActivity: m,
	}
}

// blockingClient wraps a replay client blocking runs from doing a
// specific activity request, blockchan is notified on each block.
type blockingClient struct {
	internal.Client
	cl            replay.Client
	blockedChan   chan string
	blockActivity map[string]bool
}

func (c *blockingClient) RunWorkflow(ctx context.Context, namespace, workflow, run string, message proto.Message) (bool, error) {
	return c.cl.RunWorkflow(ctx, namespace, workflow, run, message)
}

func (c *blockingClient) SignalRun(ctx context.Context, namespace, workflow, run string, s replay.Signal, message proto.Message, extID string) (bool, error) {
	return c.cl.SignalRun(ctx, namespace, workflow, run, s, message, extID)
}

func (c *blockingClient) Stream(namespace, workflow, run string) reflex.StreamFunc {
	return c.cl.Stream(namespace, workflow, run)
}

func (c *blockingClient) Internal() internal.Client {
	return c
}

func (c *blockingClient) RequestActivity(ctx context.Context, key string, args proto.Message) error {
	k, err := internal.DecodeKey(key)
	if err != nil {
		return err
	}

	if ok := c.blockActivity[k.Target]; ok {
		c.blockedChan <- k.Target
		time.Sleep(time.Hour)
	}

	return c.Client.RequestActivity(ctx, key, args)
}
