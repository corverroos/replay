package example

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/client/logical"
	"github.com/corverroos/replay/test"
	"github.com/luno/jettison/jtest"
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
		Client:       cl,
		completeChan: completeChan,
	}
	replay.RegisterWorkflow(testCtx(t), tcl, cstore, noop, replay.WithName(name))

	for i := 0; i < 5; i++ {
		run := fmt.Sprint(i)
		err := cl.RunWorkflow(ctx, name, run, new(Empty))
		jtest.RequireNil(t, err)
		require.Equal(t, run, <-completeChan)
	}
}

func TestIdenticalReplay(t *testing.T) {
	dbc := test.ConnectDB(t)
	cl := logical.New(dbc)
	ctx := context.Background()
	cstore := new(test.MemCursorStore)
	errsChan := make(chan string)
	tcl1 := &testClient{
		Client:       cl,
		activityErrs: map[string]error{"PrintGreeting": io.EOF},
		completeChan: make(chan string),
		errsChan:     errsChan,
	}

	workflow := makeWorkflow(5)
	name := "test_workflow"

	var b Backends
	replay.RegisterActivity(testCtx(t), cl, cstore, b, EnrichGreeting)
	replay.RegisterActivity(testCtx(t), cl, cstore, b, PrintGreeting)
	replay.RegisterWorkflow(testCtx(t), tcl1, cstore, workflow, replay.WithName(name)) // This workflow will block right before ctx.ExecActivity(PrintGreeting, name)

	err := cl.RunWorkflow(context.Background(), name, t.Name(), &String{Value: "World"})
	jtest.RequireNil(t, err)

	activity := <-errsChan
	require.Equal(t, activity, "PrintGreeting")

	el, err := cl.ListBootstrapEvents(ctx, name, t.Name())
	jtest.RequireNil(t, err)
	require.Len(t, el, 6)

	completeChan := make(chan string)
	tcl2 := &testClient{
		Client:       cl,
		completeChan: completeChan,
	}
	replay.RegisterWorkflow(testCtx(t), tcl2, cstore, workflow, replay.WithName(name)) // This workflow will bootstrap and continue after ctx.ExecActivity(PrintGreeting, name)
	run := <-completeChan
	require.Equal(t, t.Name(), run)

	el, err = cl.ListBootstrapEvents(ctx, name, run)
	jtest.RequireNil(t, err)
	require.Len(t, el, 7)
}

func makeWorkflow(n int) func(ctx replay.RunContext, name *String) {
	return func(ctx replay.RunContext, name *String) {
		for i := 0; i < n; i++ {
			name = ctx.ExecActivity(EnrichGreeting, name).(*String)
		}

		ctx.ExecActivity(PrintGreeting, name)
	}
}
