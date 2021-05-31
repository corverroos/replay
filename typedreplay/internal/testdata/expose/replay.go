package expose

import (
	"context"
	"time"

	"github.com/luno/fate"
	"github.com/luno/reflex"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/typedreplay"
	"github.com/corverroos/replay/typedreplay/internal/testdata"
)

type Backends interface {
	ReplayClient() replay.Client
	Leader() Leader
	Cursor() Cursor
}

func ActivityA(ctx context.Context, b Backends, f fate.Fate, _ *testdata.Empty) (*testdata.String, error) {
	panic("implement me")
}

var _ = typedreplay.Namespace{
	Name: "example",
	Workflows: []typedreplay.Workflow{
		{
			Name:        "foo",
			Description: "This is an example foo workflow",
			Input:       new(testdata.Empty),
		},
	},
	Activities: []typedreplay.Activity{
		{
			Name:        "a",
			Description: "Processes stuff",
			Func:        ActivityA,
		},
	},
	ExposeRegisterFuncs: true,
}

//go:generate go run github.com/corverroos/replay/typedreplay/cmd/typedreplay -debug

func foo(f fooFlow, str *testdata.Empty) {}

func StartLoops(b Backends) {
	RegisterA(b.Leader(), b.ReplayClient(), b.Cursor(), b, replay.WithAwaitTimeout(time.Hour))
	RegisterFoo(b.Leader(), b.ReplayClient(), b.Cursor(), foo, replay.WithConsumerOpts())
}

// These are mocks of common dependencies.

type Leader func() context.Context

type Cursor interface {
	reflex.CursorStore
}
