package testdata

import (
	"context"
	"fmt"
	"time"

	"github.com/luno/fate"
	"github.com/luno/reflex"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/typedreplay"
)

// This is how you use typedreplay to define and generate a typed replay workflow API.

// Step 1 define your proto messages.

//go:generate protoc --go_out=plugins=grpc:. ./testdata.proto

// Step 2 define your Backends; a dependency provider interface used by activities.

type Backends interface {
	ReplayClient() replay.Client
	Leader() Leader
	Cursor() Cursor
	Bar() interface{}
	Baz() string
}

// Step 3 define your activities.

func ActivityA(ctx context.Context, b Backends, f fate.Fate, _ *Empty) (*String, error) {
	panic("implement me")
}

func ActivityB(ctx context.Context, b Backends, f fate.Fate, _ *String) (*Empty, error) {
	panic("implement me")
}

// Step 4 define your typedreplay namespace and workflow(s).

var _ = typedreplay.Namespace{
	Name: "example",
	Workflows: []typedreplay.Workflow{
		{
			Name:        "foo",
			Description: "This is an example foo workflow",
			Input:       new(String),
			Signals: []typedreplay.Signal{
				{
					Name:        "s1",
					Description: "Doesn't actually do anything",
					Enum:        1,
					Message:     new(Empty),
				}, {
					Name:        "s2",
					Description: "Notifies that something has happened",
					Enum:        2,
					Message:     new(Int),
				},
			},
		},
		{
			Name:        "bar",
			Description: "Bar is bar",
			Input:       new(Empty),
		},
	},
	Activities: []typedreplay.Activity{
		{
			Name:        "a",
			Description: "Processes stuff",
			Func:        ActivityA,
		}, {
			Name:        "b",
			Description: "Notifies stuff",
			Func:        ActivityB,
		},
	},
}

// Step 5 generate code

//go:generate go run github.com/corverroos/replay/typedreplay/cmd/typedreplay -debug

// Step 6 define your actual workflow function
// using the generated typed API `fooFlow`.

func foo(f fooFlow, str *String) {
	e, ok := f.AwaitS1(time.Second)
	if !ok {
		e = new(Empty)
	}

	if str.Value == "" {
		i, ok := f.AwaitS2(time.Second)
		if !ok {
			str = &String{Value: fmt.Sprint(i.Value)}
		}
	}

	str = f.ActivityA(e)
	_ = f.ActivityB(str)
}

func bar(f barFlow, _ *Empty) {
	f.Sleep(time.Hour)
}

// Step 7 start replay loop which registers your workflow and activity consumers.

func StartLoops(b Backends) {
	startReplayLoops(
		b.Leader(),
		b.ReplayClient(),
		b.Cursor(),
		b,
		foo,
		bar)
}

// These are mocks of common dependencies.

type Leader func() context.Context

type Cursor interface {
	reflex.CursorStore
}
