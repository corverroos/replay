package testdata

// Code generated by typedreplay. DO NOT EDIT.

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/luno/reflex"

	"github.com/corverroos/replay"
	// TODO(corver): Support importing other packages.
)

const (
	_ns     = "example"
	_wFoo   = "foo"
	_oFooO1 = "o1"
	_oFooO2 = "o2"
	_wBar   = "bar"
	_oBarO3 = "o3"
	_aA     = "a"
	_aB     = "b"
)

type fooSignal int

const (
	_sFooS1 fooSignal = 1
	_sFooS2 fooSignal = 2
)

var fooSignalMessages = map[fooSignal]proto.Message{
	_sFooS1: new(Empty),
	_sFooS2: new(Int),
}

func (s fooSignal) SignalType() int {
	return int(s)
}

func (s fooSignal) MessageType() proto.Message {
	return fooSignalMessages[s]
}

// SignalFooS1 provides a typed API for signalling a foo workflow run with signal s1.
// It returns true on success or false on duplicate calls or an error.
func SignalFooS1(ctx context.Context, cl replay.Client, run string, message *Empty, extID string) (bool, error) {
	return cl.SignalRun(ctx, _ns, _wFoo, run, _sFooS1, message, extID)
}

// SignalFooS2 provides a typed API for signalling a foo workflow run with signal s2.
// It returns true on success or false on duplicate calls or an error.
func SignalFooS2(ctx context.Context, cl replay.Client, run string, message *Int, extID string) (bool, error) {
	return cl.SignalRun(ctx, _ns, _wFoo, run, _sFooS2, message, extID)
}

// RunFoo provides a type API for running the foo workflow.
// It returns true on success or false on duplicate calls or an error.
func RunFoo(ctx context.Context, cl replay.Client, run string, message *String) (bool, error) {
	return cl.RunWorkflow(ctx, _ns, _wFoo, run, message)
}

// RunBar provides a type API for running the bar workflow.
// It returns true on success or false on duplicate calls or an error.
func RunBar(ctx context.Context, cl replay.Client, run string, message *Empty) (bool, error) {
	return cl.RunWorkflow(ctx, _ns, _wBar, run, message)
}

// startReplayLoops registers the workflow and activities for typed workflow functions.
func startReplayLoops(getCtx func() context.Context, cl replay.Client, cstore reflex.CursorStore, b Backends,
	foo func(fooFlow, *String), bar func(barFlow, *Empty)) {

	fooFunc := func(ctx replay.RunContext, message *String) {
		foo(fooFlowImpl{ctx}, message)
	}
	replay.RegisterWorkflow(getCtx, cl, cstore, _ns, fooFunc, replay.WithName(_wFoo))

	barFunc := func(ctx replay.RunContext, message *Empty) {
		bar(barFlowImpl{ctx}, message)
	}
	replay.RegisterWorkflow(getCtx, cl, cstore, _ns, barFunc, replay.WithName(_wBar))

	replay.RegisterActivity(getCtx, cl, cstore, b, _ns, ActivityA, replay.WithName(_aA))
	replay.RegisterActivity(getCtx, cl, cstore, b, _ns, ActivityB, replay.WithName(_aB))
}

// fooFlow defines a typed API for the foo workflow.
type fooFlow interface {

	// Sleep blocks for at least d duration.
	// Note that replay sleeps aren't very accurate and
	// a few seconds is the practical minimum.
	Sleep(d time.Duration)

	// CreateEvent returns the reflex event that started the run iteration (type is internal.CreateRun).
	// The event timestamp could be used to reason about run age.
	CreateEvent() *reflex.Event

	// LastEvent returns the latest reflex event (type is either internal.CreateRun or internal.ActivityResponse).
	// The event timestamp could be used to reason about run age.
	LastEvent() *reflex.Event

	// Run returns the run name/identifier.
	Run() string

	// Restart completes the current run iteration and starts a new run iteration with the provided input message.
	// The run state is effectively reset. This is handy to mitigate bootstrap load for long running tasks.
	// It also allows updating the activity logic/ordering.
	Restart(message *String)

	// ActivityA results in the ActivityA activity being called asynchronously
	// with the provided parameter and returns the response once available.
	ActivityA(message *Empty) *String

	// ActivityB results in the ActivityB activity being called asynchronously
	// with the provided parameter and returns the response once available.
	ActivityB(message *String) *Empty

	// AwaitS1 blocks and returns true when a s1 signal is/was
	// received for this run. If no signal is/was received it returns false after d duration.
	AwaitS1(d time.Duration) (*Empty, bool)

	// AwaitS2 blocks and returns true when a s2 signal is/was
	// received for this run. If no signal is/was received it returns false after d duration.
	AwaitS2(d time.Duration) (*Int, bool)

	// EmitO1 stores the o1 output in the event log and returns when successful.
	EmitO1(message *Int)

	// EmitO2 stores the o2 output in the event log and returns when successful.
	EmitO2(message *String)
}

type fooFlowImpl struct {
	ctx replay.RunContext
}

func (f fooFlowImpl) Sleep(d time.Duration) {
	f.ctx.Sleep(d)
}

func (f fooFlowImpl) CreateEvent() *reflex.Event {
	return f.ctx.CreateEvent()
}

func (f fooFlowImpl) LastEvent() *reflex.Event {
	return f.ctx.LastEvent()
}

func (f fooFlowImpl) Run() string {
	return f.ctx.Run()
}

func (f fooFlowImpl) Restart(message *String) {
	f.ctx.Restart(message)
}

func (f fooFlowImpl) ActivityA(message *Empty) *String {
	return f.ctx.ExecActivity(ActivityA, message, replay.WithName(_aA)).(*String)
}

func (f fooFlowImpl) ActivityB(message *String) *Empty {
	return f.ctx.ExecActivity(ActivityB, message, replay.WithName(_aB)).(*Empty)
}

func (f fooFlowImpl) AwaitS1(d time.Duration) (*Empty, bool) {
	res, ok := f.ctx.AwaitSignal(_sFooS1, d)
	if !ok {
		return nil, false
	}
	return res.(*Empty), true
}

func (f fooFlowImpl) AwaitS2(d time.Duration) (*Int, bool) {
	res, ok := f.ctx.AwaitSignal(_sFooS2, d)
	if !ok {
		return nil, false
	}
	return res.(*Int), true
}

func (f fooFlowImpl) EmitO1(message *Int) {
	f.ctx.EmitOutput(_oFooO1, message)
}

func (f fooFlowImpl) EmitO2(message *String) {
	f.ctx.EmitOutput(_oFooO2, message)
}

// StreamFoo returns a stream of replay events for the foo workflow and an optional run.
func StreamFoo(cl replay.Client, run string) reflex.StreamFunc {
	return cl.Stream(_ns, _wFoo, run)
}

// HandleO1 calls fn and returns true if the event is a o1 output.
// Use StreamFoo to provide the events.
func HandleO1(e *reflex.Event, fn func(run string, message *Int) error) (bool, error) {
	var ok bool
	err := replay.Handle(e,
		replay.HandleSkip(func(namespace, workflow, run string) bool {
			return namespace != _ns || workflow != _wFoo
		}),
		replay.HandleOutput(func(namespace, workflow, run string, output string, message proto.Message) error {
			if output != _oFooO1 {
				return nil
			}
			ok = true
			return fn(run, message.(*Int))
		}))
	if err != nil {
		return false, err
	}
	return ok, nil
}

// HandleO2 calls fn and returns true if the event is a o2 output.
// Use StreamFoo to provide the events.
func HandleO2(e *reflex.Event, fn func(run string, message *String) error) (bool, error) {
	var ok bool
	err := replay.Handle(e,
		replay.HandleSkip(func(namespace, workflow, run string) bool {
			return namespace != _ns || workflow != _wFoo
		}),
		replay.HandleOutput(func(namespace, workflow, run string, output string, message proto.Message) error {
			if output != _oFooO2 {
				return nil
			}
			ok = true
			return fn(run, message.(*String))
		}))
	if err != nil {
		return false, err
	}
	return ok, nil
}

// barFlow defines a typed API for the bar workflow.
type barFlow interface {

	// Sleep blocks for at least d duration.
	// Note that replay sleeps aren't very accurate and
	// a few seconds is the practical minimum.
	Sleep(d time.Duration)

	// CreateEvent returns the reflex event that started the run iteration (type is internal.CreateRun).
	// The event timestamp could be used to reason about run age.
	CreateEvent() *reflex.Event

	// LastEvent returns the latest reflex event (type is either internal.CreateRun or internal.ActivityResponse).
	// The event timestamp could be used to reason about run age.
	LastEvent() *reflex.Event

	// Run returns the run name/identifier.
	Run() string

	// Restart completes the current run iteration and starts a new run iteration with the provided input message.
	// The run state is effectively reset. This is handy to mitigate bootstrap load for long running tasks.
	// It also allows updating the activity logic/ordering.
	Restart(message *Empty)

	// ActivityA results in the ActivityA activity being called asynchronously
	// with the provided parameter and returns the response once available.
	ActivityA(message *Empty) *String

	// ActivityB results in the ActivityB activity being called asynchronously
	// with the provided parameter and returns the response once available.
	ActivityB(message *String) *Empty

	// EmitO3 stores the o3 output in the event log and returns when successful.
	EmitO3(message *Int)
}

type barFlowImpl struct {
	ctx replay.RunContext
}

func (f barFlowImpl) Sleep(d time.Duration) {
	f.ctx.Sleep(d)
}

func (f barFlowImpl) CreateEvent() *reflex.Event {
	return f.ctx.CreateEvent()
}

func (f barFlowImpl) LastEvent() *reflex.Event {
	return f.ctx.LastEvent()
}

func (f barFlowImpl) Run() string {
	return f.ctx.Run()
}

func (f barFlowImpl) Restart(message *Empty) {
	f.ctx.Restart(message)
}

func (f barFlowImpl) ActivityA(message *Empty) *String {
	return f.ctx.ExecActivity(ActivityA, message, replay.WithName(_aA)).(*String)
}

func (f barFlowImpl) ActivityB(message *String) *Empty {
	return f.ctx.ExecActivity(ActivityB, message, replay.WithName(_aB)).(*Empty)
}

func (f barFlowImpl) EmitO3(message *Int) {
	f.ctx.EmitOutput(_oBarO3, message)
}

// StreamBar returns a stream of replay events for the bar workflow and an optional run.
func StreamBar(cl replay.Client, run string) reflex.StreamFunc {
	return cl.Stream(_ns, _wBar, run)
}

// HandleO3 calls fn and returns true if the event is a o3 output.
// Use StreamBar to provide the events.
func HandleO3(e *reflex.Event, fn func(run string, message *Int) error) (bool, error) {
	var ok bool
	err := replay.Handle(e,
		replay.HandleSkip(func(namespace, workflow, run string) bool {
			return namespace != _ns || workflow != _wBar
		}),
		replay.HandleOutput(func(namespace, workflow, run string, output string, message proto.Message) error {
			if output != _oBarO3 {
				return nil
			}
			ok = true
			return fn(run, message.(*Int))
		}))
	if err != nil {
		return false, err
	}
	return ok, nil
}