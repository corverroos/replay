package {{.PackageName}}

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
	_ns     = "{{.Name}}"
	{{- range .Workflows}} {{$workflowPascal := .Pascal}}
	_w{{.Pascal}} = "{{.Name}}"
	  {{- range .Outputs}}
    _o{{$workflowPascal}}{{.Pascal}} = "{{.Name}}"
      {{- end}}
	{{- end}}
	{{- range .Activities}}
    _a{{.Pascal}} = "{{.Name}}"
    {{- end}}
)

{{range .Workflows}} {{$workflowName := .Name}} {{$workflowCamel := .Camel}} {{$workflowPascal := .Pascal}}
{{if .Signals}}
type {{$workflowCamel}}Signal int

const (
	{{- range $i, $s := .Signals}}
	_s{{$workflowPascal}}{{$s.Pascal}} {{$workflowCamel}}Signal = {{inc $i}}
	{{- end}}
)

var {{$workflowCamel}}SignalMessages = map[{{$workflowCamel}}Signal]proto.Message{
    {{- range .Signals}}
	_s{{$workflowPascal}}{{.Pascal}}: new({{.Message}}),
	{{- end}}
}

func (s {{$workflowCamel}}Signal) SignalType() int {
	return int(s)
}

func (s {{$workflowCamel}}Signal) MessageType() proto.Message {
	return {{$workflowCamel}}SignalMessages[s]
}
{{end}}

{{- range .Signals}}
// Signal{{$workflowPascal}}{{.Pascal}} provides a typed API for signalling a {{$workflowName}} workflow run with signal {{.Name}}.
// It returns true on success or false on duplicate calls or an error.
func Signal{{$workflowPascal}}{{.Pascal}}(ctx context.Context, cl replay.Client, run string, message *{{.Message}}, extID string) (bool, error) {
	return cl.SignalRun(ctx, _ns, _w{{$workflowPascal}}, run, _s{{$workflowPascal}}{{.Pascal}}, message, extID)
}
{{- end}}

// Run{{$workflowPascal}} provides a type API for running the {{.Name}} workflow.
// It returns true on success or false on duplicate calls or an error.
func Run{{$workflowPascal}}(ctx context.Context, cl replay.Client, run string, message *{{.Input}}) (bool, error) {
	return cl.RunWorkflow(ctx, _ns, _w{{$workflowPascal}}, run, message)
}
{{- end}}

{{if .ExposeRegisters}}
{{range .Workflows}}
// Register{{.Pascal}} registers and starts the {{.Name}} workflow consumer.
func Register{{.Pascal}}(getCtx func() context.Context, cl replay.Client, cstore reflex.CursorStore,
    {{.Camel}} func({{.Camel}}Flow, *{{.Input}}), opts ...replay.Option) {

    {{.Camel}}Func := func(ctx replay.RunContext, message *{{.Input}}) {
        {{.Camel}}({{.Camel}}FlowImpl{ctx}, message)
    }

    copied := append([]replay.Option{replay.WithName(_w{{.Pascal}})}, opts...)

    replay.RegisterWorkflow(getCtx, cl, cstore, _ns, {{.Camel}}Func, copied...)
}
{{end}}
{{range .Activities}}
// Register{{.Pascal}} registers and starts the {{.FuncName}} activity consumer.
func Register{{.Pascal}}(getCtx func() context.Context, cl replay.Client, cstore reflex.CursorStore, b Backends, opts ...replay.Option) {

    copied := append([]replay.Option{replay.WithName(_a{{.Pascal}})}, opts...)

    replay.RegisterActivity(getCtx, cl, cstore, b, _ns, {{.FuncName}}, copied...)
}
{{end}}
{{else}}
// startReplayLoops registers the workflow and activities for typed workflow functions.
func startReplayLoops(getCtx func() context.Context, cl replay.Client, cstore reflex.CursorStore, b Backends,
    {{range .Workflows}} {{.Camel}} func({{.Camel}}Flow, *{{.Input}}), {{end}} ){

	{{range .Workflows}}
	{{.Camel}}Func := func(ctx replay.RunContext, message *{{.Input}}) {
		{{.Camel}}({{.Camel}}FlowImpl{ctx}, message)
	}
	replay.RegisterWorkflow(getCtx, cl, cstore, _ns, {{.Camel}}Func, replay.WithName(_w{{.Pascal}}))
	{{end}}

	{{- range .Activities}}
	replay.RegisterActivity(getCtx, cl, cstore, b, _ns, {{.FuncName}}, replay.WithName(_a{{.Pascal}}))
	{{- end}}
}
{{end}}

{{$al := .Activities}}
{{- range .Workflows}} {{$workflowCamel := .Camel}} {{$workflowPascal := .Pascal}}
// {{.Camel}}Flow defines a typed API for the {{.Name}} workflow.
type {{.Camel}}Flow interface {

    // Sleep blocks for at least d duration.
    // Note that replay sleeps aren't very accurate and
    // a few seconds is the practical minimum.
	Sleep(d time.Duration)

	// CreateEvent returns the reflex event that started the run iteration (type is internal.CreateRun).
	// The event timestamp could be used to reason about run age.
	CreateEvent() *reflex.Event

	// LastEvent returns the latest reflex event (type is either internal.CreateRun or internal.ActivityResponse).
    // The event timestamp could be used to reason about run liveliness.
	LastEvent() *reflex.Event

    // Now returns the last event timestamp as the deterministic "current" time.
    // It is assumed the first time this is used in logic it will be very close to correct while
    // producing deterministic logic during bootstrapping.
	Now() time.Time

	// Run returns the run name/identifier.
	Run() string

	// Restart completes the current run iteration and starts a new run iteration with the provided input message.
	// The run state is effectively reset. This is handy to mitigate bootstrap load for long running tasks.
	// It also allows updating the activity logic/ordering.
	Restart(message *{{.Input}})

	{{range $al}}
	// {{.FuncTitle}} results in the {{.FuncName}} activity being called asynchronously
	// with the provided parameter and returns the response once available.
	{{.FuncTitle}}(message *{{.Input}}) *{{.Output}}
	{{end}}
	{{- range .Signals}}

	// Await{{.Pascal}} blocks and returns true when a {{.Name}} signal is/was
	// received for this run. If no signal is/was received it returns false after d duration.
	Await{{.Pascal}}(d time.Duration) (*{{.Message}}, bool)
	{{- end}}
	{{range .Outputs}}

    // Emit{{.Pascal}} stores the {{.Name}} output in the event log and returns when successful.
    Emit{{.Pascal}}(message *{{.Message}})
    {{end}}
}

type {{.Camel}}FlowImpl struct {
	ctx replay.RunContext
}

func (f {{.Camel}}FlowImpl) Sleep(d time.Duration) {
	f.ctx.Sleep(d)
}

func (f {{.Camel}}FlowImpl) CreateEvent() *reflex.Event {
	return f.ctx.CreateEvent()
}

func (f {{.Camel}}FlowImpl) LastEvent() *reflex.Event {
	return f.ctx.LastEvent()
}

func (f {{.Camel}}FlowImpl) Now() time.Time {
	return f.ctx.LastEvent().Timestamp
}

func (f {{.Camel}}FlowImpl) Run() string {
	return f.ctx.Run()
}

func (f {{.Camel}}FlowImpl) Restart(message *{{.Input}}) {
	f.ctx.Restart(message)
}

{{range $al}}
func (f {{$workflowCamel}}FlowImpl) {{.FuncTitle}}(message *{{.Input}}) *{{.Output}} {
	return f.ctx.ExecActivity({{.FuncName}}, message, replay.WithName(_a{{.Pascal}})).(*{{.Output}})
}
{{end}}

{{range .Signals}}
func (f {{$workflowCamel}}FlowImpl) Await{{.Pascal}}(d time.Duration) (*{{.Message}}, bool) {
	res, ok := f.ctx.AwaitSignal(_s{{$workflowPascal}}{{.Pascal}}, d)
	if !ok {
		return nil, false
	}
	return res.(*{{.Message}}), true
}
{{end}}

{{range .Outputs}}
func (f {{$workflowCamel}}FlowImpl) Emit{{.Pascal}}(message *{{.Message}}) {
	f.ctx.EmitOutput(_o{{$workflowPascal}}{{.Pascal}}, message)
}
{{end}}

// Stream{{.Pascal}} returns a stream of replay events for the {{.Name}} workflow and an optional run.
func Stream{{.Pascal}}(cl replay.Client, run string) reflex.StreamFunc {
	return cl.Stream(_ns, _w{{.Pascal}}, run)
}

// Handle{{.Pascal}}Run calls fn if the event is a {{.Name}} RunCreated event.
// Use Stream{{.Pascal}} to provide the events.
func Handle{{.Pascal}}Run(e *reflex.Event, fn func(run string, message *{{.Input}}) error) error{
	return replay.Handle(e,
		replay.HandleSkip(func(namespace, workflow, run string) bool {
			return namespace != _ns || workflow != _w{{.Pascal}}
		}),
		replay.HandleRunCreated(func(namespace, workflow, run string, message proto.Message) error {
			return fn(run, message.(*{{.Input}}))
		}),
	)
}

{{range .Outputs}}
// Handle{{.Pascal}} calls fn if the event is a {{.Name}} output.
// Use Stream{{$workflowPascal}} to provide the events.
func Handle{{.Pascal}}(e *reflex.Event, fn func(run string, message *{{.Message}}) error) error{
	return replay.Handle(e,
		replay.HandleSkip(func(namespace, workflow, run string) bool {
			return namespace != _ns || workflow != _w{{$workflowPascal}}
		}),
		replay.HandleOutput(func(namespace, workflow, run string, output string, message proto.Message) error {
			if output != _o{{$workflowPascal}}{{.Pascal}} {
				return nil
			}
			return fn(run, message.(*{{.Message}}))
		}),
	)
}
{{end}}

{{end}}
