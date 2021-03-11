package replay

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/corverroos/replay/db"
	"github.com/corverroos/replay/internal"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"
)

type Client interface {
	RunWorkflow(ctx context.Context, workflow, run string, message proto.Message) error
	SignalRun(ctx context.Context, workflow, run string, s Signal, message proto.Message, extID string) error

	RequestActivity(ctx context.Context, key string, message proto.Message) error
	CompleteActivity(ctx context.Context, key string, message proto.Message) error
	CompleteRun(ctx context.Context, workflow, run string) error
	ListBootstrapEvents(ctx context.Context, workflow, run string) ([]reflex.Event, error)
	Stream(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error)
}

type Signal interface {
	SignalType() int
	MessageType() proto.Message
}

func RegisterActivity(ctx context.Context, cl Client, cstore reflex.CursorStore, backends interface{}, activityFunc interface{}) {
	activity := getFunctionName(activityFunc)

	fn := func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
		if !reflex.IsType(e.Type, db.ActivityRequest) {
			return nil
		}

		key, message, err := internal.ParseEvent(e)
		if err != nil {
			return err
		}

		if key.Activity != activity {
			return nil
		}

		args := []reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(backends),
			reflect.ValueOf(message),
		}

		respVals := reflect.ValueOf(activityFunc).Call(args)

		resp := respVals[0].Interface().(proto.Message)
		// TODO(corver): Handle activity errors.
		return cl.CompleteActivity(ctx, e.ForeignID, resp)
	}

	spec := reflex.NewSpec(cl.Stream, cstore, reflex.NewConsumer(activity, fn))
	go rpatterns.RunForever(func() context.Context { return ctx }, spec)
}

func RegisterWorkflow(ctx context.Context, cl Client, cstore reflex.CursorStore, workflowFunc interface{}) {
	workflow := getFunctionName(workflowFunc) // TODO(corver): Prefix service name.

	// TODO(corver): Validate workflowfunc signature.

	r := runner{
		cl:           cl,
		workflow:     workflow,
		workflowFunc: workflowFunc,
		runs:         make(map[string]*runState),
	}
	fn := func(ctx context.Context, f fate.Fate, e *reflex.Event) error {

		key, message, err := internal.ParseEvent(e)
		if err != nil {
			return err
		}

		if key.Workflow != workflow {
			return nil
		}

		switch e.Type.ReflexType() {
		case db.CreateRun.ReflexType():
			return r.StartRun(ctx, e, key, message)
		case db.ActivityResponse.ReflexType():
			return r.RespondActivity(ctx, e, key, message)
		case db.ActivityRequest.ReflexType():
			return nil
		case db.CompleteRun.ReflexType():
			return nil
		case db.FailRun.ReflexType():
			return nil
		default:
			return errors.New("unknown type")
		}
	}

	spec := reflex.NewSpec(cl.Stream, cstore, reflex.NewConsumer(workflow, fn))
	go rpatterns.RunForever(func() context.Context { return ctx }, spec)
}

type runState struct {
	mu        sync.Mutex
	indexes   map[string]int
	responses map[internal.Key]chan response
	ack       chan struct{}
}

type response struct {
	message proto.Message
	event   *reflex.Event
}

func (s *runState) RespondActivity(e *reflex.Event, key internal.Key, message proto.Message) {
	s.mu.Lock()
	if _, ok := s.responses[key]; !ok {
		s.responses[key] = make(chan response)
	}
	s.mu.Unlock()
	s.responses[key] <- response{event: e, message: message}
	<-s.ack
}

func (s *runState) GetAndInc(activity string) int {
	s.mu.Lock()
	defer func() {
		s.indexes[activity]++
		s.mu.Unlock()
	}()

	return s.indexes[activity]
}

func (s *runState) AwaitActivity(key internal.Key) response {
	s.mu.Lock()
	if _, ok := s.responses[key]; !ok {
		s.responses[key] = make(chan response)
	}
	s.mu.Unlock()
	s.ack <- struct{}{}
	return <-s.responses[key]
}

type runner struct {
	sync.Mutex

	cl           Client
	workflow     string
	workflowFunc interface{}

	runs map[string]*runState
}

func (r *runner) StartRun(ctx context.Context, e *reflex.Event, key internal.Key, args proto.Message) error {
	r.Lock()
	defer r.Unlock()

	if _, ok := r.runs[key.Run]; ok {
		return errors.New("run already started")
	}

	s := &runState{
		responses: make(map[internal.Key]chan response),
		indexes:   make(map[string]int),
		ack:       make(chan struct{}),
	}
	r.runs[key.Run] = s

	go func() {
		for {
			r.run(ctx, e, key.Run, args, s)

			ensure(ctx, func() error {
				return r.cl.CompleteRun(ctx, r.workflow, key.Run)
			})

			return
		}
	}()

	<-s.ack
	return nil
}

func (r *runner) bootstrapRun(ctx context.Context, run string, upTo int64) error {
	el, err := r.cl.ListBootstrapEvents(ctx, r.workflow, run)
	if err != nil {
		return errors.Wrap(err, "list responses")
	}

	for i, e := range el {
		key, message, err := internal.ParseEvent(&e)
		if err != nil {
			return err
		}

		if i == 0 {
			if !reflex.IsType(e.Type, db.CreateRun) {
				return errors.New("unexpected first event", j.KV("type", e.Type))
			}

			err := r.StartRun(ctx, &e, key, message)
			if err != nil {
				return err
			}

			continue
		}

		if e.IDInt() > upTo {
			break
		}

		err = r.RespondActivity(ctx, &e, key, message)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *runner) RespondActivity(ctx context.Context, e *reflex.Event, key internal.Key, message proto.Message) error {
	r.Lock()

	if _, ok := r.runs[key.Run]; !ok {
		// This run was previously started and is now continuing; bootstrap it.
		r.Unlock()
		return r.bootstrapRun(ctx, key.Run, e.IDInt())
	}

	r.runs[key.Run].RespondActivity(e, key, message)

	r.Unlock()
	return nil
}

func (r *runner) run(ctx context.Context, e *reflex.Event, run string, args proto.Message, state *runState) {
	workflowArgs := []reflect.Value{
		reflect.ValueOf(RunContext{
			Context:     ctx,
			workflow:    r.workflow,
			run:         run,
			state:       state,
			cl:          r.cl,
			createEvent: e,
			lastEvent:   e,
		}),
		reflect.ValueOf(args),
	}

	reflect.ValueOf(r.workflowFunc).Call(workflowArgs)
}

type RunContext struct {
	context.Context
	workflow string
	run      string
	state    *runState
	cl       Client

	createEvent *reflex.Event
	lastEvent   *reflex.Event
}

func (c *RunContext) ExecActivity(activityFunc interface{}, args proto.Message) proto.Message {
	activity := getFunctionName(activityFunc)
	index := c.state.GetAndInc(activity)
	key := internal.Key{
		Workflow: c.workflow,
		Run:      c.run,
		Activity: activity,
		Sequence: fmt.Sprint(index),
	}

	ensure(c, func() error {
		return c.cl.RequestActivity(c, key.Encode(), args)
	})

	res := c.state.AwaitActivity(key)
	c.lastEvent = res.event
	return res.message
}

func (c *RunContext) AwaitSignal(s Signal, duration time.Duration) (proto.Message, bool) {
	activity := internal.SignalActivity
	seq := internal.SignalSequence{
		SignalType: s.SignalType(),
		Index:      c.state.GetAndInc(fmt.Sprintf("%s:%d", activity, s.SignalType())),
	}

	key := internal.Key{
		Workflow: c.workflow,
		Run:      c.run,
		Activity: activity,
		Sequence: seq.Encode(),
	}
	ensure(c, func() error {
		return c.cl.RequestActivity(c, key.Encode(), &internal.SleepRequest{
			Duration: ptypes.DurationProto(duration),
		})
	})

	res := c.state.AwaitActivity(key)
	c.lastEvent = res.event
	if _, ok := res.message.(*internal.SleepDone); ok {
		return nil, false
	}
	return res.message, true
}

func (c *RunContext) Sleep(duration time.Duration) {
	activity := internal.SleepActivity
	index := c.state.GetAndInc(activity)
	key := internal.Key{
		Workflow: c.workflow,
		Run:      c.run,
		Activity: activity,
		Sequence: fmt.Sprint(index),
	}

	ensure(c, func() error {
		return c.cl.RequestActivity(c, key.Encode(), &internal.SleepRequest{
			Duration: ptypes.DurationProto(duration),
		})
	})

	c.state.AwaitActivity(key)
}

func (c *RunContext) CreateEvent() *reflex.Event {
	return c.createEvent
}
func (c *RunContext) LastEvent() *reflex.Event {
	return c.lastEvent
}
func (c *RunContext) Run() string {
	return c.run
}

func getFunctionName(i interface{}) string {
	if fullName, ok := i.(string); ok {
		return fullName
	}
	fullName := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
	elements := strings.Split(fullName, ".")
	shortName := elements[len(elements)-1]
	// This allows to call activities by method pointer
	// Compiler adds -fm suffix to a function name which has a receiver
	// Note that this works even if struct pointer used to get the function is nil
	// It is possible because nil receivers are allowed.
	// For example:
	// var a *Activities
	// ExecActivity(ctx, a.Foo)
	// will call this function which is going to return "Foo"
	return strings.TrimSuffix(shortName, "-fm")
}

func ensure(ctx context.Context, fn func() error) {
	for {
		err := fn()
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "ensure"))
			time.Sleep(time.Second)
			continue
		}
		return
	}
}
