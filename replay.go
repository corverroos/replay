package replay

import (
	"context"
	"encoding/json"
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
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"
)

type Client interface {
	RunWorkflow(ctx context.Context, workflow , run string, args proto.Message,) error
	CompleteAsyncActivity(ctx context.Context, token string, message proto.Message) error

	RequestActivity(ctx context.Context, workflow, run string, activity string, index int, message proto.Message) error
	CompleteActivity(ctx context.Context, workflow, run string, activity string, index int, message proto.Message) error
	RequestAsyncActivity(ctx context.Context, workflow, run string, activity string, index int, message proto.Message) error
	CompleteRun(ctx context.Context, workflow, run string) error
	ListBootstrapEvents(ctx context.Context, workflow, run string) ([]reflex.Event, error)
	Stream(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error)
}

func RegisterActivity(ctx context.Context, cl Client, cstore reflex.CursorStore, backends interface{}, activityFunc interface{}) {
	activity := getFunctionName(activityFunc)

	fn := func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
		if !reflex.IsType(e.Type, db.ActivityRequest) {
			return nil
		}

		id, message, err := internal.ParseEvent(e)
		if err != nil {
			return err
		}

		if id.Activity != activity {
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
		return cl.CompleteActivity(ctx, id.Workflow, id.Run, id.Activity, id.Index, resp)
	}

	spec := reflex.NewSpec(cl.Stream, cstore, reflex.NewConsumer(activity, fn))
	go rpatterns.RunForever(func() context.Context { return ctx }, spec)
}

func RegisterAsyncActivity(ctx context.Context, cl Client, cstore reflex.CursorStore, backends interface{}, activityFunc interface{}) {
	activity := getFunctionName(activityFunc)

	fn := func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
		if !reflex.IsType(e.Type, db.AsyncActivityRequest) {
			return nil
		}

		id, message, err := internal.ParseEvent(e)
		if err != nil {
			return err
		}

		if id.Activity != activity {
			return nil
		}

		args := []reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(backends),
			reflect.ValueOf(internal.MarshalAsyncToken(e.ForeignID)),
			reflect.ValueOf(message),
		}

		reflect.ValueOf(activityFunc).Call(args)

		// TODO(corver): Handle activity errors.
		return nil
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

		id, message, err := internal.ParseEvent(e)
		if err != nil {
			return err
		}

		if id.Workflow != workflow {
			return nil
		}

		switch e.Type.ReflexType() {
		case db.CreateRun.ReflexType():
			return r.StartRun(ctx, e, id.Run, message)
		case db.ActivityResponse.ReflexType(), db.AsyncActivityResponse.ReflexType():
			return r.RespondActivity(ctx, e, id.Run, id.Activity, id.Index, message)
		case db.ActivityRequest.ReflexType():
			return nil
		case db.AsyncActivityRequest.ReflexType():
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
	responses map[string]chan response // TODO(corver): refactor key from activity to activity+index
	ack       chan struct{}
}

type response struct {
	message proto.Message
	event   *reflex.Event
}

func (s *runState) RespondActivity(e *reflex.Event, activity string, index int, message proto.Message) {
	s.mu.Lock()
	if _, ok := s.responses[activity]; !ok {
		s.responses[activity] = make(chan response)
	}
	s.mu.Unlock()
	s.responses[activity] <- response{event: e, message: message}
	<-s.ack
}

func (s *runState) GetAndIncIndex(activity string) int {
	s.mu.Lock()
	defer func() {
		s.indexes[activity]++
		s.mu.Unlock()
	}()

	return s.indexes[activity]
}

func (s *runState) AwaitActivity(activity string, index int) response {
	s.mu.Lock()
	if _, ok := s.responses[activity]; !ok {
		s.responses[activity] = make(chan response)
	}
	s.mu.Unlock()
	s.ack <- struct{}{}
	return <-s.responses[activity]
}

type runner struct {
	sync.Mutex

	cl           Client
	workflow     string
	workflowFunc interface{}

	runs map[string]*runState
}

func (r *runner) StartRun(ctx context.Context, e *reflex.Event, run string, args proto.Message) error {
	r.Lock()
	defer r.Unlock()

	if _, ok := r.runs[run]; ok {
		return errors.New("run already started")
	}

	s := &runState{
		responses: make(map[string]chan response),
		indexes:   make(map[string]int),
		ack:       make(chan struct{}),
	}
	r.runs[run] = s

	go func() {
		for {
			r.run(ctx, e, run, args, s)

			ensure(ctx, func() error {
				return r.cl.CompleteRun(ctx, r.workflow, run)
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
		id, message, err := internal.ParseEvent(&e)
		if err != nil {
			return err
		}

		if i == 0 {
			if !reflex.IsType(e.Type, db.CreateRun) {
				return errors.New("unexpected first event", j.KV("type", e.Type))
			}

			err := r.StartRun(ctx, &e, run, message)
			if err != nil {
				return err
			}

			continue
		}

		if e.IDInt() > upTo {
			break
		}

		err = r.RespondActivity(ctx, &e, run, id.Activity, id.Index, message)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *runner) RespondActivity(ctx context.Context, e *reflex.Event, run string, activity string, index int, message proto.Message) error {
	r.Lock()

	if _, ok := r.runs[run]; !ok {
		// This run was previously started and is now continuing; bootstrap it.
		r.Unlock()
		return r.bootstrapRun(ctx, run, e.IDInt())
	}

	r.runs[run].RespondActivity(e, activity, index, message)

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
	index := c.state.GetAndIncIndex(activity)

	ensure(c, func() error {
		return c.cl.RequestActivity(c, c.workflow, c.run, activity, index, args)
	})

	res := c.state.AwaitActivity(activity, index)
	c.lastEvent = res.event
	return res.message
}

func (c *RunContext) AsyncActivity(activityFunc interface{}, args proto.Message) *Future {
	activity := getFunctionName(activityFunc)
	index := c.state.GetAndIncIndex(activity)

	ensure(c, func() error {
		return c.cl.RequestAsyncActivity(c, c.workflow, c.run, activity, index, args)
	})

	eid := db.EventID{
		Workflow: c.workflow,
		Run:      c.run,
		Activity: activity,
		Index:    index,
	}
	foreignID, err := json.Marshal(eid)
	if err != nil {
		panic("what!")
	}

	return &Future{
		activity: activity,
		activityIdx: index,
		token: internal.MarshalAsyncToken(string(foreignID)),
	}
}

func (c *RunContext) Sleep(duration time.Duration) {
	activity := internal.SleepActivity
	index := c.state.GetAndIncIndex(activity)

	ensure(c, func() error {
		return c.cl.RequestActivity(c, c.workflow, c.run, activity, index, &internal.SleepRequest{
			Duration:             ptypes.DurationProto(duration),
		})
	})

	c.state.AwaitActivity(activity, index)
}

func (c *RunContext) Await(f *Future, timeout time.Duration) (proto.Message, bool) {
	index := int(int64(f.activityIdx)<<10 | int64(f.awaitIdx))
	f.awaitIdx++
	ensure(c, func() error {
		return c.cl.RequestActivity(c, c.workflow, c.run, internal.SleepActivity, index, &internal.SleepRequest{
			Duration:             ptypes.DurationProto(timeout),
			AsyncToken: f.token,
		})
	})

	resp :=c.state.AwaitActivity(f.activity, index)
	c.lastEvent= resp.event
	_, didTimeout := resp.message.(*internal.SleepDone)
	return resp.message, !didTimeout
}

func (c *RunContext) CreateEvent() *reflex.Event {
	return c.createEvent
}
func (c *RunContext) LastEvent() *reflex.Event {
	return c.lastEvent
}

type Future struct {
	token string
	activity string
	activityIdx int
	awaitIdx int
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
