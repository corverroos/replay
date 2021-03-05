package replay

import (
	"context"
	"encoding/json"
	"github.com/corverroos/replay/db"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/log"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"
	"google.golang.org/protobuf/types/known/anypb"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"
)

type Client interface {
	CreateRun(ctx context.Context, workflow string, args proto.Message) error
	RequestActivity(ctx context.Context, workflow, run string, activity string, index int,args proto.Message) error
	CompleteActivity(ctx context.Context, workflow, run string, activity string, index int,response proto.Message) error
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

		id, message, err := ParseEvent(e)
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
	go rpatterns.RunForever(func() context.Context{ return ctx}, spec)
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

		id, message, err := ParseEvent(e)
		if err != nil {
			return err
		}

		if id.Workflow != workflow {
			return nil
		}


		switch e.Type.ReflexType() {
		case db.CreateRun.ReflexType():
			return r.StartRun(ctx, id.Run, message)
		case db.ActivityResponse.ReflexType():
			return r.RespondActivity(ctx, id.Run, id.Activity, message, e.IDInt())
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
	go rpatterns.RunForever(func() context.Context{ return ctx}, spec)
}

type runState struct {
	mu      sync.Mutex
	indexes map[string]int
	responses map[string]chan proto.Message
	ack chan struct{}
}

func (s *runState) RespondActivity(activity string, response proto.Message) {
	s.mu.Lock()
	if _, ok := s.responses[activity]; !ok {
		s.responses[activity] = make(chan proto.Message)
	}
	s.mu.Unlock()
	s.responses[activity] <- response
	<- s.ack
}

func (s *runState) GetAndIncIndex(activity string) int {
	s.mu.Lock()
	defer func() {
		s.indexes[activity]++
		s.mu.Unlock()
	}()

	return s.indexes[activity]
}

func (s *runState) AwaitActivity(activity string) proto.Message {
	s.mu.Lock()
	if _, ok := s.responses[activity]; !ok {
		s.responses[activity] = make(chan proto.Message)
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

func (r *runner) StartRun(ctx context.Context, run string, args proto.Message) error {
	r.Lock()
	defer r.Unlock()

	if _, ok := r.runs[run]; ok {
		return errors.New("run already started")
	}

	s := &runState{
		responses: make(map[string]chan proto.Message),
		indexes: make(map[string]int),
		ack: make(chan struct{}),
	}
	r.runs[run] = s

	go func() {
		for {
			r.run(ctx, run, args, s)

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
		id, message, err := ParseEvent(&e)
		if err != nil {
			return err
		}

		if i == 0 {
			if !reflex.IsType(e.Type, db.CreateRun) {
				return errors.New("unexpected first event")
			}

			err := r.StartRun(ctx, run, message)
			if err != nil {
				return err
			}

			continue
		}

		if e.IDInt() > upTo {
			break
		}

		err = r.RespondActivity(ctx, run, id.Activity, message, e.IDInt())
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *runner) RespondActivity(ctx context.Context, run string, activity string, message proto.Message, eventID int64) error {
	r.Lock()

	if _, ok := r.runs[run]; !ok {
		// This run was previously started and is now continuing; bootstrap it.
		r.Unlock()
		return r.bootstrapRun(ctx, run, eventID)
	}

	r.runs[run].RespondActivity(activity, message)

	r.Unlock()
	return nil
}

func (r *runner) run(ctx context.Context, run string, args proto.Message,state *runState ) {
	workflowArgs := []reflect.Value{
		reflect.ValueOf(RunContext{
			Context:  ctx,
			workflow: r.workflow,
			run:      run,
			state:    state,
			cl:       r.cl,
		}),
		reflect.ValueOf(args),
	}

	reflect.ValueOf(r.workflowFunc).Call(workflowArgs)
}

type RunContext struct {
	context.Context
	workflow string
	run string
	state *runState
	cl Client
}

func (c *RunContext) ExecActivity(activityFunc interface{}, args proto.Message) proto.Message {
	activity := getFunctionName(activityFunc)
	index := c.state.GetAndIncIndex(activity)

	ensure(c, func() error {
		return c.cl.RequestActivity(c, c.workflow, c.run, activity, index, args)
	})

	return c.state.AwaitActivity(activity)
}

func (c *RunContext) Sleep(duration time.Duration) {
	activity := SleepActivity
	index := c.state.GetAndIncIndex(activity)

	ensure(c, func() error {
		return c.cl.RequestActivity(c, c.workflow, c.run, activity, index, ptypes.DurationProto(duration))
	})

	c.state.AwaitActivity(activity)
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

// TODO(corver): Move this to an internal package
// Deprecated: Only for internal use.
const SleepActivity = "replay_sleep"

// TODO(corver): Move this to an internal package
// Deprecated: Only for internal use.
func ParseEvent(e *reflex.Event) (db.EventID, proto.Message, error) {
	var id db.EventID
	if err := json.Unmarshal([]byte(e.ForeignID), &id); err != nil {
		return db.EventID{}, nil, err
	}

	if len(e.MetaData) == 0 {
		return id, nil, nil
	}

		var a anypb.Any
		if err := proto.Unmarshal(e.MetaData, &a); err != nil {
			return db.EventID{}, nil, errors.Wrap(err, "unmarshal proto")
		}

	var d ptypes.DynamicAny
		if err := ptypes.UnmarshalAny(&a, &d); err != nil {
			return db.EventID{}, nil, errors.Wrap(err, "unmarshal anypb")
		}

		return id, d.Message, nil
}
