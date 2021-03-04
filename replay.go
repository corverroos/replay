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
	RequestActivity(ctx context.Context, workflow, run string, name string, args proto.Message) error
	CompleteActivity(ctx context.Context, workflow, run string, name string, response proto.Message) error
	CompleteRun(ctx context.Context, workflow, run string) error
	Stream(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error)
}

type WorkflowFunc func(RunContext, proto.Message) error

func RegisterActivity(cl Client, cstore reflex.CursorStore, backends interface{}, activityFunc interface{}) {
	activity := getFunctionName(activityFunc)

	fn := func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
		if !reflex.IsType(e.Type, db.ActivityRequest) {
			return nil
		}

		var id db.EventID
		if err := json.Unmarshal([]byte(e.ForeignID), &id); err != nil {
			return err
		}

		if id.Activity != activity {
			return nil
		}

		var d ptypes.DynamicAny
		if len(e.MetaData) > 0 {
			var a anypb.Any
			if err := proto.Unmarshal(e.MetaData, &a); err != nil {
				return errors.Wrap(err, "unmarshal proto")
			}

			if err := ptypes.UnmarshalAny(&a, &d); err != nil {
				return errors.Wrap(err, "unmarshal anypb")
			}
		}

		args := []reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(backends),
			reflect.ValueOf(d.Message),
		}

		respVals := reflect.ValueOf(activityFunc).Call(args)

		resp := respVals[0].Interface().(proto.Message)
		// TODO(corver): Handle activity errors.
		return cl.CompleteActivity(ctx, id.Workflow, id.Run, id.Activity, resp)
	}

	spec := reflex.NewSpec(cl.Stream, cstore, reflex.NewConsumer(activity, fn))
	go rpatterns.RunForever(func() context.Context{ return context.Background()}, spec)
}

func RegisterWorkflow(cl Client, cstore reflex.CursorStore, workflowFunc WorkflowFunc) {
	workflow := getFunctionName(workflowFunc) // TODO(corver): Prefix service name.

	r := runner{
		cl:           cl,
		workflow:     workflow,
		workflowFunc: workflowFunc,
		runs:         make(map[string]*runState),
	}
	fn := func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
		var id db.EventID
		if err := json.Unmarshal([]byte(e.ForeignID), &id); err != nil {
			return err
		}

		if id.Workflow != workflow {
			return nil
		}

		var d ptypes.DynamicAny
		if len(e.MetaData) > 0 {
			var a anypb.Any
			if err := proto.Unmarshal(e.MetaData, &a); err != nil {
				return errors.Wrap(err, "unmarshal proto")
			}

			if err := ptypes.UnmarshalAny(&a, &d); err != nil {
				return errors.Wrap(err, "unmarshal anypb")
			}
		}

		switch e.Type.ReflexType() {
		case db.CreateRun.ReflexType():
			return r.StartRun(ctx, id.Run, d.Message)
		case db.ActivityResponse.ReflexType():
			return r.RespondActivity(ctx, id.Run, id.Activity, d.Message)
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
	go rpatterns.RunForever(func() context.Context{ return context.Background()}, spec)
}

type runState struct {
	cond      *sync.Cond
	responses map[string][]proto.Message
}

func (s *runState) RespondActivity(name string, response proto.Message) {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()

	s.responses[name] = append(s.responses[name], response)

	s.cond.Broadcast()
}

func (s *runState) AwaitActivity(name string) proto.Message {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()

	for len(s.responses[name]) == 0 {
		s.cond.Wait()
	}

	pop := s.responses[name][0]
	s.responses[name] = s.responses[name][1:]

	return pop
}

type runner struct {
	sync.Mutex

	cl           Client
	workflow     string
	workflowFunc WorkflowFunc

	runs map[string]*runState
}

func (r *runner) StartRun(ctx context.Context, run string, args proto.Message) error {
	r.Lock()
	defer r.Unlock()

	if _, ok := r.runs[run]; ok {
		return errors.New("run already started")
	}

	s := &runState{
		cond:      sync.NewCond(new(sync.Mutex)),
		responses: make(map[string][]proto.Message),
	}
	r.runs[run] = s

	go func() {
		for {
			if err := r.run(ctx, run, args, s); err != nil {
				log.Error(ctx, errors.Wrap(err, "run"))
				time.Sleep(time.Minute)
				continue
			}

			if err := r.cl.CompleteRun(ctx, r.workflow, run); err != nil {
				log.Error(ctx, errors.Wrap(err, "run"))
				time.Sleep(time.Minute)
				continue
			}

			return
		}
	}()

	return nil
}

func (r *runner) RespondActivity(ctx context.Context, run string, activity string, message proto.Message) error {
	r.Lock()
	defer r.Unlock()

	if _, ok := r.runs[run]; !ok {
		return errors.New("run not found")
	}

	r.runs[run].RespondActivity(activity, message)

	return nil
}

func (r *runner) run(ctx context.Context, run string, args proto.Message,state *runState ) error {
	return r.workflowFunc(RunContext{
		Context:  ctx,
		workflow: r.workflow,
		run:      run,
		state:    state,
		cl:       r.cl,
	}, args)
}

type RunContext struct {
	context.Context
	workflow string
	run string
	state *runState
	cl Client
}

func (c *RunContext) ExecuteActivity(activityFunc interface{}, args proto.Message) (proto.Message, error) {
	activity := getFunctionName(activityFunc)
	err := c.cl.RequestActivity(c, c.workflow, c.run, activity, args)
	if err != nil {
		return nil, err
	}

	return c.state.AwaitActivity(activity), nil
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
	// ExecuteActivity(ctx, a.Foo)
	// will call this function which is going to return "Foo"
	return strings.TrimSuffix(shortName, "-fm")
}
