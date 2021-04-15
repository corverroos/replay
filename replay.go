// Package replay provides a workflow framework that is robust with respect to temporary errors.
package replay

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/corverroos/replay/internal"
	"github.com/corverroos/replay/internal/replaypb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"
)

var ErrDuplicate = errors.New("duplicate entry", j.C("ERR_96713b1c52c5d59f"))

const cancel = "cancel"
const restart = "restart"

// Client defines the main replay server API.
type Client interface {
	// RunWorkflow inserts a CreateRun event which results in an invocation
	// of the workflow with the message.
	//
	// The run identifier must be unique otherwise ErrDuplicate is returned
	// since the run was already created.
	RunWorkflow(ctx context.Context, namespace, workflow, run string, message proto.Message) error

	// SignalRun inserts signal which results in the signal being available
	// to the run if it subsequently calls ctx.AwaitSignal.
	//
	// External ID must be unique per namespace and workflow otherwise ErrDuplicate is returned since
	// the signal was already created.
	SignalRun(ctx context.Context, namespace, workflow, run string, s Signal, message proto.Message, extID string) error

	// Stream returns a replay events stream function for the namespace.
	Stream(namespace string) reflex.StreamFunc

	// Internal returns the internal replay server API. This should only be used by the replay package itself.
	Internal() internal.Client
}

// Signal defines a signal.
type Signal interface {
	// SignalType identifies different signal types of a workflow.
	SignalType() int
	// MessageType defines the data-type associated with this signal.
	MessageType() proto.Message
}

// RegisterActivity starts a activity consumer that consumes replay events and executes the activity if requested.
func RegisterActivity(getCtx func() context.Context, cl Client, cstore reflex.CursorStore, backends interface{}, namespace string, activityFunc interface{}, opts ...option) {
	if err := validateActivity(activityFunc); err != nil {
		panic(err)
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	activity := o.nameFunc(activityFunc)
	metrics := o.activityMetrics(namespace, activity)

	fn := func(ctx context.Context, f fate.Fate, e *reflex.Event) (err error) {
		defer func(t0 time.Time) {
			metrics.IncComplete(time.Since(t0))
			if err != nil {
				metrics.IncErrors() // NoReturnErr: Just incrementing metrics here.
			}
		}(time.Now())

		if !reflex.IsType(e.Type, internal.ActivityRequest) {
			return nil
		}

		key, err := internal.DecodeKey(e.ForeignID)
		if err != nil {
			return err
		}

		if key.Activity != activity {
			return nil
		}

		message, err := internal.ParseMessage(e)
		if err != nil {
			return err
		}

		args := []reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(backends),
			reflect.ValueOf(fate.New()),
			reflect.ValueOf(message),
		}

		respVals := reflect.ValueOf(activityFunc).Call(args)

		if !respVals[1].IsNil() {
			return respVals[1].Interface().(error)
		}

		return cl.Internal().RespondActivity(ctx, e.ForeignID, respVals[0].Interface().(proto.Message))
	}

	spec := reflex.NewSpec(cl.Stream(namespace), cstore, reflex.NewConsumer(activity, fn))
	go rpatterns.RunForever(getCtx, spec)
}

// RegisterWorkflow starts a workflow consumer that consumes replay events and executes the workflow.
// It maintains a goroutine for each run when started or when an activity response is received.
func RegisterWorkflow(getCtx func() context.Context, cl Client, cstore reflex.CursorStore, namespace string, workflowFunc interface{}, opts ...option) {
	if err := validateWorkflow(workflowFunc); err != nil {
		panic(err)
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	workflow := o.nameFunc(workflowFunc)
	metrics := o.workflowMetrics(namespace, workflow)

	r := runner{
		cl:           cl.Internal(),
		metrics:      metrics,
		namespace:    namespace,
		workflow:     workflow,
		workflowFunc: workflowFunc,
		runs:         make(map[string]*runState),
	}
	fn := func(ctx context.Context, f fate.Fate, e *reflex.Event) (err error) {
		defer func() {
			if err != nil {
				metrics.IncErrors() // NoReturnErr: Just incrementing metrics here.
			}
		}()

		key, err := internal.DecodeKey(e.ForeignID)
		if err != nil {
			return err
		}

		if key.Workflow != workflow {
			return nil
		}

		message, err := internal.ParseMessage(e)
		if err != nil {
			return err
		}

		switch e.Type.ReflexType() {
		case internal.CreateRun.ReflexType():
			metrics.IncStart()
			_, err := r.StartRun(ctx, e, key, message)
			return err
		case internal.ActivityResponse.ReflexType():
			_, err := r.RespondActivity(ctx, e, key, message, false)
			return err
		case internal.ActivityRequest.ReflexType():
			return nil
		case internal.CompleteRun.ReflexType():
			return nil
		default:
			return errors.New("bug: unknown type")
		}
	}

	spec := reflex.NewSpec(cl.Stream(namespace), cstore, reflex.NewConsumer(workflow, fn))
	go rpatterns.RunForever(getCtx, spec)
}

type runState struct {
	mu        sync.Mutex
	indexes   map[string]int
	responses map[internal.Key]chan response

	//  ackChan is used by the run goroutine to signal the main workflow consumer
	// that it has progressed to the next checkpoint (await or complete) or not (panic or cancel).
	ackChan chan ack
}

type response struct {
	message proto.Message
	event   *reflex.Event
}

type ack struct {
	await    bool
	complete bool
	panic    bool
	cancel   bool
}

func (s *runState) RespondActivity(e *reflex.Event, key internal.Key, message proto.Message) error {
	s.mu.Lock()
	ch, ok := s.responses[key]
	if !ok {
		return errors.New("workflow logic changed, run goroutine not awaiting response", j.KS("key", key.Encode()))
	}
	s.mu.Unlock()

	select {
	case ch <- response{event: e, message: message}:
		return nil
	default:
		return errors.New("run goroutine not awaiting response, maybe it was cancelled")
	}
}

func (s *runState) GetAndInc(activity string) int {
	s.mu.Lock()
	defer func() {
		s.indexes[activity]++
		s.mu.Unlock()
	}()

	return s.indexes[activity]
}

func (s *runState) AwaitActivity(ctx context.Context, key internal.Key) response {
	s.mu.Lock()
	ch, ok := s.responses[key]
	if !ok {
		ch = make(chan response)
		s.responses[key] = ch
	}
	s.mu.Unlock()
	s.ackChan <- ack{await: true}

	select {
	case <-ctx.Done():
		panic(cancel)
	case r := <-ch:
		return r
	}
}

type runner struct {
	sync.Mutex

	cl           internal.Client
	namespace    string
	workflow     string
	workflowFunc interface{}
	metrics      Metrics

	runs map[string]*runState
}

// StartRun starts a run goroutine and registers it with the runner. The run goroutine will call
// the workflow function with the provided message. It blocks until an ack is received from the run goroutine.
// It returns true if the run is still active (awaiting first activity response).
func (r *runner) StartRun(ctx context.Context, e *reflex.Event, key internal.Key, message proto.Message) (bool, error) {
	r.Lock()
	defer r.Unlock()

	if _, ok := r.runs[key.Run]; ok {
		return false, errors.New("bug: run already started")
	}

	s := &runState{
		responses: make(map[internal.Key]chan response),
		indexes:   make(map[string]int),
		ackChan:   make(chan ack),
	}
	r.runs[key.Run] = s

	go func() {
		defer func() {
			if r := recover(); r == cancel {
				log.Error(ctx, errors.New("run cancelled"), j.MKV{"key": key.Encode()})
				s.ackChan <- ack{cancel: true}
			} else if r == restart {
				s.ackChan <- ack{complete: true}
			} else if r != nil {
				log.Error(ctx, errors.New("run panic"), j.MKV{"key": key.Encode(), "panic": r})
				s.ackChan <- ack{panic: true}
			} else {
				s.ackChan <- ack{complete: true}
			}
		}()

		r.run(ctx, e, key.Run, key.Iteration, message, s)

		ensure(ctx, func() error {
			return r.cl.CompleteRun(ctx, r.namespace, r.workflow, key.Run, key.Iteration)
		})

		r.metrics.IncComplete(time.Since(e.Timestamp))
	}()

	return r.processAck(ctx, s.ackChan, key.Run)
}

// bootstrapRun bootstraps a previously started run by replaying all previous events. It returns
// true if the run is still active after bootstrapping.
func (r *runner) bootstrapRun(ctx context.Context, run string, iter int, upTo int64) (bool, error) {
	el, err := r.cl.ListBootstrapEvents(ctx, r.namespace, r.workflow, run, iter)
	if err != nil {
		return false, errors.Wrap(err, "list responses")
	}

	for _, e := range el {
		if reflex.IsType(e.Type, internal.CompleteRun) {
			// Complete event in bootstrap list means logic changed
			// and the run already completed before a previously requested
			// activity's response.
			// Ignore events after complete by skipping bootstrap.
			log.Error(ctx, errors.New("not bootstrapping completed run"))
			return false, nil
		}
	}

	for i, e := range el {
		key, err := internal.DecodeKey(e.ForeignID)
		if err != nil {
			return false, err
		}

		message, err := internal.ParseMessage(&e)
		if err != nil {
			return false, err
		}

		if i == 0 {
			if !reflex.IsType(e.Type, internal.CreateRun) {
				return false, errors.New("bug: unexpected first event", j.KV("type", e.Type))
			}

			active, err := r.StartRun(ctx, &e, key, message)
			if err != nil {
				return active, err
			} else if !active {
				// Workflow logic changed: run completed during bootstrap. Skip rest of events.
				log.Error(ctx, errors.New("run completed during bootstrap"))
				return false, nil
			}

			continue
		}

		if e.IDInt() > upTo {
			break
		}

		if !reflex.IsType(e.Type, internal.ActivityResponse) {
			return false, errors.New("bug: unexpected type")
		}

		active, err := r.RespondActivity(ctx, &e, key, message, true)
		if err != nil {
			return false, err
		} else if !active {
			// Workflow logic changed: run completed during bootstrap. Skip rest of events.
			log.Error(ctx, errors.New("run completed during bootstrap"))
			return false, nil
		}
	}

	return true, nil
}

// RespondActivity hands the activity response over to the run goroutine and blocks until it acks.
// If the run is not registered with this runner, it is bootstrapped.
// It returns true if the run is still active afterwards (waiting for the subsequent activity response).
func (r *runner) RespondActivity(ctx context.Context, e *reflex.Event, key internal.Key, message proto.Message, bootstrap bool) (bool, error) {
	r.Lock()

	if _, ok := r.runs[key.Run]; !ok {
		// This run was previously started and is now continuing; bootstrap it.
		r.Unlock()

		if bootstrap {
			return false, errors.New("bug: recursive bootstrapping")
		}

		return r.bootstrapRun(ctx, key.Run, key.Iteration, e.IDInt())
	}

	s := r.runs[key.Run]
	r.Unlock()

	err := s.RespondActivity(e, key, message)
	if err != nil {
		return false, err
	}

	return r.processAck(ctx, s.ackChan, key.Run)
}

func (r *runner) run(ctx context.Context, e *reflex.Event, run string, iter int, message proto.Message, state *runState) {
	args := []reflect.Value{
		reflect.ValueOf(RunContext{
			Context:     ctx,
			namespace:   r.namespace,
			workflow:    r.workflow,
			run:         run,
			iter:        iter,
			state:       state,
			cl:          r.cl,
			createEvent: e,
			lastEvent:   e,
		}),
		reflect.ValueOf(message),
	}

	reflect.ValueOf(r.workflowFunc).Call(args)
}

// processAck waits for an ack from the run goroutine and does cleanup if the run completed.
// It returns true if the run is still active; awaiting next activity response.
func (r *runner) processAck(ctx context.Context, ackChan chan ack, run string) (bool, error) {
	var a ack
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case a = <-ackChan:
	}

	if a.await {
		return true, nil
	} else if a.complete {
		delete(r.runs, run)
		return false, nil
	} else if a.cancel {
		delete(r.runs, run)
		return false, errors.New("run cancelled")
	} else if a.panic {
		delete(r.runs, run)
		return false, errors.New("run panic")
	} else {
		return false, errors.New("bug: invalid ack")
	}
}

type RunContext struct {
	context.Context
	namespace string
	workflow  string
	run       string
	iter      int
	state     *runState
	cl        internal.Client

	createEvent *reflex.Event
	lastEvent   *reflex.Event
}

func (c *RunContext) Restart(message proto.Message) {
	// TODO(corver): Maybe add option to drain signals.
	ensure(c, func() error {
		return c.cl.RunWorkflowInternal(c, internal.MinKey(c.namespace, c.workflow, c.run, c.iter+1), message)
	})
	ensure(c, func() error {
		return c.cl.CompleteRun(c, c.namespace, c.workflow, c.run, c.iter)
	})
	panic(restart)
}

func (c *RunContext) ExecActivity(activityFunc interface{}, message proto.Message, opts ...option) proto.Message {
	if err := validateActivity(activityFunc); err != nil {
		panic(err)
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	activity := o.nameFunc(activityFunc)

	index := c.state.GetAndInc(activity)
	key := internal.Key{
		Namespace: c.namespace,
		Workflow:  c.workflow,
		Run:       c.run,
		Iteration: c.iter,
		Activity:  activity,
		Sequence:  fmt.Sprint(index),
	}

	ensure(c, func() error {
		return c.cl.RequestActivity(c, key.Encode(), message)
	})

	res := c.state.AwaitActivity(c, key)
	c.lastEvent = res.event
	return res.message
}

func (c *RunContext) AwaitSignal(s Signal, duration time.Duration) (proto.Message, bool) {
	activity := internal.ActivitySignal
	seq := internal.SignalSequence{
		SignalType: s.SignalType(),
		Index:      c.state.GetAndInc(fmt.Sprintf("%s:%d", activity, s.SignalType())),
	}

	key := internal.Key{
		Namespace: c.namespace,
		Workflow:  c.workflow,
		Run:       c.run,
		Iteration: c.iter,
		Activity:  activity,
		Sequence:  seq.Encode(),
	}
	ensure(c, func() error {
		return c.cl.RequestActivity(c, key.Encode(), &replaypb.SleepRequest{
			Duration: ptypes.DurationProto(duration),
		})
	})

	res := c.state.AwaitActivity(c, key)
	c.lastEvent = res.event
	if _, ok := res.message.(*replaypb.SleepDone); ok {
		return nil, false
	}
	return res.message, true
}

func (c *RunContext) Sleep(duration time.Duration) {
	activity := internal.ActivitySleep
	index := c.state.GetAndInc(activity)
	key := internal.Key{
		Namespace: c.namespace,
		Workflow:  c.workflow,
		Run:       c.run,
		Iteration: c.iter,
		Activity:  activity,
		Sequence:  fmt.Sprint(index),
	}

	ensure(c, func() error {
		return c.cl.RequestActivity(c, key.Encode(), &replaypb.SleepRequest{
			Duration: ptypes.DurationProto(duration),
		})
	})

	c.state.AwaitActivity(c, key)
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
		if ctx.Err() != nil {
			panic(cancel)
		} else if err != nil {
			// NoReturnErr: Log and try again.
			log.Error(ctx, errors.Wrap(err, "ensure"))
			time.Sleep(time.Second)
			continue
		}
		return
	}
}

func validateActivity(activityFunc interface{}) error {
	if activityFunc == nil {
		return errors.New("nil activity function")
	}

	t := reflect.TypeOf(activityFunc)

	if t.Kind() != reflect.Func {
		return errors.New("non-function activity function")
	}

	if !checkParams(t.NumIn, t.In, ctxType, anyType, fateType, protoType) {
		return errors.New("invalid activity function, input parameters not " +
			"context.Context, interface{}, fate.Fate, proto.Message: " + t.String())
	}

	if !checkParams(t.NumOut, t.Out, protoType, errorType) {
		return errors.New("invalid activity function, output parameters not " +
			"proto.Message, error: " + t.String())
	}

	return nil
}

func validateWorkflow(workflowFunc interface{}) error {
	if workflowFunc == nil {
		return errors.New("nil workflow function")
	}

	t := reflect.TypeOf(workflowFunc)

	if t.Kind() != reflect.Func {
		return errors.New("non-function workflow function")
	}

	if !checkParams(t.NumIn, t.In, anyType, protoType) || t.In(0) != runCtxType {
		return errors.New("invalid workflow function, input parameters not " +
			"replay.RunContext, proto.Message: " + t.String())
	}

	if !checkParams(t.NumOut, t.Out) {
		return errors.New("invalid workflow function, output parameters not empty: " +
			t.String())
	}

	return nil
}

func checkParams(num func() int, get func(int) reflect.Type, types ...reflect.Type) bool {
	if num() != len(types) {
		return false
	}
	for i, typ := range types {
		if !get(i).Implements(typ) {
			return false
		}
	}
	return true
}

var ctxType = reflect.TypeOf((*context.Context)(nil)).Elem()
var fateType = reflect.TypeOf((*fate.Fate)(nil)).Elem()
var protoType = reflect.TypeOf((*proto.Message)(nil)).Elem()
var errorType = reflect.TypeOf((*error)(nil)).Elem()
var anyType = reflect.TypeOf((*interface{})(nil)).Elem()
var runCtxType = reflect.TypeOf(RunContext{})
