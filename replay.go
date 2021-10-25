// Package replay provides a workflow framework that is robust with respect to temporary errors.
//
// This package presents the replay sdk that the user of the replay framework uses. It internally
// uses the internal package.
package replay

import (
	"context"
	"flag"
	"fmt"
	"path"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/corverroos/replay/internal"
	"github.com/corverroos/replay/internal/replaypb"
	"github.com/luno/fate"
	"github.com/luno/jettison"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

var debug = flag.Bool("replay_debug", false, "Verbose logging for debugging purposes")

var errLogicChanged = errors.New("workflow logic changed", j.C("ERR_277dc6c2c7014a6f"))
var errCtxCancel = errors.New("run goroutine ctx cancelled", j.C("ERR_f5e064b4f7e2ab1a"))
var errRestart = errors.New("run restarted ", j.C("ERR_dac36478f37775c1"))
var errAckTimeout = errors.New("timeout waiting for ack ", j.C("ERR_20099904c7fa4477"))

// Client defines the main replay sdk API.
type Client interface {
	// RunWorkflow returns true if a RunCreated event
	// was inserted which will result in an invocation
	// of the workflow with the message.
	//
	// The run identifier must be unique otherwise false is returned
	// since the run was already created.
	RunWorkflow(ctx context.Context, namespace, workflow, run string, message proto.Message) (bool, error)

	// SignalRun returns true of the signal was inserted which will result
	// in the signal being available to the run if it subsequently calls ctx.AwaitSignal.
	//
	// External ID must be unique per namespace and workflow otherwise false is returned since
	// the signal was already created.
	SignalRun(ctx context.Context, namespace, workflow, run string, signal string, message proto.Message, extID string) (bool, error)

	// Stream returns a replay events stream function with optional namespace, workflow and run filters.
	// Note that empty filters can have a negative performance impact.
	Stream(namespace, workflow, run string) reflex.StreamFunc

	// Internal returns the internal replay API. This should only be used by this replay package itself.
	Internal() internal.Client
}

// RegisterActivity starts a activity consumer that consumes replay events and executes the activity if requested.
func RegisterActivity(getCtx func() context.Context, cl Client, cstore reflex.CursorStore, backends interface{}, namespace string, activityFunc interface{}, opts ...Option) {
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

		if key.Target != activity || key.Namespace != namespace || !o.shardFunc(key.Run) {
			return nil
		}

		// TODO(corver): Dont re-execute activity if ActivityResponse event already
		//  present (e.g. when reprocessing due to re-shard cursor reset).

		message, err := internal.ParseMessage(e)
		if err != nil {
			return err
		}

		ctx = log.ContextWith(ctx, j.KS("replay_run", key.Run))

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

		return cl.Internal().InsertEvent(ctx, internal.ActivityResponse, key, respVals[0].Interface().(proto.Message))
	}

	name := path.Join("replay_activity", namespace, activity, o.shardName)
	consumer := reflex.NewConsumer(name, fn, o.consumerOpts...)
	spec := reflex.NewSpec(cl.Stream(namespace, "", ""), cstore, consumer)
	go rpatterns.RunForever(getCtx, spec)
}

// RegisterWorkflow starts a workflow consumer that consumes replay events and executes the workflow.
// It maintains a goroutine for each run when started or when an activity response is received.
func RegisterWorkflow(getCtx func() context.Context, cl Client, cstore reflex.CursorStore, namespace string, workflowFunc interface{}, opts ...Option) {
	if err := validateWorkflow(workflowFunc); err != nil {
		panic(err)
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	workflow := o.nameFunc(workflowFunc)
	metrics := o.workflowMetrics(namespace, workflow)

	s := wcState{
		cl:           cl.Internal(),
		metrics:      metrics,
		namespace:    namespace,
		workflow:     workflow,
		workflowFunc: workflowFunc,
		awaitTimeout: o.awaitTimeout,
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

		if key.Workflow != workflow || key.Namespace != namespace || !o.shardFunc(key.Run) {
			return nil
		}

		logDebug(ctx, "workflow consuming event", j.MKV{"event": e.ID, "key": e.ForeignID,
			"type": internal.EventType(e.Type.ReflexType())})

		message, err := internal.ParseMessage(e)
		if err != nil {
			return err
		}

		switch e.Type.ReflexType() {
		case internal.RunCreated.ReflexType():
			metrics.IncStart()
			logDebug(ctx, "starting run", j.KS("key", key.Encode()))
			_, err := s.StartRun(ctx, e, key, message, nil)
			return err
		case internal.ActivityResponse.ReflexType(), internal.SleepResponse.ReflexType():
			_, err := s.RespondActivity(ctx, e, key, message, false)
			return err
		case internal.RunSignal.ReflexType():
			_, err := s.EnqueueSignal(ctx, e, key, message, false)
			return err
		case internal.ActivityRequest.ReflexType():
			return nil
		case internal.RunCompleted.ReflexType():
			return nil
		case internal.RunOutput.ReflexType():
			return nil
		case internal.SleepRequest.ReflexType():
			return nil
		case internal.NoopEvent.ReflexType():
			return nil
		default:
			return errors.New("bug: unknown type", j.KV("type", e.Type))
		}
	}

	name := path.Join("replay_workflow", namespace, workflow, o.shardName)
	consumer := reflex.NewConsumer(name, fn, o.consumerOpts...)
	spec := reflex.NewSpec(cl.Stream(namespace, workflow, ""), cstore, consumer)
	go rpatterns.RunForever(getCtx, spec)
}

// runState contains the state of a run goroutine.
type runState struct {
	// bufferedSignals buffer all received bufferedSignals.
	bufferedSignals map[string][]response
	awaitingSignal  *internal.Key
	signalsMu       sync.Mutex

	//  awaitTimeout defines the duration the run goroutine will wait for an activity response after
	// which it will exit. In this case, the workflow consumer will bootstrap it when the response is received.
	awaitTimeout time.Duration

	//  resChan is used by the workflow consumer to pass activity responses and bufferedSignals to the run goroutine.
	resChan chan response

	//  ackChan is used by the run goroutine to signal the main workflow consumer
	// that it has progressed to the next checkpoint (await or complete) or not (panic or cancel).
	ackChan chan ack

	// clearBootstrap is called by the workflow consumer when it has completed bootstrapping.
	clearBootstrap func()
}

// response is an activity response passed from the workflow consumer to a run goroutine.
type response struct {
	key     internal.Key
	message proto.Message
	event   *reflex.Event
}

//- when run goroutine calls awaitsignal
//-- if buffered, pop and return (no ack)
//-- else enqueue sleep activity
//-- then ack and wait for signal or sleep response
//
//- when workflow consumes signal
//-- if not waiting for this signal, buffer and return
//-- else handoff and wait for ack
//
//- when workflow consumes signal sleep timeout
//-- if not waiting for this signal, drop and return
//-- else handoff and wait for ack

// ack is a signal from a run goroutine to the workflow consumer that it is progressed to
// a checkpoint.
type ack struct {
	await    bool
	complete bool
	panic    bool
	cancel   bool
}

func (s *runState) MaybePopSignal(signal string) (response, bool) {
	s.signalsMu.Lock()
	defer s.signalsMu.Unlock()

	sl := s.bufferedSignals[signal]
	if len(sl) == 0 {
		return response{}, false
	}

	s.bufferedSignals[signal] = sl[1:]

	return sl[0], true
}

func (s *runState) IsAwaitingSignal(key internal.Key, incomingSignal bool) bool {
	s.signalsMu.Lock()
	defer s.signalsMu.Unlock()

	if s.awaitingSignal == nil {
		return false
	}

	if incomingSignal {
		// For incoming signals, we only compare target.
		return s.awaitingSignal.Target == key.Target
	}
	// For sleep responses, we compare the whole key.
	return *s.awaitingSignal == key
}

func (s *runState) AwaitSignal(ctx context.Context, signal string, key internal.Key) response {
	s.signalsMu.Lock()
	s.awaitingSignal = &key
	s.signalsMu.Unlock()
	defer func() {
		s.signalsMu.Lock()
		s.awaitingSignal = nil
		s.signalsMu.Unlock()
	}()

	s.ackChan <- ack{await: true}

	ctx, cancelFunc := context.WithTimeout(ctx, s.awaitTimeout)
	defer cancelFunc()

	select {
	case <-ctx.Done():
		panic(errors.Wrap(errCtxCancel, "awaiting signal", j.KS("signal", signal)))
	case r := <-s.resChan:
		if r.key.Target == internal.SleepTarget && r.key != key {
			panic(errors.New("bug: unexpected signal sleep", j.MKS{"want": key.Encode(), "got": r.key.Encode()}))
		} else if r.key.Target != internal.SleepTarget && r.key.Target != key.Target {
			panic(errors.New("bug: unexpected signal", j.MKS{"want": signal, "got": key.Target}))
		}
		return r
	}
}

func (s *runState) AwaitActivity(ctx context.Context, key internal.Key) response {
	s.ackChan <- ack{await: true}

	ctx, cancelFunc := context.WithTimeout(ctx, s.awaitTimeout)
	defer cancelFunc()

	select {
	case <-ctx.Done():
		panic(errors.Wrap(errCtxCancel, "awaiting response", j.KS("key", key.Encode())))
	case r := <-s.resChan:
		if r.key != key {
			panic(errors.Wrap(errLogicChanged, "awaiting response", j.MKS{"want": key.Encode(), "got": r.key.Encode()}))
		}
		return r
	}
}

// wcState represents the workflow consumer state.
type wcState struct {
	sync.Mutex

	cl           internal.Client
	namespace    string
	workflow     string
	workflowFunc interface{}
	metrics      Metrics
	awaitTimeout time.Duration

	runs map[string]*runState
}

func (s *wcState) ClearBootstrap(key internal.Key) error {
	s.Lock()
	defer s.Unlock()

	rs, ok := s.runs[key.Run]
	if !ok {
		return errors.New("bug: cannot clear bootstrap")
	}

	rs.clearBootstrap()

	return nil
}

// StartRun starts a run goroutine and registers it with the wcState. The run goroutine will call
// the workflow function with the provided message. It blocks until an ack is received from the run goroutine.
// It returns true if the run is still active (awaiting first activity response).
//
// TODO(corver): Dont start a run if a RunCompleted event already present (when reprocessing due to re-shard cursor reset).
func (s *wcState) StartRun(ctx context.Context, e *reflex.Event, key internal.Key,
	message proto.Message, bootstrapEvents []reflex.Event) (bool, error) {

	s.Lock()
	defer s.Unlock()

	if _, ok := s.runs[key.Run]; ok {
		return false, errors.New("bug: run already started")
	}

	rs := &runState{
		resChan:         make(chan response), // Must be blocking, so workflow consumer knows that it handed over responses.
		ackChan:         make(chan ack, 1),
		bufferedSignals: make(map[string][]response),
		awaitTimeout:    s.awaitTimeout,
	}
	s.runs[key.Run] = rs

	go func() {
		rctx := log.ContextWith(ctx, j.KS("replay_run", key.Run))
		defer func() {
			if v := recover(); v != nil {
				if err, ok := v.(error); !ok {
					log.Error(rctx, errors.New("run panic", j.MKS{"key": key.Encode(), "panic": fmt.Sprint(v)}))
					rs.ackChan <- ack{panic: true}
				} else if errors.Is(err, errCtxCancel) {
					logDebug(rctx, "run context cancelled", log.WithError(err))
					rs.ackChan <- ack{cancel: true} // NoReturnErr: Return via ack
				} else if errors.Is(err, errRestart) {
					logDebug(rctx, "run restarted", j.KS("key", key.Encode()))
					rs.ackChan <- ack{complete: true} // NoReturnErr: Return via ack
				} else {
					log.Error(rctx, errors.Wrap(err, "unexpected panic error"))
					rs.ackChan <- ack{panic: true} // NoReturnErr: Return via ack
				}
			} else {
				logDebug(rctx, "run completed", j.KS("key", key.Encode()))
				rs.ackChan <- ack{complete: true}
			}

			s.Lock()
			delete(s.runs, key.Run)
			s.Unlock()
		}()

		rc := &RunContext{
			Context:     ctx,
			namespace:   s.namespace,
			workflow:    s.workflow,
			run:         key.Run,
			iter:        key.Iteration,
			state:       rs,
			createEvent: e,
			lastEvent:   e,
			indexes:     make(map[tuple]int),
			writer:      &eventWriter{bootstrapEvents: bootstrapEvents, cl: s.cl},
		}

		rs.clearBootstrap = rc.writer.ClearBootstrap

		reflect.ValueOf(s.workflowFunc).Call([]reflect.Value{
			reflect.ValueOf(rc),
			reflect.ValueOf(message),
		})

		rc.writer.CompleteRun(rctx, internal.RunKey(s.namespace, s.workflow, key.Run, key.Iteration))

		s.metrics.IncComplete(time.Since(e.Timestamp))
	}()

	return awaitAck(ctx, rs)
}

// bootstrapRun bootstraps a previously started run by replaying all previous events. It returns
// true if the run is still active after bootstrapping.
func (s *wcState) bootstrapRun(ctx context.Context, run string, iter int, to string) (bool, error) {
	el, err := s.cl.ListBootstrapEvents(ctx, internal.RunKey(s.namespace, s.workflow, run, iter), to)
	if err != nil {
		return false, errors.Wrap(err, "list responses")
	}

	for i, e := range el {
		if reflex.IsType(e.Type, internal.RunCompleted) {
			// Run already completed, do not bootstrap.
			if i != len(el)-1 {
				// Complete event in middle of bootstrap list means logic changed
				log.Error(ctx, errors.New("completed run ignoring activity response", j.KS("key", e.ForeignID)))
			}
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

		if i == 0 && !reflex.IsType(e.Type, internal.RunCreated) {
			return false, errors.New("bug: unexpected first event", j.KV("type", e.Type))
		}

		if e.ID == to {
			err := s.ClearBootstrap(key)
			if err != nil {
				return false, err
			}
		}

		if reflex.IsType(e.Type, internal.RunCreated) {
			active, err := s.StartRun(ctx, &e, key, message, el)
			if err != nil {
				return active, err
			} else if !active {
				// Workflow logic changed: run completed during bootstrap. Skip rest of events.
				log.Error(ctx, errors.New("run completed during bootstrap"))
				return false, nil
			}

		} else if reflex.IsAnyType(e.Type, internal.ActivityResponse, internal.SleepResponse) {
			active, err := s.RespondActivity(ctx, &e, key, message, true)
			if err != nil {
				return false, err
			} else if !active {
				if e.ID != to {
					// Workflow logic changed: run completed during bootstrap. Skip rest of events.
					log.Error(ctx, errors.New("run completed during bootstrap"))
				}
				return false, nil
			}

		} else if reflex.IsType(e.Type, internal.RunSignal) {
			active, err := s.EnqueueSignal(ctx, &e, key, message, true)
			if err != nil {
				return false, err
			} else if !active {
				if e.ID != to {
					// Workflow logic changed: run completed during bootstrap. Skip rest of events.
					log.Error(ctx, errors.New("run completed during bootstrap"))
				}
				return false, nil
			}
		}

		if e.ID == to {
			break
		}
	}

	return true, nil
}

// RespondActivity hands the activity response over to the run goroutine and blocks until it acks.
// If the run is not registered with this wcState, it is bootstrapped.
// It returns true if the run is still active afterwards (waiting for the subsequent activity response).
func (s *wcState) RespondActivity(ctx context.Context, e *reflex.Event, key internal.Key, message proto.Message, bootstrap bool) (bool, error) {
	s.Lock()

	if _, ok := s.runs[key.Run]; !ok {
		// This run was previously started and is now continuing; bootstrap it.
		s.Unlock()

		if bootstrap {
			return false, errors.New("bug: recursive bootstrapping")
		}
		logDebug(ctx, "bootstrapping run", j.KS("key", key.Encode()))

		return s.bootstrapRun(ctx, key.Run, key.Iteration, e.ID)
	}

	rs := s.runs[key.Run]
	s.Unlock()

	// Signal sleep responses are special, drop them if the goroutine isn't waiting anymore.
	if reflex.IsType(e.Type, internal.SleepResponse) && key.Target != internal.SleepTarget && !rs.IsAwaitingSignal(key, false) {
		fmt.Printf("JCR: dropping ignored sleep=%+v\n", key.Encode())
		return true, nil
	}

	ok := handOff(ctx, rs.resChan, response{key: key, message: message, event: e})

	active, err := awaitAck(ctx, rs)
	if !ok && (err == nil || errors.Is(err, errAckTimeout)) {
		return false, errors.New("bug: run goroutine not waiting nor acked with an error")
	}
	return active, err
}

func (s *wcState) EnqueueSignal(ctx context.Context, e *reflex.Event, key internal.Key, message proto.Message, bootstrap bool) (bool, error) {
	s.Lock()

	if _, ok := s.runs[key.Run]; !ok {
		// This run received a signal, but isn't currently running, try to bootstrap it.
		s.Unlock()

		if bootstrap {
			return false, errors.New("bug: recursive bootstrapping")
		}
		logDebug(ctx, "bootstrapping run", j.KS("key", key.Encode()))

		return s.bootstrapRun(ctx, key.Run, key.Iteration, e.ID)
	}

	rs := s.runs[key.Run]
	s.Unlock()

	if !rs.IsAwaitingSignal(key, false) {
		// Just buffer the signal and return
		rs.bufferedSignals[key.Target] = append(rs.bufferedSignals[key.Target], response{
			key:     key,
			message: message,
			event:   e,
		})

		return true, nil
	}

	ok := handOff(ctx, rs.resChan, response{key: key, message: message, event: e})

	active, err := awaitAck(ctx, rs)
	if !ok && (err == nil || errors.Is(err, errAckTimeout)) {
		return false, errors.New("bug: run goroutine not waiting nor acked with an error")
	}
	return active, err
}

// RunContext provides the replay API for a workflow function.
type RunContext struct {
	context.Context
	namespace string
	workflow  string
	run       string
	iter      int
	state     *runState
	writer    *eventWriter

	createEvent *reflex.Event
	lastEvent   *reflex.Event

	//  indexes maintains the request index/sequence per target. The nth time a run calls an activity/output.
	indexes map[tuple]int
}

// Restart completes the current run iteration and start the next iteration with the provided message.
func (c *RunContext) Restart(message proto.Message) {
	// TODO(corver): Maybe add Option to transfer bufferedSignals.

	c.writer.RestartRun(c, internal.RunKey(c.namespace, c.workflow, c.run, c.iter), message)

	panic(errRestart)
}

// ExecActivity results in the activity being called asynchronously
// with the provided parameter and returns the response once available.
func (c *RunContext) ExecActivity(activityFunc interface{}, message proto.Message, opts ...Option) proto.Message {
	if err := validateActivity(activityFunc); err != nil {
		panic(err)
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	activity := o.nameFunc(activityFunc)

	key := internal.Key{
		Namespace: c.namespace,
		Workflow:  c.workflow,
		Run:       c.run,
		Iteration: c.iter,
		Target:    activity,
		Sequence:  c.getAndInc(internal.ActivityRequest, activity),
	}

	c.writer.InsertEvent(c, internal.ActivityRequest, key, message)

	res := c.state.AwaitActivity(c, key)
	c.lastEvent = res.event
	return res.message
}

// AwaitSignal blocks and returns true when this type of signal is/was
// received for this run. If no signal is/was received it returns false after d duration.
func (c *RunContext) AwaitSignal(signal string, duration time.Duration) (proto.Message, bool) {

	if res, ok := c.state.MaybePopSignal(signal); ok {
		// We have a buffered signal ready!
		return res.message, true
	}

	key := internal.Key{
		Namespace: c.namespace,
		Workflow:  c.workflow,
		Run:       c.run,
		Iteration: c.iter,
		Target:    signal,
		Sequence:  c.getAndInc(internal.SleepRequest, signal),
	}

	c.writer.InsertEvent(c, internal.SleepRequest, key, &replaypb.SleepRequest{
		Duration: durationpb.New(duration),
	})

	res := c.state.AwaitSignal(c, signal, key)
	c.lastEvent = res.event
	if _, ok := res.message.(*replaypb.SleepDone); ok {
		return nil, false
	}
	return res.message, true
}

// EmitOutput stores the output in the event log and returns on success.
func (c *RunContext) EmitOutput(output string, message proto.Message) {
	key := internal.Key{
		Namespace: c.namespace,
		Workflow:  c.workflow,
		Run:       c.run,
		Iteration: c.iter,
		Target:    output,
		Sequence:  c.getAndInc(internal.RunOutput, output),
	}
	c.writer.InsertEvent(c, internal.RunOutput, key, message)
}

// Sleep blocks for at least d duration.
//
// Note that replay sleeps aren't very accurate and
// a few seconds is the practical minimum.
func (c *RunContext) Sleep(duration time.Duration) {
	key := internal.Key{
		Namespace: c.namespace,
		Workflow:  c.workflow,
		Run:       c.run,
		Iteration: c.iter,
		Target:    internal.SleepTarget,
		Sequence:  c.getAndInc(internal.SleepRequest, internal.SleepTarget),
	}

	c.writer.InsertEvent(c, internal.SleepRequest, key, &replaypb.SleepRequest{
		Duration: durationpb.New(duration),
	})

	c.state.AwaitActivity(c, key)
}

// CreateEvent returns the reflex event that started the run iteration (type is internal.RunCreated).
// The event timestamp could be used to reason about run age.
func (c *RunContext) CreateEvent() *reflex.Event {
	return c.createEvent
}

// LastEvent returns the latest reflex event (type is either internal.RunCreated or internal.ActivityResponse).
// The event timestamp could be used to reason about run age.
func (c *RunContext) LastEvent() *reflex.Event {
	return c.lastEvent
}

// Run returns the run name/identifier.
func (c *RunContext) Run() string {
	return c.run
}

type tuple struct {
	Type   internal.EventType
	Target string
}

// getAndInc returns the current target index value and then increments it.
func (c *RunContext) getAndInc(typ internal.EventType, target string) string {
	t := tuple{Type: typ, Target: target}

	defer func() {
		c.indexes[t]++
	}()

	return strconv.Itoa(c.indexes[t])
}

// awaitAck waits for an ack from the run goroutine.
// It returns true if the run is still active; awaiting next activity response.
func awaitAck(ctx context.Context, rs *runState) (bool, error) {
	for {
		var a ack
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(time.Minute):
			return false, errors.Wrap(errAckTimeout, "")
		case a = <-rs.ackChan:
		}

		if a.await {
			return true, nil
		} else if a.complete {
			return false, nil
		} else if a.cancel {
			return false, errors.New("run cancelled")
		} else if a.panic {
			return false, errors.New("run panic")
		} else {
			return false, errors.New("bug: invalid ack")
		}
	}
}

// handOff returns true if the response was handed off to the run goroutine.
// It returns false if the context was cancelled or after a timeout.
func handOff(ctx context.Context, ch chan<- response, r response) bool {
	select {
	case ch <- r:
		// Handoff successful
		return true
	case <-ctx.Done():
		return false
	case <-time.After(time.Second):
		// Run goroutine not waiting for response, expect an error.
		//
		// Note that run goroutines can timeout while waiting, so there is always a probability that these errors
		// can occur. Maybe add support to handle it gracefully without an error.
		//
		// We have to allow some time though since there is a delay after the run goroutine acks that it is waiting for a response
		// and before it blocks receiving on the response channel.
		return false
	}
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

type eventWriter struct {
	cl              internal.Client
	bootstrapEvents []reflex.Event
}

func (w *eventWriter) ClearBootstrap() {
	w.bootstrapEvents = nil
}

func (w *eventWriter) InsertEvent(ctx context.Context, typ internal.EventType, key internal.Key, msg proto.Message) {
	if len(w.bootstrapEvents) > 0 {
		k := key.Encode()

		// Skip event if part of bootstrap list.
		for _, e := range w.bootstrapEvents {
			if e.ForeignID == k {
				return
			}
		}

		// Only allow new outputs during bootstrap.
		if typ != internal.RunOutput {
			panic(errors.Wrap(errLogicChanged, "unexpected event during bootstrap", j.KS("key", k)))
		}
	}

	ensure(ctx, func() error {
		return w.cl.InsertEvent(ctx, typ, key, msg)
	})
}

func (w *eventWriter) CompleteRun(ctx context.Context, key internal.Key) {
	ensure(ctx, func() error {
		return w.cl.CompleteRun(ctx, key)
	})
}

func (w *eventWriter) RestartRun(ctx context.Context, key internal.Key, msg proto.Message) {
	ensure(ctx, func() error {
		return w.cl.RestartRun(ctx, key, msg)
	})
}

// ensure retries the function until no error is returned or the context is canceled.
func ensure(ctx context.Context, fn func() error) {
	for {
		err := fn()
		if ctx.Err() != nil {
			panic(errors.Wrap(errCtxCancel, "ensuring"))
		} else if err != nil {
			// NoReturnErr: Log and try again.
			log.Error(ctx, errors.Wrap(err, "ensure"))
			time.Sleep(time.Second)
			continue
		}
		return
	}
}

// validateActivity returns an error if the activity function signature isn't valid.
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

// validateWorkflow returns an error if the workflow function signature isn't valid.
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
			"*replay.RunContext, proto.Message: " + t.String())
	}

	if !checkParams(t.NumOut, t.Out) {
		return errors.New("invalid workflow function, output parameters not empty: " +
			t.String())
	}

	return nil
}

func logDebug(ctx context.Context, msg string, opts ...jettison.Option) {
	if !*debug {
		return
	}
	log.Info(ctx, msg, opts...)
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
var runCtxType = reflect.TypeOf(new(RunContext))
