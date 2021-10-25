// Package internal provides the replay internal types and helper functions. It is used both by the replay server
// as well as the replay sdk.
package internal

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/reflex"
	"github.com/luno/reflex/reflexpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const SleepTarget = "replay_sleep"

// const ActivitySignalSleep = "replay_signal_sleep"

// ErrDuplicate indicates a specific action (like RunWorkflow or SignalRun) has already been performed.
var ErrDuplicate = errors.New("duplicate entry", j.C("ERR_96713b1c52c5d59f"))

type EventType int

func (e EventType) ReflexType() int {
	return int(e)
}

//go:generate stringer -type=EventType

const (
	NoopEvent        EventType = 0
	RunCreated       EventType = 1
	RunCompleted     EventType = 2
	RunOutput        EventType = 3
	RunSignal        EventType = 4
	ActivityRequest  EventType = 5
	ActivityResponse EventType = 6
	SleepRequest     EventType = 7
	SleepResponse    EventType = 8
)

// ParseMessage returns the typed proto message of the event.
//
// Note that this fails if the actual proto definition is not registered
// in this binary. It should therefore only be called on
// the client side and only for events related to the specific client.
func ParseMessage(e *reflex.Event) (proto.Message, error) {
	if len(e.MetaData) == 0 {
		return nil, nil
	}

	var a anypb.Any
	if err := proto.Unmarshal(e.MetaData, &a); err != nil {
		return nil, errors.Wrap(err, "unmarshal proto")
	}

	return a.UnmarshalNew()
}

type Key struct {
	Namespace string
	Workflow  string
	Run       string
	Iteration int // -1 for incoming signals
	Target    string
	Sequence  string
}

func (k Key) Encode() string {
	if k.Sequence != "" && k.Target == "" {
		panic("invalid key: " + fmt.Sprint(k))
	}
	return path.Join(k.Namespace, k.Workflow, k.Run, strconv.Itoa(k.Iteration), k.Target, k.Sequence)
}

func DecodeKey(key string) (Key, error) {
	split := strings.Split(key, "/")
	if len(split) < 4 || len(split) > 6 {
		return Key{}, errors.New("invalid key")
	}
	var k Key
	for i, s := range split {
		if i == 0 {
			k.Namespace = s
		} else if i == 1 {
			k.Workflow = s
		} else if i == 2 {
			k.Run = s
		} else if i == 3 {
			var err error
			k.Iteration, err = strconv.Atoi(s)
			if err != nil {
				return Key{}, err
			}
		} else if i == 4 {
			k.Target = s
		} else if i == 5 {
			k.Sequence = s
		}
	}
	return k, nil
}

func EventToProto(e *reflex.Event) (*reflexpb.Event, error) {
	return &reflexpb.Event{
		Id:        e.ID,
		ForeignId: e.ForeignID,
		Type:      int32(e.Type.ReflexType()),
		Timestamp: timestamppb.New(e.Timestamp),
		Metadata:  e.MetaData,
	}, nil
}

func EventFromProto(e *reflexpb.Event) (*reflex.Event, error) {
	return &reflex.Event{
		ID:        e.Id,
		ForeignID: e.ForeignId,
		Type:      eventType(e.Type),
		Timestamp: e.Timestamp.AsTime(),
		MetaData:  e.Metadata,
	}, nil
}

// RunKey returns a key for run iteration level events; Create/Complete/Restart.
func RunKey(namespace, workflow, run string, iteration int) Key {
	return Key{
		Namespace: namespace,
		Workflow:  workflow,
		Run:       run,
		Iteration: iteration,
	}
}

// SignalKey returns a key for run signal event.
func SignalKey(namespace, workflow, run string, signal string, extID string) Key {
	return Key{
		Namespace: namespace,
		Workflow:  workflow,
		Run:       run,
		Iteration: -1,
		Target:    signal,
		Sequence:  extID,
	}
}

type eventType int

func (e eventType) ReflexType() int {
	return int(e)
}

func ToAny(message proto.Message) (*anypb.Any, error) {
	if message == nil {
		return nil, nil
	}

	return anypb.New(message)
}

func Marshal(message proto.Message) ([]byte, error) {
	if message == nil {
		return nil, nil
	}

	return proto.Marshal(message)
}

// Client defines the replay server's internal API. It may only be used by the replay package itself.
type Client interface {
	// InsertEvent inserts a event.
	InsertEvent(ctx context.Context, typ EventType, key Key, message proto.Message) error

	// CompleteRun inserts a RunComplete event.
	CompleteRun(ctx context.Context, key Key) error

	// RestartRun completes the current run iteration and start the next iteration with the provided message.
	RestartRun(ctx context.Context, key Key, message proto.Message) error

	// ListBootstrapEvents returns the boostrap events up to (including) the provide event ID for the run.
	ListBootstrapEvents(ctx context.Context, key Key, to string) ([]reflex.Event, error)
}
