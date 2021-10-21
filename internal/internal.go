// Package internal provides the replay internal types and helper functions. It is used both by the replay server
// as well as the replay sdk.
package internal

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/reflex"
	"github.com/luno/reflex/reflexpb"
)

const ActivitySleep = "replay_sleep"
const ActivitySignal = "replay_signal"

// ErrDuplicate indicates a specific action (like RunWorkflow or SignalRun) has already been performed.
var ErrDuplicate = errors.New("duplicate entry", j.C("ERR_96713b1c52c5d59f"))

type EventType int

func (e EventType) ReflexType() int {
	return int(e)
}

//go:generate stringer -type=EventType

const (
	RunCreated       EventType = 1
	RunCompleted     EventType = 2
	RunOutput        EventType = 3
	ActivityRequest  EventType = 4
	ActivityResponse EventType = 5
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

	var a any.Any
	if err := proto.Unmarshal(e.MetaData, &a); err != nil {
		return nil, errors.Wrap(err, "unmarshal proto")
	}

	var d ptypes.DynamicAny
	if err := ptypes.UnmarshalAny(&a, &d); err != nil {
		return nil, errors.Wrap(err, "unmarshal anypb")
	}

	return d.Message, nil
}

type SignalSequence struct {
	Signal string
	Index  int
}

func (s SignalSequence) Encode() string {
	return fmt.Sprintf("%s:%d", s.Signal, s.Index)
}

func DecodeSignalSequence(sequence string) (SignalSequence, error) {
	split := strings.Split(sequence, ":")
	if len(split) < 2 {
		return SignalSequence{}, errors.New("invalid sequence")
	}

	index, err := strconv.Atoi(split[1])
	if err != nil {
		return SignalSequence{}, errors.New("invalid sequence")
	}

	return SignalSequence{
		Signal: split[0],
		Index:  index,
	}, nil
}

type Key struct {
	Namespace string
	Workflow  string
	Run       string
	Iteration int
	Target    string
	Sequence  string
}

func (k Key) Encode() string {
	return path.Join(k.Namespace, k.Workflow, k.Run, strconv.Itoa(k.Iteration), k.Target, k.Sequence)
}

func DecodeKey(key string) (Key, error) {
	split := strings.Split(key, "/")
	if len(split) < 4 {
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
		} else {
			k.Sequence = s
		}
	}
	return k, nil
}

func EventToProto(e *reflex.Event) (*reflexpb.Event, error) {
	ts, err := ptypes.TimestampProto(e.Timestamp)
	if err != nil {
		return nil, err
	}

	return &reflexpb.Event{
		Id:        e.ID,
		ForeignId: e.ForeignID,
		Type:      int32(e.Type.ReflexType()),
		Timestamp: ts,
		Metadata:  e.MetaData,
	}, nil
}

func EventFromProto(e *reflexpb.Event) (*reflex.Event, error) {
	ts, err := ptypes.Timestamp(e.Timestamp)
	if err != nil {
		return nil, err
	}

	return &reflex.Event{
		ID:        e.Id,
		ForeignID: e.ForeignId,
		Type:      eventType(e.Type),
		Timestamp: ts,
		MetaData:  e.Metadata,
	}, nil
}

func MinKey(namespace, workflow, run string, iteration int) Key {
	return Key{
		Namespace: namespace,
		Workflow:  workflow,
		Run:       run,
		Iteration: iteration,
	}
}

type eventType int

func (e eventType) ReflexType() int {
	return int(e)
}

func ToAny(message proto.Message) (*any.Any, error) {
	if message == nil {
		return nil, nil
	}

	return ptypes.MarshalAny(message)
}

func Marshal(message proto.Message) ([]byte, error) {
	if message == nil {
		return nil, nil
	}

	return proto.Marshal(message)
}

// Client defines the replay server's internal API. It may only be used by the replay package itself.
type Client interface {
	// RequestActivity inserts a ActivityRequest event.
	RequestActivity(ctx context.Context, key Key, message proto.Message) error

	// RespondActivity inserts a ActivityResponse event.
	RespondActivity(ctx context.Context, key Key, message proto.Message) error

	// EmitOutput insets a RunOutput event.
	EmitOutput(ctx context.Context, key Key, message proto.Message) error

	// CompleteRun inserts a RunComplete event.
	CompleteRun(ctx context.Context, key Key) error

	// RestartRun completes the current run iteration and start the next iteration with the provided message.
	RestartRun(ctx context.Context, key Key, message proto.Message) error

	// ListBootstrapEvents returns the boostrap events for the run before hte provide event ID.
	ListBootstrapEvents(ctx context.Context, key Key, before string) ([]reflex.Event, error)
}
