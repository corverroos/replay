package internal

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"github.com/luno/reflex/reflexpb"
)

const (
	ActivitySleep  = "replay_sleep"
	ActivitySignal = "replay_signal"

	paramActivity = "a"
	paramSequence = "n"
)

type EventType int

func (e EventType) ReflexType() int {
	return int(e)
}

const (
	CreateRun   EventType = 1
	CompleteRun EventType = 2
	//  FailRun      EventType = 3
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
	SignalType int
	Index      int
}

func (s SignalSequence) Encode() string {
	return fmt.Sprintf("%d:%d", s.SignalType, s.Index)
}

func DecodeSignalSequence(sequence string) (SignalSequence, error) {
	split := strings.Split(sequence, ":")
	if len(split) < 2 {
		return SignalSequence{}, errors.New("invalid sequence")
	}
	typ, err := strconv.Atoi(split[0])
	if err != nil {
		return SignalSequence{}, errors.New("invalid sequence")
	}
	index, err := strconv.Atoi(split[1])
	if err != nil {
		return SignalSequence{}, errors.New("invalid sequence")
	}
	return SignalSequence{
		SignalType: typ,
		Index:      index,
	}, nil
}

type Key struct {
	Namespace string
	Workflow  string
	Run       string

	Activity string
	Sequence string
}

func (k Key) Encode() string {
	p := path.Join(k.Namespace, k.Workflow, k.Run)

	v := make(url.Values)
	if k.Activity != "" {
		v.Set(paramActivity, k.Activity)
	}
	if k.Sequence != "" {
		v.Set(paramSequence, k.Sequence)
	}

	if len(v) == 0 {
		return p
	}

	return p + "?" + v.Encode()
}

func DecodeKey(key string) (Key, error) {
	split1 := strings.Split(key, "?")
	if len(split1) > 2 || len(split1) == 0 || key == "" {
		return Key{}, errors.New("invalid key")
	}

	split2 := strings.Split(split1[0], "/")
	if len(split2) != 3 {
		return Key{}, errors.New("invalid key")
	}

	var k Key
	for i, s := range split2 {
		if i == 0 {
			k.Namespace = s
		} else if i == 1 {
			k.Workflow = s
		} else if i == 2 {
			k.Run = s
		}
	}

	if len(split1) > 1 {
		v, err := url.ParseQuery(split1[1])
		if err != nil {
			return Key{}, errors.New("invalid key params")
		}

		k.Activity = v.Get(paramActivity)
		k.Sequence = v.Get(paramSequence)
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

func ShortKey(namespace, workflow, run string) string {
	return Key{
		Namespace: namespace,
		Workflow:  workflow,
		Run:       run,
	}.Encode()
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
	RequestActivity(ctx context.Context, key string, message proto.Message) error

	// RespondActivity inserts a ActivityResponse event.
	RespondActivity(ctx context.Context, key string, message proto.Message) error

	// RespondActivityRaw inserts a ActivityResponse event without wrapping the message in an any.
	RespondActivityRaw(ctx context.Context, key string, message *any.Any) error

	// CompleteRun inserts a RunComplete event.
	CompleteRun(ctx context.Context, namespace, workflow, run string) error

	// ListBootstrapEvents returns the boostrap events for the run.
	ListBootstrapEvents(ctx context.Context, namespace, workflow, run string) ([]reflex.Event, error)
}
