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
	"github.com/luno/reflex"
	"github.com/luno/reflex/reflexpb"
)

const SleepActivity = "replay_sleep"
const SignalActivity = "replay_signal"

type EventType int

func (e EventType) ReflexType() int {
	return int(e)
}

const (
	CreateRun        EventType = 1
	CompleteRun      EventType = 2
	FailRun          EventType = 3
	ActivityRequest  EventType = 4
	ActivityResponse EventType = 5
)

func ParseEvent(e *reflex.Event) (Key, proto.Message, error) {
	k, err := DecodeKey(e.ForeignID)
	if err != nil {
		return Key{}, nil, err
	}

	if len(e.MetaData) == 0 {
		return k, nil, nil
	}

	var a any.Any
	if err := proto.Unmarshal(e.MetaData, &a); err != nil {
		return Key{}, nil, errors.Wrap(err, "unmarshal proto")
	}

	var d ptypes.DynamicAny
	if err := ptypes.UnmarshalAny(&a, &d); err != nil {
		return Key{}, nil, errors.Wrap(err, "unmarshal anypb")
	}

	return k, d.Message, nil
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
	Workflow string
	Run      string
	Activity string
	Sequence string
}

func (k Key) Encode() string {
	return path.Join(k.Workflow, k.Run, k.Activity, k.Sequence)
}

func DecodeKey(key string) (Key, error) {
	split := strings.Split(key, "/")
	if len(split) < 2 {
		return Key{}, errors.New("invalid key")
	}
	var k Key
	for i, s := range split {
		if i == 0 {
			k.Workflow = s
		} else if i == 1 {
			k.Run = s
		} else if i == 2 {
			k.Activity = s
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

func ShortKey(workflow, run string) string {
	return Key{Workflow: workflow, Run: run}.Encode()
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

type Client interface {
	RequestActivity(ctx context.Context, key string, message proto.Message) error
	CompleteActivity(ctx context.Context, key string, message proto.Message) error
	CompleteActivityRaw(ctx context.Context, key string, message *any.Any) error
	CompleteRun(ctx context.Context, workflow, run string) error
	ListBootstrapEvents(ctx context.Context, workflow, run string) ([]reflex.Event, error)
	Stream(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error)
}
