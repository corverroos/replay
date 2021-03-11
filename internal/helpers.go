package internal

import (
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"google.golang.org/protobuf/types/known/anypb"
	"path"
	"strings"
)

const SleepActivity = "replay_sleep"

func ParseEvent(e *reflex.Event) (Key, proto.Message, error) {
	k, err := DecodeKey(e.ForeignID)
	if err != nil {
		return Key{}, nil, err
	}

	if len(e.MetaData) == 0 {
		return k, nil, nil
	}

	var a anypb.Any
	if err := proto.Unmarshal(e.MetaData, &a); err != nil {
		return Key{}, nil, errors.Wrap(err, "unmarshal proto")
	}

	var d ptypes.DynamicAny
	if err := ptypes.UnmarshalAny(&a, &d); err != nil {
		return Key{}, nil, errors.Wrap(err, "unmarshal anypb")
	}

	return k, d.Message, nil
}

type Key struct {
	Workflow string
	Run string
	Activity string
	Sequence string
}

func (k Key) Encode() string {
	return path.Join(k.Workflow, k.Run, k.Activity, k.Sequence)
}

func DecodeKey(key string) (Key, error) {
	split := strings.Split(key, "/")
	if len(split) < 1 {
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
