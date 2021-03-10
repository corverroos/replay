package internal

import (
	"encoding/base64"
	"encoding/json"
	"github.com/corverroos/replay/db"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"google.golang.org/protobuf/types/known/anypb"
)

const SleepActivity = "replay_sleep"

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

func MarshalAsyncToken(foreignID string) string {
	return base64.URLEncoding.EncodeToString([]byte(foreignID))
}

func UnmarshalAsyncToken(token string) (foreignID string, err error) {
	b, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		return "", err
	}
	return string(b), err
}

