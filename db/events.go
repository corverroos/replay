package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"testing"

	"github.com/luno/reflex"
	"github.com/luno/reflex/rsql"
)

var events = rsql.NewEventsTable("events",
	rsql.WithEventTimeField("timestamp"),
	rsql.WithEventForeignIDField("workflow"),
	rsql.WithEventsInMemNotifier(),
	rsql.WithEventMetadataField("metadata"),
)

// ToStream returns a reflex stream for the events.
func ToStream(dbc *sql.DB) reflex.StreamFunc {
	return events.ToStream(dbc)
}

// CleanCache clears the cache after testing to clear test artifacts.
func CleanCache(t *testing.T) {
	t.Cleanup(func() {
		events = events.Clone()
	})
}

// FillGaps registers the default gap filler for the action events table.
func FillGaps(dbc *sql.DB) {
	rsql.FillGaps(dbc, events)
}

type EventType int

func (e EventType) ReflexType() int {
	return int(e)
}

const (
	CreateRun EventType = 1
	CompleteRun EventType = 2
	FailRun EventType = 3
	ActivityRequest EventType = 4
	ActivityResponse EventType = 5
)

func Insert(ctx context.Context, dbc *sql.DB, workflow, run string, activity string, typ EventType, args proto.Message) error {
	eid, err := json.Marshal(EventID{
		Workflow: workflow,
		Run:      run,
		Activity: activity,
	})
	if err != nil {
		return err
	}

	var meta []byte
	if args != nil {
		any, err := ptypes.MarshalAny(args)
		if err != nil {
			return err
		}

		meta, err = proto.Marshal(any)
		if err != nil {
			return err
		}
	}

	tx, err := dbc.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	notify, err := events.InsertWithMetadata(ctx, tx, string(eid), typ, meta)
	if err != nil {
		return err
	}
	defer notify()

	return tx.Commit()
}

type EventID struct {
	Workflow string
	Run string
	Activity string `json:"activity,omitempty"`
}
