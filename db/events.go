package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/luno/jettison/errors"
	"testing"

	"github.com/luno/reflex"
	"github.com/luno/reflex/rsql"
)

var events = rsql.NewEventsTable("events",
	rsql.WithEventTimeField("timestamp"),
	rsql.WithEventsInMemNotifier(),
	rsql.WithEventMetadataField("metadata"),
	rsql.WithEventsInserter(inserter),
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

func ListBootstrapEvents(ctx context.Context, dbc *sql.DB, workflow, run string) ([]reflex.Event, error) {
	rows, err := dbc.QueryContext(ctx, "select id, foreign_id, type, timestamp, metadata " +
		"from events where workflow=? and run=? and (type=? or type=?)", workflow, run, CreateRun, ActivityResponse)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []reflex.Event
	for rows.Next() {
		var (
			e reflex.Event
			typ int
		)

		err := rows.Scan(&e.ID, &e.ForeignID, &typ, &e.Timestamp, &e.MetaData)
		if err != nil {
			return nil, err
		}

		e.Type = EventType(typ)
		res = append(res, e)
	}

	return res, rows.Err()
}

// row is a common interface for *sql.Rows and *sql.Row.
type row interface {
	Scan(dest ...interface{}) error
}

func Insert(ctx context.Context, dbc *sql.DB, workflow, run string, activity string, index int, typ EventType, args proto.Message) error {
	eid, err := json.Marshal(EventID{
		Workflow: workflow,
		Run:      run,
		Activity: activity,
		Index: index,
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

	// Do lookup to avoid creating tons of gaps when replaying long running runs.
	var exists int
	err = tx.QueryRowContext(ctx, "select exists(" +
		"select 1 from events where foreign_id = ? and type = ?)", string(eid), typ).
		Scan(&exists)
	if err != nil {
		return err
	} else if exists == 1 {
		return nil
	}

	notify, err := events.InsertWithMetadata(ctx, tx, string(eid), typ, meta)
	if _, ok := MaybeWrapErrDuplicate(err, "foreign_id_type"); ok {
		return nil
	} else if err != nil {
		return err
	}
	defer notify()

	return tx.Commit()
}

type EventID struct {
	Workflow string
	Run string
	Activity string `json:"activity,omitempty"`
	Index int
}

func inserter (ctx context.Context, tx *sql.Tx,
foreignID string, typ reflex.EventType, metadata []byte) error {
	var eid EventID
	if err := json.Unmarshal([]byte(foreignID), &eid); err != nil {
		return errors.Wrap(err, "insert foreign id")
	}

	var run sql.NullString
	if eid.Run != "" {
		run.String = eid.Run
		run.Valid = true
	}

	_, err := tx.ExecContext(ctx, "insert into events set foreign_id=?, workflow=?, run=?, " +
		"timestamp=now(3), type=?, metadata=?", foreignID, eid.Workflow, run, typ, metadata)
	return err
}
