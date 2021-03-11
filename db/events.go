package db

import (
	"context"
	"database/sql"
	"github.com/corverroos/replay/internal"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/luno/jettison/errors"
	"testing"
	"time"

	"github.com/luno/reflex"
	"github.com/luno/reflex/rsql"
)

var events = rsql.NewEventsTable("events",
	rsql.WithEventTimeField("timestamp"),
	rsql.WithEventsInMemNotifier(),
	rsql.WithEventMetadataField("metadata"),
	rsql.WithEventForeignIDField("`key`"),
	rsql.WithEventsInserter(inserter),
	rsql.WithEventsBackoff(time.Millisecond*100),
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
	CreateRun        EventType = 1
	CompleteRun      EventType = 2
	FailRun          EventType = 3
	ActivityRequest  EventType = 4
	ActivityResponse EventType = 5
)

func ListBootstrapEvents(ctx context.Context, dbc *sql.DB, workflow, run string) ([]reflex.Event, error) {
	rows, err := dbc.QueryContext(ctx, "select id, `key`, type, timestamp, metadata "+
		"from events where workflow=? and run=? and (type=? or type=?) order by id asc", workflow, run, CreateRun, ActivityResponse)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []reflex.Event
	for rows.Next() {
		var (
			e   reflex.Event
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

func Insert(ctx context.Context, dbc *sql.DB, key string, typ EventType, message proto.Message) error {
	var meta []byte
	if message != nil {
		any, err := ptypes.MarshalAny(message)
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
	err = tx.QueryRowContext(ctx, "select exists("+
		"select 1 from events where `key` = ? and type = ?)", key, typ).
		Scan(&exists)
	if err != nil {
		return err
	} else if exists == 1 {
		return nil
	}

	notify, err := events.InsertWithMetadata(ctx, tx, key, typ, meta)
	if _, ok := MaybeWrapErrDuplicate(err, "by_type_key"); ok {
		return err
	} else if err != nil {
		return errors.Wrap(err, "insert")
	}
	defer notify()

	return tx.Commit()
}

func inserter(ctx context.Context, tx *sql.Tx,
	key string, typ reflex.EventType, metadata []byte) error {

	k, err := internal.DecodeKey(key)
	if err != nil {
		return err
	}

	var run sql.NullString
	if k.Run != "" {
		run.String = k.Run
		run.Valid = true
	}

	_, err = tx.ExecContext(ctx, "insert into events set `key`=?, workflow=?, run=?, "+
		"timestamp=now(3), type=?, metadata=?", key, k.Workflow, run, typ, metadata)
	return err
}
