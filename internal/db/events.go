package db

import (
	"context"
	"database/sql"
	"testing"

	"github.com/corverroos/replay/internal"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rsql"
)

var events = rsql.NewEventsTable("replay_events",
	rsql.WithEventTimeField("timestamp"),
	rsql.WithEventsInMemNotifier(),
	rsql.WithEventMetadataField("metadata"),
	rsql.WithEventForeignIDField("`key`"),
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

func ListBootstrapEvents(ctx context.Context, dbc *sql.DB, workflow, run string) ([]reflex.Event, error) {
	rows, err := dbc.QueryContext(ctx, "select id, `key`, type, timestamp, metadata "+
		"from replay_events where workflow=? and run=? and (type=? or type=?) order by id asc", workflow, run, internal.CreateRun, internal.ActivityResponse)
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

		e.Type = internal.EventType(typ)
		res = append(res, e)
	}

	return res, rows.Err()
}

func Insert(ctx context.Context, dbc *sql.DB, key string, typ internal.EventType, message []byte) error {
	tx, err := dbc.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Do lookup to avoid creating tons of gaps when replaying long running runs.
	var exists int
	err = tx.QueryRowContext(ctx, "select exists("+
		"select 1 from replay_events where `key` = ? and type = ?)", key, typ).
		Scan(&exists)
	if err != nil {
		return err
	} else if exists == 1 {
		return nil
	}

	notify, err := events.InsertWithMetadata(ctx, tx, key, typ, message)
	if err, ok := MaybeWrapErrDuplicate(err, "by_type_key"); ok {
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

	_, err = tx.ExecContext(ctx, "insert into replay_events set `key`=?, workflow=?, run=?, "+
		"timestamp=now(3), type=?, metadata=?", key, k.Workflow, run, typ, metadata)
	return err
}
