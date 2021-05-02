package db

import (
	"context"
	"database/sql"
	"strings"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rsql"

	"github.com/corverroos/replay/internal"
)

const noopPrefix = "replay_noop"

func DefaultEvents() *rsql.EventsTable {
	return rsql.NewEventsTable("replay_events",
		rsql.WithEventTimeField("timestamp"),
		rsql.WithEventsInMemNotifier(),
		rsql.WithEventMetadataField("message"),
		rsql.WithEventForeignIDField("`key`"),
		rsql.WithEventsInserter(inserter),
	)
}

// ToStream returns a reflex stream filtering only events for the namespace unless a wildcard is provided.
func ToStream(dbc *sql.DB, events *rsql.EventsTable, namespace string) reflex.StreamFunc {
	return func(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error) {
		cl, err := events.ToStream(dbc)(ctx, after, opts...)
		if err != nil {
			return nil, err
		}

		return &filter{
			namespace: namespace,
			cl:        cl,
		}, nil
	}
}

type filter struct {
	namespace string
	cl        reflex.StreamClient
}

func (f *filter) Recv() (*reflex.Event, error) {
	for {
		e, err := f.cl.Recv()
		if err != nil {
			return nil, err
		}

		if strings.HasPrefix(e.ForeignID, noopPrefix) && e.Type.ReflexType() == 0 {
			// Always filter out noop gaps.
			continue
		}

		key, err := internal.DecodeKey(e.ForeignID)
		if err != nil {
			return nil, err
		}

		if f.namespace != "*" && key.Namespace != f.namespace {
			continue
		}

		return e, nil
	}
}

func RestartRun(ctx context.Context, dbc *sql.DB, events *rsql.EventsTable, key string, message []byte) error {
	k, err := internal.DecodeKey(key)
	if err != nil {
		return err
	}

	tx, err := dbc.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Mark previous iteration as complete
	notify1, err := insertTX(ctx, events, tx, key, internal.CompleteRun, nil)
	if errors.Is(err, internal.ErrDuplicate) {
		// NoReturnErr: Continue below
	} else if err != nil {
		return err
	} else {
		defer notify1()
	}

	// Start next iteration.
	k.Iteration++
	notify2, err := insertTX(ctx, events, tx, k.Encode(), internal.CreateRun, message)
	if errors.Is(err, internal.ErrDuplicate) {
		// NoReturnErr: Continue below
	} else if err != nil {
		return err
	} else {
		defer notify2()
	}

	return tx.Commit()
}

func ListBootstrapEvents(ctx context.Context, dbc *sql.DB, events *rsql.EventsTable, key string) ([]reflex.Event, error) {
	k, err := internal.DecodeKey(key)
	if err != nil {
		return nil, err
	}

	rows, err := dbc.QueryContext(ctx, "select id, `key`, type, timestamp, message "+
		"from replay_events where namespace=? and workflow=? and run=? and iteration = ? and (type=? or type=? or type=?) order by id asc",
		k.Namespace, k.Workflow, k.Run, k.Iteration, internal.CreateRun, internal.ActivityResponse, internal.CompleteRun)
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

func Insert(ctx context.Context, dbc *sql.DB, events *rsql.EventsTable, key string, typ internal.EventType, message []byte) error {
	tx, err := dbc.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	notify, err := insertTX(ctx, events, tx, key, typ, message)
	if err != nil {
		return err
	}
	defer notify()

	return tx.Commit()
}

func insertTX(ctx context.Context, events *rsql.EventsTable, tx *sql.Tx, key string, typ internal.EventType, message []byte) (rsql.NotifyFunc, error) {
	// Do lookup to avoid creating tons of gaps when replaying long running runs.
	var exists int
	err := tx.QueryRowContext(ctx, "select exists("+
		"select 1 from replay_events where `key` = ? and type = ?)", key, typ).
		Scan(&exists)
	if err != nil {
		return nil, err
	} else if exists == 1 {
		return func() {}, errors.Wrap(internal.ErrDuplicate, "duplicate for key", j.KS("key", key))
	}

	notify, err := events.InsertWithMetadata(ctx, tx, key, typ, message)
	if err, ok := MaybeWrapErrDuplicate(err, "by_type_key"); ok {
		return nil, err
	} else if err != nil {
		return nil, err
	}

	return notify, nil
}

func inserter(ctx context.Context, tx *sql.Tx,
	key string, typ reflex.EventType, message []byte) error {

	k, err := internal.DecodeKey(key)
	if err != nil {
		return err
	}

	if k.Namespace == "" {
		return errors.New("namespace empty", j.KS("key", key))
	} else if k.Workflow == "" {
		return errors.New("workflow empty", j.KS("key", key))
	} else if k.Run == "" && reflex.IsAnyType(typ, internal.ActivityRequest, internal.ActivityResponse) {
		return errors.New("run empty", j.KS("key", key))
	} else if k.Activity == "" && reflex.IsAnyType(typ, internal.ActivityRequest, internal.ActivityResponse) {
		return errors.New("activity empty", j.KS("key", key))
	} else if k.Sequence == "" && reflex.IsAnyType(typ, internal.ActivityRequest, internal.ActivityResponse) {
		return errors.New("sequence empty", j.KS("key", key))
	}

	var run sql.NullString
	if k.Run != "" {
		run.String = k.Run
		run.Valid = true
	}

	_, err = tx.ExecContext(ctx, "insert into replay_events set `key`=?, namespace=?, workflow=?, run=?, "+
		"iteration=?, timestamp=now(3), type=?, message=?", key, k.Namespace, k.Workflow, run, k.Iteration, typ, message)
	return err
}
