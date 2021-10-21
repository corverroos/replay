package db

import (
	"context"
	"database/sql"
	"sort"
	"strconv"
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

// ToStream returns a reflex stream filtering by namespace, workflow and run if not empty.
func ToStream(dbc *sql.DB, events *rsql.EventsTable, namespace, workflow, run string) reflex.StreamFunc {
	return func(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error) {

		filterOpts := j.MKV{"namespace": namespace, "workflow": workflow, "run": run}
		if (run != "" && workflow == "") || (workflow != "" && namespace == "") {
			return nil, errors.New("missing filter", filterOpts)
		}

		// When starting a stream with a filter...
		if after == "" && namespace != "" {
			// ... bootstrap to first event matching the filter.
			where := "type=? and namespace=?"
			args := []interface{}{internal.RunCreated, namespace}
			if workflow != "" {
				where += " and workflow=?"
				args = append(args, workflow)
			}
			if run != "" {
				where += " and run=?"
				args = append(args, run)
			}

			var id int64
			err := dbc.QueryRowContext(ctx, "select id from replay_events where "+
				where+" order by id asc limit 1", args...).Scan(&id)
			if errors.Is(err, sql.ErrNoRows) {
				// NoReturnErr: Just start from the beginning.
				// TODO(corver): Maybe start from head-1000 or something instead of zero.
			} else if err != nil {
				return nil, err
			} else {
				after = strconv.FormatInt(id-1, 10)
			}
		}

		cl, err := events.ToStream(dbc)(ctx, after, opts...)
		if err != nil {
			return nil, err
		}

		return &filter{
			namespace: namespace,
			workflow:  workflow,
			run:       run,
			cl:        cl,
		}, nil
	}
}

type filter struct {
	namespace string
	workflow  string
	run       string
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

		if f.namespace != "" && key.Namespace != f.namespace {
			continue
		}
		if f.workflow != "" && key.Workflow != f.workflow {
			continue
		}
		if f.run != "" && key.Run != f.run {
			continue
		}

		return e, nil
	}
}

func RestartRun(ctx context.Context, dbc *sql.DB, events *rsql.EventsTable, key internal.Key, message []byte) error {
	tx, err := dbc.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Mark previous iteration as complete
	notify1, err := insertTX(ctx, events, tx, key, internal.RunCompleted, nil)
	if errors.Is(err, internal.ErrDuplicate) {
		// NoReturnErr: Continue below
	} else if err != nil {
		return err
	} else {
		defer notify1()
	}

	// Start next iteration.
	key.Iteration++
	notify2, err := insertTX(ctx, events, tx, key, internal.RunCreated, message)
	if errors.Is(err, internal.ErrDuplicate) {
		// NoReturnErr: Continue below
	} else if err != nil {
		return err
	} else {
		defer notify2()
	}

	return tx.Commit()
}

func ListBootstrapEvents(ctx context.Context, dbc *sql.DB, key internal.Key) ([]reflex.Event, error) {
	rows, err := dbc.QueryContext(ctx, "select id, `key`, type, timestamp, message "+
		"from replay_events where namespace=? and workflow=? and run=? and iteration = ? and (type=? or type=? or type=?) order by id asc",
		key.Namespace, key.Workflow, key.Run, key.Iteration, internal.RunCreated, internal.ActivityResponse, internal.RunCompleted)
	if err != nil {
		return nil, err
	}

	el, err := scanEvents(rows)
	if err != nil {
		return nil, err
	}

	if len(el) == 0 {
		return nil, nil
	}

	rows, err = dbc.QueryContext(ctx, "select id, `key`, type, timestamp, message "+
		"from replay_events where namespace=? and workflow=? and run=? and iteration=-1 and type=? and id>? order by id asc",
		key.Namespace, key.Workflow, key.Run, internal.RunSignal, el[0].ID)
	if err != nil {
		return nil, err
	}

	signals, err := scanEvents(rows)
	if err != nil {
		return nil, err
	}

	el = append(el, signals...)
	sort.Slice(el, func(i, j int) bool {
		return el[i].IDInt() < el[j].IDInt()
	})

	return el, nil
}

func scanEvents(rows *sql.Rows) ([]reflex.Event, error) {
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

func Insert(ctx context.Context, dbc *sql.DB, events *rsql.EventsTable, key internal.Key, typ internal.EventType, message []byte) error {
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

func insertTX(ctx context.Context, events *rsql.EventsTable, tx *sql.Tx, key internal.Key, typ internal.EventType, message []byte) (rsql.NotifyFunc, error) {
	k := key.Encode()

	// Do lookup to avoid creating tons of gaps when replaying long running runs.
	var exists int
	err := tx.QueryRowContext(ctx, "select exists("+
		"select 1 from replay_events where `key` = ? and type = ?)", k, typ).
		Scan(&exists)
	if err != nil {
		return nil, err
	} else if exists == 1 {
		return func() {}, errors.Wrap(internal.ErrDuplicate, "duplicate for key", j.KS("key", k))
	}

	notify, err := events.InsertWithMetadata(ctx, tx, k, typ, message)
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
	} else if k.Target == "" && reflex.IsAnyType(typ, internal.ActivityRequest, internal.ActivityResponse, internal.RunOutput) {
		return errors.New("target empty", j.KS("key", key))
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
