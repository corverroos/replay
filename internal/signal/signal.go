package signal

import (
	"context"
	"database/sql"
	"encoding/binary"
	"hash/fnv"
	"time"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/internal"
	"github.com/corverroos/replay/internal/db"
	"github.com/corverroos/replay/internal/replaypb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/log"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"
)

//go:generate protoc --go_out=plugins=grpc:. ./sleep.proto

type await struct {
	ID        int64
	Key       string
	TimeoutAt time.Time
	Status    AwaitStatus
}

type AwaitStatus int

const (
	AwaitPending AwaitStatus = 1
	AwaitTimeout AwaitStatus = 2
	AwaitSuccess AwaitStatus = 3
)

func RegisterForTesting(ctx context.Context, cl replay.Client, cstore reflex.CursorStore, dbc *sql.DB) {
	pollPeriod = time.Millisecond * 100
	shouldComplete = func(completeAt time.Time) bool {
		return true
	}
	Register(func() context.Context { return ctx }, cl, cstore, dbc)
}

func Register(getCtx func() context.Context, cl replay.Client, cstore reflex.CursorStore, dbc *sql.DB) {
	fn := func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
		if !reflex.IsType(e.Type, internal.ActivityRequest) {
			return nil
		}

		key, err := internal.DecodeKey(e.ForeignID)
		if err != nil {
			return err
		}

		if key.Activity != internal.ActivitySignal {
			return nil
		}

		message, err := internal.ParseMessage(e)
		if err != nil {
			return err
		}

		req := message.(*replaypb.SleepRequest)
		completeAt := time.Now().Add(time.Duration(req.Duration.Seconds) * time.Second)

		_, err = dbc.ExecContext(ctx, "insert into replay_signal_awaits set `key`=?, "+
			"created_at=?, timeout_at=?, status=?", e.ForeignID, time.Now(), completeAt, AwaitPending)
		if _, ok := db.MaybeWrapErrDuplicate(err, "by_key"); ok {
			// Record already exists. Continue.
		} else if err != nil {
			return err
		}

		return nil
	}

	spec := reflex.NewSpec(cl.Stream("*"), cstore, reflex.NewConsumer(internal.ActivitySignal, fn))
	go rpatterns.RunForever(getCtx, spec)
	go completeAwaitsForever(getCtx, cl.Internal(), dbc)
}

func Insert(ctx context.Context, dbc *sql.DB, namespace, workflow, run string, signalType int, message *any.Any, externalID string) error {
	b, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	// Mysql doesn't support uniq indexes with this many columns, so create a hash column.
	h := fnv.New128a()
	_, _ = h.Write([]byte(namespace))
	_, _ = h.Write([]byte(workflow))
	_, _ = h.Write([]byte(run))
	_ = binary.Write(h, binary.BigEndian, signalType)
	_, _ = h.Write([]byte(externalID))
	hash := h.Sum(nil)

	_, err = dbc.ExecContext(ctx, "insert into replay_signals set namespace=?, workflow=?, run=?, type=?, external_id=?, created_at=?, message=?, hash=?",
		namespace, workflow, run, signalType, externalID, time.Now(), b, hash)
	if err, ok := db.MaybeWrapErrDuplicate(err, "uniq"); ok {
		return err
	} else if err != nil {
		return err
	}

	return nil
}

func completeAwaitsForever(getCtx func() context.Context, cl internal.Client, dbc *sql.DB) {
	for {
		ctx := getCtx()

		err := completeAwaitsOnce(ctx, cl, dbc)
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "complete sleeps once"))
		}
		time.Sleep(pollPeriod)
	}
}

func completeAwaitsOnce(ctx context.Context, cl internal.Client, dbc *sql.DB) error {
	// TODO(corver): Do two queries, one to fail, one to complete.
	awaits, err := listPending(ctx, dbc)
	if err != nil {
		return errors.Wrap(err, "list to complete")
	}

	var other []await
	for _, a := range awaits {
		key, err := internal.DecodeKey(a.Key)
		if err != nil {
			return err
		}

		id, message, err := lookupSignal(ctx, dbc, key)
		if errors.Is(err, sql.ErrNoRows) {
			other = append(other, a)
			continue
		} else if err != nil {
			return err
		}

		var any any.Any
		if len(message) > 0 {
			if err := proto.Unmarshal(message, &any); err != nil {
				return errors.Wrap(err, "unmarshal proto")
			}
		}

		err = cl.RespondActivityRaw(ctx, a.Key, &any)
		if err != nil {
			return err
		}

		err = completeSignal(ctx, dbc, id, a.ID)
		if err != nil {
			return err
		}
	}

	for _, c := range other {
		if !shouldComplete(c.TimeoutAt) {
			return nil
		}

		err := cl.RespondActivity(ctx, c.Key, &replaypb.SleepDone{})
		if err != nil {
			return err
		}

		err = timeoutAwait(ctx, dbc, c.ID)
		if err != nil {
			return err
		}
	}

	return nil
}

var pollPeriod = time.Second
var shouldComplete = func(completeAt time.Time) bool {
	return completeAt.Before(time.Now())
}

func lookupSignal(ctx context.Context, dbc *sql.DB, key internal.Key) (id int64, message []byte, err error) {
	seq, err := internal.DecodeSignalSequence(key.Sequence)
	if err != nil {
		return 0, nil, err
	}

	err = dbc.QueryRowContext(ctx, "select id, message "+
		"from replay_signals where namespace=? and workflow=? and run=? and type=? and check_id is null",
		key.Namespace, key.Workflow, key.Run, seq.SignalType).Scan(&id, &message)
	return id, message, err
}

func timeoutAwait(ctx context.Context, dbc *sql.DB, checkID int64) error {
	res, err := dbc.ExecContext(ctx, "update replay_signal_awaits set status=? where id=? and status=?", AwaitTimeout, checkID, AwaitPending)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	} else if n != 1 {
		return errors.New("unexpected number of rows updated")
	}
	return nil
}

func completeSignal(ctx context.Context, dbc *sql.DB, signalID, checkID int64) error {
	tx, err := dbc.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, "update replay_signal_awaits set status=? where id=? and status=?", AwaitSuccess, checkID, AwaitPending)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	} else if n != 1 {
		return errors.New("unexpected number of rows updated")
	}

	res, err = tx.ExecContext(ctx, "update replay_signals set check_id=? where id=? and check_id is null", checkID, signalID)
	if err != nil {
		return err
	}
	n, err = res.RowsAffected()
	if err != nil {
		return err
	} else if n != 1 {
		return errors.New("unexpected number of rows updated")
	}

	return tx.Commit()
}

func listPending(ctx context.Context, dbc *sql.DB) ([]await, error) {
	rows, err := dbc.QueryContext(ctx, "select id, `key`, status, timeout_at "+
		"from replay_signal_awaits where status=? order by timeout_at asc", AwaitPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []await
	for rows.Next() {
		var c await

		err := rows.Scan(&c.ID, &c.Key, &c.Status, &c.TimeoutAt)
		if err != nil {
			return nil, err
		}

		res = append(res, c)
	}

	return res, rows.Err()
}
