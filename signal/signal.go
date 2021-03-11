package signal

import (
	"context"
	"database/sql"
	"time"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/db"
	"github.com/corverroos/replay/internal"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/log"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"
	"google.golang.org/protobuf/types/known/anypb"
)

//go:generate protoc --go_out=plugins=grpc:. ./sleep.proto

type signal struct {
	ID      int64
	Message []byte
}

type check struct {
	ID     int64
	Key    string
	FailAt time.Time
	Status CheckStatus
}

type CheckStatus int

const (
	CheckPending   CheckStatus = 1
	CheckFailed    CheckStatus = 2
	CheckCompleted CheckStatus = 3
)

func RegisterForTesting(ctx context.Context, cl replay.Client, cstore reflex.CursorStore, dbc *sql.DB) {
	pollPeriod = time.Millisecond * 100
	shouldComplete = func(completeAt time.Time) bool {
		return true
	}
	Register(ctx, cl, cstore, dbc)
}

func Register(ctx context.Context, cl replay.Client, cstore reflex.CursorStore, dbc *sql.DB) {
	fn := func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
		if !reflex.IsType(e.Type, db.ActivityRequest) {
			return nil
		}

		key, message, err := internal.ParseEvent(e)
		if err != nil {
			return err
		}

		if key.Activity != internal.SignalActivity {
			return nil
		}

		req := message.(*internal.SleepRequest)
		completeAt := time.Now().Add(time.Duration(req.Duration.Seconds) * time.Second)

		_, err = dbc.ExecContext(ctx, "insert into signal_checks set `key`=?, "+
			"created_at=?, fail_at=?, status=?", e.ForeignID, time.Now(), completeAt, CheckPending)
		if _, ok := db.MaybeWrapErrDuplicate(err, "by_key"); ok {
			return nil
		} else if err != nil {
			return err
		}

		return nil
	}

	spec := reflex.NewSpec(cl.Stream, cstore, reflex.NewConsumer(internal.SleepActivity, fn))
	go rpatterns.RunForever(func() context.Context { return ctx }, spec)
	go completeChecksForever(ctx, cl, dbc)
}

func Insert(ctx context.Context, dbc *sql.DB, workflow, run string, s replay.Signal, message proto.Message, externalID string) error {
	var b []byte
	if message != nil {
		any, err := ptypes.MarshalAny(message)
		if err != nil {
			return err
		}

		b, err = proto.Marshal(any)
		if err != nil {
			return err
		}
	}

	_, err := dbc.ExecContext(ctx, "insert into signals set workflow=?, run=?, type=?, external_id=?, created_at=?, message=? ",
		workflow, run, s.SignalType(), externalID, time.Now(), b)
	if err, ok := db.MaybeWrapErrDuplicate(err, "uniq"); ok {
		return err
	} else if err != nil {
		return err
	}

	return nil
}

func completeChecksForever(ctx context.Context, cl replay.Client, dbc *sql.DB) {
	for {
		err := completeChecksOnce(ctx, cl, dbc)
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "complete sleeps once"))
		}
		time.Sleep(pollPeriod)
	}
}

func completeChecksOnce(ctx context.Context, cl replay.Client, dbc *sql.DB) error {
	// TODO(corver): Do two queries, one to fail, one to complete.
	checks, err := listPending(ctx, dbc)
	if err != nil {
		return errors.Wrap(err, "list to complete")
	}

	var other []check
	for _, c := range checks {
		key, err := internal.DecodeKey(c.Key)
		if err != nil {
			return err
		}

		sig, err := lookupSignal(ctx, dbc, key)
		if errors.Is(err, sql.ErrNoRows) {
			other = append(other, c)
			continue
		} else if err != nil {
			return err
		}

		var msg proto.Message
		if len(sig.Message) > 0 {
			var a anypb.Any
			if err := proto.Unmarshal(sig.Message, &a); err != nil {
				return errors.Wrap(err, "unmarshal proto")
			}

			var d ptypes.DynamicAny
			if err := ptypes.UnmarshalAny(&a, &d); err != nil {
				return errors.Wrap(err, "unmarshal anypb")
			}

			msg = d.Message
		}

		err = cl.CompleteActivity(ctx, c.Key, msg)
		if err != nil {
			return err
		}

		err = completeSignal(ctx, dbc, sig.ID, c.ID)
		if err != nil {
			return err
		}
	}

	for _, c := range other {
		if !shouldComplete(c.FailAt) {
			return nil
		}

		err := cl.CompleteActivity(ctx, c.Key, &internal.SleepDone{})
		if err != nil {
			return err
		}

		err = failCheck(ctx, dbc, c.ID)
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

func lookupSignal(ctx context.Context, dbc *sql.DB, key internal.Key) (signal, error) {
	seq, err := internal.DecodeSignalSequence(key.Sequence)
	if err != nil {
		return signal{}, err
	}

	var s signal
	err = dbc.QueryRowContext(ctx, "select id, message "+
		"from signals where workflow=? and run=? and type=? and check_id is null",
		key.Workflow, key.Run, seq.SignalType).Scan(&s.ID, &s.Message)
	return s, err
}

func failCheck(ctx context.Context, dbc *sql.DB, checkID int64) error {
	res, err := dbc.ExecContext(ctx, "update signal_checks set status=? where id=? and status=?", CheckFailed, checkID, CheckPending)
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

	res, err := tx.ExecContext(ctx, "update signal_checks set status=? where id=? and status=?", CheckCompleted, checkID, CheckPending)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	} else if n != 1 {
		return errors.New("unexpected number of rows updated")
	}

	res, err = tx.ExecContext(ctx, "update signals set check_id=? where id=? and check_id is null", checkID, signalID)
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

func listPending(ctx context.Context, dbc *sql.DB) ([]check, error) {
	rows, err := dbc.QueryContext(ctx, "select id, `key`, status, fail_at "+
		"from signal_checks where status=? order by fail_at asc", CheckPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []check
	for rows.Next() {
		var c check

		err := rows.Scan(&c.ID, &c.Key, &c.Status, &c.FailAt)
		if err != nil {
			return nil, err
		}

		res = append(res, c)
	}

	return res, rows.Err()
}
