package sleep

import (
	"context"
	"database/sql"
	"path"
	"time"

	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/log"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/internal"
	"github.com/corverroos/replay/internal/db"
	"github.com/corverroos/replay/internal/replaypb"
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

		if key.Activity != internal.ActivitySleep {
			return nil
		}

		message, err := internal.ParseMessage(e)
		if err != nil {
			return err
		}

		req := message.(*replaypb.SleepRequest)
		completeAt := time.Now().Add(time.Duration(req.Duration.Seconds) * time.Second)

		_, err = dbc.ExecContext(ctx, "insert into replay_sleeps set `key`=?, "+
			"created_at=?, complete_at=?, completed=false", e.ForeignID, time.Now(), completeAt)
		if _, ok := db.MaybeWrapErrDuplicate(err, "by_key"); ok {
			// Record already exists. Continue.
		} else if err != nil {
			return err
		}

		return nil
	}

	consumer := path.Join("replay_activity", "internal", internal.ActivitySleep)
	spec := reflex.NewSpec(cl.Stream("*"), cstore, reflex.NewConsumer(consumer, fn))
	go rpatterns.RunForever(getCtx, spec)
	go completeSleepsForever(getCtx, cl.Internal(), dbc)
}

func completeSleepsForever(getCtx func() context.Context, cl internal.Client, dbc *sql.DB) {
	for {
		ctx := getCtx()

		err := completeSleepsOnce(ctx, cl, dbc)
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "complete sleeps once"))
		}
		time.Sleep(pollPeriod)
	}
}

func completeSleepsOnce(ctx context.Context, cl internal.Client, dbc *sql.DB) error {
	sl, err := listToComplete(ctx, dbc)
	if err != nil {
		return errors.Wrap(err, "list to complete")
	}

	for _, s := range sl {
		if !shouldComplete(s.CompleteAt) {
			return nil
		}

		err := cl.RespondActivity(ctx, s.Key, &replaypb.SleepDone{})
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

type sleep struct {
	ID         int
	Key        string
	CreatedAt  time.Time
	CompleteAt time.Time
}

func listToComplete(ctx context.Context, dbc *sql.DB) ([]sleep, error) {
	rows, err := dbc.QueryContext(ctx, "select id, `key`, created_at, complete_at "+
		"from replay_sleeps where completed=false order by complete_at asc")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []sleep
	for rows.Next() {
		var s sleep

		err := rows.Scan(&s.ID, &s.Key, &s.CreatedAt, &s.CompleteAt)
		if err != nil {
			return nil, err
		}

		res = append(res, s)
	}

	return res, rows.Err()
}
