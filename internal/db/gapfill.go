package db

import (
	"context"
	"database/sql"
	"strconv"
	"time"

	"github.com/corverroos/replay/internal"
	"github.com/go-sql-driver/mysql"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	"github.com/luno/reflex/rsql"
)

func FillGaps(dbc *sql.DB) {
	events.ListenGaps(makeFill(dbc))
}

// makeFill returns a fill function that ensures that rows exist
// with the ids indicated by the Gap. It does so by either detecting
// existing rows or by inserting noop events. It is idempotent.
func makeFill(dbc *sql.DB) func(rsql.Gap) {
	return func(gap rsql.Gap) {
		ctx := context.Background()
		for i := gap.Prev + 1; i < gap.Next; i++ {
			err := fillGap(ctx, dbc, i)
			if err != nil {
				log.Error(ctx, errors.Wrap(err, "errors filling gap",
					j.MKV{"table": "replay_events", "id": i}))
				return
			}
		}
	}
}

// fillGap blocks until an event with id exists (committed) in the table or if it
// could insert a noop event with that id.
func fillGap(ctx context.Context, dbc *sql.DB, id int64) error {
	// Wait until the event is committed.
	committed, err := waitCommitted(ctx, dbc, id)
	if err != nil {
		return err
	}
	if committed {
		return nil // Gap already filled
	}

	// It does not exists at all, so insert noop.
	_, err = dbc.ExecContext(ctx, "insert into replay_events set id=?, `key`=?, "+
		"timestamp=now(3), type=0", id, internal.Key{Workflow: "gap", Sequence: strconv.FormatInt(id, 10)}.Encode())

	if isMySQLErr(err, 1062) {
		// MySQL duplicate entry error. Someone got there first, but that's ok.
		return nil
	} else if err != nil {
		return err
	}

	eventsGapFilledCounter.Inc()

	return nil
}

func exists(ctx context.Context, dbc *sql.DB, id int64,
	level sql.IsolationLevel) (bool, error) {

	tx, err := dbc.BeginTx(ctx, &sql.TxOptions{Isolation: level})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	var exists int
	err = tx.QueryRow("select exists(select 1 from replay_events where id=?)", id).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists == 1, tx.Commit()
}

// waitCommitted blocks while an uncommitted event with id exists and returns true once
// it is committed or false if it is rolled back or there is no uncommitted event at all.
func waitCommitted(ctx context.Context, dbc *sql.DB, id int64) (bool, error) {
	for {
		uncommitted, err := exists(ctx, dbc, id, sql.LevelReadUncommitted)
		if err != nil {
			return false, err
		}

		if !uncommitted {
			return false, nil
		}

		committed, err := exists(ctx, dbc, id, sql.LevelDefault)
		if err != nil {
			return false, err
		}

		if committed {
			return true, nil
		}

		time.Sleep(time.Millisecond * 100) // Don't spin
	}
}

func isMySQLErr(err error, nums ...uint16) bool {
	if err == nil {
		return false
	}

	me := new(mysql.MySQLError)
	if !errors.As(err, &me) {
		return false
	}

	for _, num := range nums {
		if me.Number == num {
			return true
		}
	}
	return false
}
