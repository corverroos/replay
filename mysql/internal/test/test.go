package test

import (
	"context"
	"database/sql"
	"flag"
	"sync"
	"testing"
	"time"

	"github.com/corverroos/replay/mysql"
	"github.com/corverroos/replay/mysql/internal/db"
	"github.com/corverroos/replay/mysql/internal/sleep"

	"github.com/luno/reflex"
	"github.com/luno/reflex/rsql"

	"github.com/corverroos/replay"
)

func Setup(t *testing.T, opts ...rsql.EventsOption) (*mysql.DBClient, *sql.DB) {
	flag.Set("replay_debug", "true")

	dbc := db.ConnectForTesting(t)
	cl := mysql.NewDBClient(dbc, opts...)

	// Start gap fillers only, not signal/sleep activities.
	cl.StartLoops(func() context.Context {
		time.Sleep(time.Hour)
		panic("do not start sleep/signal loops")
	}, nil, "")

	return cl, dbc
}

func RegisterNoopSleeps(getCtx func() context.Context, cl replay.Client, cstore reflex.CursorStore, dbc *sql.DB) {
	sleep.RegisterForTesting(getCtx, cl, cstore, dbc)
}

type MemCursorStore struct {
	sync.Mutex
	cursors map[string]string
}

func (m *MemCursorStore) GetCursor(_ context.Context, consumerName string) (string, error) {
	m.Lock()
	defer m.Unlock()
	return m.cursors[consumerName], nil
}

func (m *MemCursorStore) SetCursor(_ context.Context, consumerName string, cursor string) error {
	m.Lock()
	defer m.Unlock()
	if m.cursors == nil {
		m.cursors = make(map[string]string)
	}
	m.cursors[consumerName] = cursor
	return nil
}

func (m *MemCursorStore) Flush(_ context.Context) error { return nil }
