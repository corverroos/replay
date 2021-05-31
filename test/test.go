package test

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/luno/reflex"
	"github.com/luno/reflex/rsql"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/internal/db"
	"github.com/corverroos/replay/internal/signal"
	"github.com/corverroos/replay/internal/sleep"
	"github.com/corverroos/replay/server"
)

func Setup(t *testing.T, opts ...rsql.EventsOption) (*server.DBClient, *sql.DB) {
	dbc := db.ConnectForTesting(t)
	cl := server.NewDBClient(dbc, opts...)

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

func RegisterNoSleepSignals(getCtx func() context.Context, cl *server.DBClient, cstore reflex.CursorStore, dbc *sql.DB) {
	signal.RegisterForTesting(getCtx, cl, cstore, dbc)
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
