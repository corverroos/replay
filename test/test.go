package test

import (
	"context"
	"database/sql"
	"sync"
	"testing"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/internal/db"
	"github.com/corverroos/replay/internal/signal"
	"github.com/corverroos/replay/internal/sleep"
	"github.com/luno/reflex"
)

func ConnectDB(t *testing.T) *sql.DB {
	return db.ConnectForTesting(t)
}

func RegisterNoopSleeps(ctx context.Context, cl replay.Client, cstore reflex.CursorStore, dbc *sql.DB) {
	sleep.RegisterForTesting(ctx, cl, cstore, dbc)
}

func RegisterNoSleepSignals(ctx context.Context, cl replay.Client, cstore reflex.CursorStore, dbc *sql.DB) {
	signal.RegisterForTesting(ctx, cl, cstore, dbc)
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
