package db

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
	"github.com/luno/reflex/rsql"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/corverroos/replay/internal"
)

func TestGapFill(t *testing.T) {
	dbc := ConnectForTesting(t)
	ctx := context.Background()

	events := DefaultEvents().Clone(rsql.WithEventsBackoff(time.Millisecond * 100))
	FillGaps(dbc, events)

	insert := func(i int) bool {
		tx, err := dbc.Begin()
		jtest.RequireNil(t, err)
		defer tx.Rollback()

		notify, err := events.InsertWithMetadata(ctx, tx, internal.MinKey("ns", "w", "r", i), internal.RunCreated, nil)
		if err, ok := MaybeWrapErrDuplicate(err, "replay_events.by_type_key"); ok {
			return false
		} else {
			jtest.RequireNil(t, err)
		}
		jtest.RequireNil(t, tx.Commit())
		notify()
		return true
	}

	require.True(t, insert(0))
	require.True(t, insert(1))
	require.False(t, insert(0)) // Gap
	require.False(t, insert(1)) // Gap
	require.True(t, insert(2))

	sc, err := ToStream(dbc, events, "", "", "")(ctx, "")
	jtest.RequireNil(t, err)

	for i := 0; i < 3; i++ {
		e, err := sc.Recv()
		jtest.RequireNil(t, err, i)
		require.True(t, strings.HasSuffix(e.ForeignID, fmt.Sprint(i)))
		if i < 2 {
			require.Equal(t, int64(i+1), e.IDInt())
		} else {
			require.Equal(t, int64(i+3), e.IDInt()) // Gaps skipped
		}
	}

	require.Equal(t, 2.0, testutil.ToFloat64(eventsGapFilledCounter))
}
