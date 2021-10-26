package jet_test

import (
	"context"
	"testing"
	"time"

	"github.com/corverroos/delayq/dqradix"
	"github.com/luno/jettison/jtest"
	"github.com/luno/reflex"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/internal"
	"github.com/corverroos/replay/jet"
)

const (
	cursor = "cursor"
	ns     = "namespace"
	w      = "workflow"
	r      = "run"
)

var _ replay.Client = jet.Client{}

func setup(t *testing.T) (context.Context, nats.JetStreamContext, jet.Client, dqradix.Client) {
	jc, ncl := jet.NewForTesting(t, "reflex")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rc := dqradix.NewForTesting(t, cursor)

	return ctx, ncl, jc, rc
}

func TestEvents(t *testing.T) {
	ctx, _, cl, _ := setup(t)

	pb := durationpb.New(time.Second)

	b, err := cl.RunWorkflow(ctx, ns, w, r, pb)
	jtest.RequireNil(t, err)
	require.True(t, b)

	b, err = cl.RunWorkflow(ctx, ns, w, r, pb)
	jtest.RequireNil(t, err)
	require.False(t, b)

	err = cl.InsertEvent(ctx, internal.NoopEvent, internal.RunKey(ns, w, r, 0), pb)
	jtest.RequireNil(t, err)

	err = cl.InsertEvent(ctx, internal.RunOutput, internal.RunKey(ns, w, r, 0), pb)
	jtest.RequireNil(t, err)

	ok, err := cl.SignalRun(ctx, ns, w, r, "signal", pb, "extID")
	jtest.RequireNil(t, err)
	require.True(t, ok)
	ok, err = cl.SignalRun(ctx, ns, w, r, "signal", pb, "extID")
	jtest.RequireNil(t, err)
	require.False(t, ok)

	ok, err = cl.SignalRun(ctx, ns, w, r, "signal", pb, "extID2")
	jtest.RequireNil(t, err)
	require.True(t, ok)

	err = cl.CompleteRun(ctx, internal.RunKey(ns, w, r, 0))
	jtest.RequireNil(t, err)

	err = cl.RestartRun(ctx, internal.RunKey(ns, w, r, 0), pb)
	jtest.RequireNil(t, err)

	el, err := cl.ListBootstrapEvents(ctx, internal.RunKey(ns, w, r, 0), "6")
	jtest.RequireNil(t, err)
	require.Len(t, el, 7)

	el, err = cl.ListBootstrapEvents(ctx, internal.RunKey(ns, w, r, 1), "7")
	jtest.RequireNil(t, err)
	require.Len(t, el, 2)

	sc, err := cl.Stream(ns, w, r)(ctx, "", reflex.WithStreamToHead())
	jtest.RequireNil(t, err)

	for i := 0; i < 8; i++ {
		e, err := sc.Recv()
		jtest.RequireNil(t, err)
		require.Equal(t, e.IDInt(), int64(i)+1)
	}

	_, err = sc.Recv()
	jtest.Require(t, err, reflex.ErrHeadReached)
}
