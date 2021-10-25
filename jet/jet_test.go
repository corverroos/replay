package jet_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/corverroos/delayq/dqradix"
	"github.com/luno/jettison/jtest"
	"github.com/luno/reflex"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/corverroos/replay/internal"
	"github.com/corverroos/replay/jet"
)

const (
	cursor = "cursor"
	stream = "replay_stream"
	ns     = "namespace"
	w      = "workflow"
	r      = "run"
)

func setup(t *testing.T) (context.Context, nats.JetStreamContext, jet.Client, dqradix.Client) {
	c, err := nats.Connect(nats.DefaultURL)
	jtest.RequireNil(t, err)

	ncl, err := c.JetStream()
	jtest.RequireNil(t, err)

	t0 := time.Now()
	for {
		status := c.Status()
		if status == nats.CONNECTED {
			break
		}
		if time.Since(t0) > time.Second {
			t.Fatalf("nats not connected")
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rc := dqradix.NewForTesting(t, cursor)
	jtest.RequireNil(t, err)

	clean := func() {
		for sinfo := range ncl.StreamsInfo() {
			name := sinfo.Config.Name
			if strings.HasPrefix(name, stream) || strings.HasPrefix(name, "reflex") {
				_ = ncl.PurgeStream(name)
				_ = ncl.DeleteStream(name)
			}
		}
	}

	clean()
	t.Cleanup(func() {
		clean()
		c.Close()
	})

	jc := jet.New(ncl, stream)

	_, err = ncl.AddStream(jc.StreamConfig())
	jtest.RequireNil(t, err)

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
