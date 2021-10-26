package jet_test

import (
	"context"
	"testing"
	"time"

	"github.com/corverroos/delayq"
	"github.com/corverroos/rjet"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/corverroos/replay/internal"
	"github.com/corverroos/replay/internal/replaypb"
	"github.com/corverroos/replay/jet"
)

func TestSleep(t *testing.T) {
	ctx, ncl, cl, rc := setup(t)

	cstore := rjet.NewCursorStore(ncl, cursor)
	getCtx := func() (context.Context, bool) {
		return ctx, ctx.Err() == nil
	}

	req := func(d time.Duration) *replaypb.SleepRequest {
		return &replaypb.SleepRequest{Duration: durationpb.New(d)}
	}

	jet.RegisterSleeperForTesting(getCtx, cl, cstore, delayq.New(rc, cursor))

	err := cl.InsertEvent(ctx, internal.SleepRequest, internal.RunKey(ns, w, r, 0), req(time.Hour))
	jtest.RequireNil(t, err)

	sc, err := cl.Stream(ns, w, r)(ctx, "")
	jtest.RequireNil(t, err)

	for i := 0; i < 2; i++ {
		e, err := sc.Recv()
		jtest.RequireNil(t, err, i)
		require.Equal(t, e.IDInt(), int64(i)+1)
		switch i {
		case 0:
			require.Equal(t, e.Type.ReflexType(), internal.SleepRequest.ReflexType())
		case 1:
			require.Equal(t, e.Type.ReflexType(), internal.SleepResponse.ReflexType())
		}
	}
}
