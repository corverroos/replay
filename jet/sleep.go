package jet

import (
	"context"
	"path"
	"time"

	"github.com/corverroos/delayq"
	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	"github.com/luno/reflex"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/internal"
	"github.com/corverroos/replay/internal/replaypb"
)

func RegisterSleeperForTesting(getCtx func() (context.Context, bool), cl replay.Client, cstore reflex.CursorStore, q *delayq.Queue) {
	pollPeriod = time.Millisecond * 10
	deadlineFunc = func(d *durationpb.Duration) time.Time {
		return time.Now()
	}
	RegisterSleeper(getCtx, cl, cstore, q, "")
}

func RegisterSleeper(getCtx func() (context.Context, bool), cl replay.Client, cstore reflex.CursorStore, q *delayq.Queue, cursorPrefix string) {
	fn := func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
		if !reflex.IsType(e.Type, internal.SleepRequest) {
			return nil
		}

		message, err := internal.ParseMessage(e)
		if err != nil {
			return err
		}

		req, ok := message.(*replaypb.SleepRequest)
		if !ok {
			return errors.New("invalid sleep request")
		}

		err = q.AddMsg(ctx, &delayq.Msg{
			ID:       e.ForeignID,
			Deadline: deadlineFunc(req.Duration),
		})
		if errors.Is(err, delayq.ErrExists) {
			return nil
		} else if err != nil {
			return err
		}

		return nil
	}

	name := path.Join(cursorPrefix, "replay_activity", "internal", internal.SleepTarget)
	consumer := reflex.NewConsumer(name, fn, reflex.WithoutConsumerActivityTTL())
	spec := reflex.NewSpec(cl.Stream("", "", ""), cstore, consumer)
	go runForever(getCtx, spec)
	go completeSleepsForever(getCtx, cl.Internal(), q)
}

var (
	pollPeriod   = time.Second
	deadlineFunc = func(d *durationpb.Duration) time.Time {
		return time.Now().Add(d.AsDuration())
	}
)

func completeSleepsForever(getCtx func() (context.Context, bool), cl internal.Client, q *delayq.Queue) {
	for {
		ctx, ok := getCtx()
		if !ok {
			return
		}

		err := q.Dequeue(ctx, func(msg *delayq.Msg) error {
			key, err := internal.DecodeKey(msg.ID)
			if err != nil {
				return err
			}

			return cl.InsertEvent(ctx, internal.SleepResponse, key, &replaypb.SleepDone{})

		}, delayq.WithPollPeriod(pollPeriod))
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "complete sleeps once"))
		}
	}
}

// runForever continuously calls the run function, backing off
// and logging on unexpected errors.
func runForever(getCtx func() (context.Context, bool), req reflex.Spec) {
	for {
		ctx, ok := getCtx()
		if !ok {
			return
		}

		ctx = log.ContextWith(ctx, j.KS("consumer", req.Name()))

		err := reflex.Run(ctx, req)
		if reflex.IsExpected(err) {
			// Just retry on expected errors.
			time.Sleep(time.Millisecond * 100) // Don't spin
			continue
		}

		log.Error(ctx, errors.Wrap(err, "run forever error"))
		time.Sleep(time.Second) // 1 sec backoff on errors
	}
}
