package jet

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/corverroos/rjet"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/corverroos/replay/internal"
)

const (
	prefix = "replay"
)

func New(ncl nats.JetStreamContext, stream string) Client {
	return Client{
		ncl:    ncl,
		stream: stream,
	}
}

type Client struct {
	ncl    nats.JetStreamContext
	stream string
}

// StreamConfig returns the config of the underlying nats stream.
func (c Client) StreamConfig() *nats.StreamConfig {
	return &nats.StreamConfig{
		Name:        c.stream,
		Description: "replay stream",
		Subjects:    []string{joinsub(prefix, c.stream, ">")},
		Retention:   nats.LimitsPolicy,
		Discard:     nats.DiscardOld,
		MaxAge:      time.Hour * 24 * 30 * 6, // 6 months.
		Storage:     nats.FileStorage,
		Replicas:    0, // TODO(corver): Non zero probably
		Duplicates:  time.Minute * 5,
	}
}

// InsertEvent inserts the event (type, key, message) into the stream.
// It is a method on the internal.Client interface.
func (c Client) InsertEvent(ctx context.Context, typ internal.EventType, key internal.Key, message proto.Message) error {
	_, err := c.insertEvent(ctx, typ, key, message)
	return err
}

func (c Client) insertEvent(ctx context.Context, typ internal.EventType, key internal.Key, message proto.Message) (uint64, error) {
	b, err := proto.Marshal(message)
	if err != nil {
		return 0, err
	}

	m := newMsg(c.stream, typ, key, b)

	ack, err := c.ncl.PublishMsg(m, nats.Context(ctx))
	return ack.Sequence, err
}

// CompleteRun inserts a RunCompleted event for key.
// It is a method on the internal.Client interface.
func (c Client) CompleteRun(ctx context.Context, key internal.Key) error {
	m := newMsg(c.stream, internal.RunCompleted, key, nil)

	_, err := c.ncl.PublishMsg(m, nats.Context(ctx))
	return err
}

// RestartRun inserts a RunCompleted and a RunCreated event for key.
// It is a method on the internal.Client interface.
func (c Client) RestartRun(ctx context.Context, key internal.Key, message proto.Message) error {
	err := c.CompleteRun(ctx, key)
	if err != nil {
		return err
	}

	_, err = c.runWorkflow(ctx, key.Namespace, key.Workflow, key.Run, key.Iteration+1, message)
	return err
}

func (c Client) RunWorkflow(ctx context.Context, namespace, workflow, run string, message proto.Message) (bool, error) {
	return c.runWorkflow(ctx, namespace, workflow, run, 0, message)
}

func (c Client) runWorkflow(ctx context.Context, namespace, workflow, run string, iter int, message proto.Message) (bool, error) {
	key := internal.RunKey(namespace, workflow, run, iter)

	b, err := proto.Marshal(message)
	if err != nil {
		return false, err
	}

	m := newMsg(c.stream, internal.RunCreated, key, b)

	ack, err := c.ncl.PublishMsg(m, nats.Context(ctx))
	if err != nil {
		return false, err
	}

	return !ack.Duplicate, nil
}

func (c Client) SignalRun(ctx context.Context, namespace, workflow, run string, signal string, message proto.Message, extID string) (bool, error) {
	key := internal.SignalKey(namespace, workflow, run, signal, extID)

	apb, err := internal.ToAny(message)
	if err != nil {
		return false, err
	}

	b, err := proto.Marshal(apb)
	if err != nil {
		return false, err
	}

	m := newMsg(c.stream, internal.RunSignal, key, b)

	ack, err := c.ncl.PublishMsg(m, nats.Context(ctx))
	return !ack.Duplicate, err
}

func (c Client) ListBootstrapEvents(ctx context.Context, key internal.Key, to string) ([]reflex.Event, error) {

	stream := func(sub string, after string, to string) ([]reflex.Event, error) {
		s, err := rjet.NewStream(c.ncl, c.stream, rjet.WithSubjectFilter(sub))
		if err != nil {
			return nil, err
		}

		// Add timeout just in case 'to' isn't found.
		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()

		sc, err := s.Stream(ctx, after, reflex.WithStreamToHead())
		if err != nil {
			return nil, err
		}

		var res []reflex.Event
		for {
			e, err := sc.Recv()
			if errors.Is(err, reflex.ErrHeadReached) {
				return res, nil
			} else if err != nil {
				return nil, err
			}

			res = append(res, *e)

			if e.ID == to {
				return res, nil
			}
		}
	}

	sub := joinsub(
		prefix,
		c.stream,
		key.Namespace,
		key.Workflow,
		key.Run,
		strconv.Itoa(key.Iteration),
		">")

	el, err := stream(sub, "", to)
	if err != nil || len(el) == 0 {
		return nil, err
	}

	// Stream signals that overlap with run iteration.

	// Insert noop to ensure stream can complete
	noop := internal.SignalKey(key.Namespace, key.Workflow, key.Run, "noop", strconv.FormatInt(time.Now().Unix(), 10))
	noopSeq, err := c.insertEvent(ctx, internal.NoopEvent, noop, nil)
	if err != nil {
		return nil, err
	}

	sub = joinsub(
		prefix,
		c.stream,
		key.Namespace,
		key.Workflow,
		key.Run,
		strconv.Itoa(-1),
		">")

	sl, err := stream(sub, el[0].ID, strconv.FormatUint(noopSeq, 10))
	if err != nil {
		return nil, err
	}

	el = append(el, sl...)
	sort.Slice(el, func(i, j int) bool {
		return el[i].IDInt() < el[j].IDInt()
	})

	return el, nil
}

func (c Client) Stream(namespace, workflow, run string) reflex.StreamFunc {
	sub := joinsub(
		prefix,
		c.stream,
		namespace,
		workflow,
		run,
		">")

	s, err := rjet.NewStream(c.ncl, c.stream, rjet.WithSubjectFilter(sub))
	if err != nil {
		panic("unexpected error" + err.Error())
	}

	return s.Stream
}

func (c Client) Internal() internal.Client {
	return c
}

func newMsg(stream string, typ reflex.EventType, key internal.Key, data []byte) *nats.Msg {
	h := nats.Header{}
	rjet.SetTypeHeader(h, typ)

	// See https://docs.nats.io/jetstream/model_deep_dive#message-deduplication.
	h.Set(nats.MsgIdHdr, fmt.Sprintf("%s-%d", key.Encode(), typ.ReflexType()))

	return &nats.Msg{
		Subject: keyToSubject(stream, key),
		Header:  h,
		Data:    data,
	}
}

func keyToSubject(stream string, k internal.Key) string {
	if k.Target == "" {
		return joinsub(
			prefix,
			stream,
			k.Namespace,
			k.Workflow,
			k.Run,
			strconv.Itoa(k.Iteration),
			"_")
	}

	return joinsub(
		prefix,
		stream,
		k.Namespace,
		k.Workflow,
		k.Run,
		strconv.Itoa(k.Iteration),
		k.Target,
		k.Sequence)
}

func joinsub(strs ...string) string {
	return strings.Join(strs, ".")
}
