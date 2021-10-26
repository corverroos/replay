package jet

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/corverroos/rjet"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/jtest"
	"github.com/luno/reflex"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/corverroos/replay/internal"
)

const (
	prefix = "replay"
	keyHdr = "replay_key"
)

func New(ncl nats.JetStreamContext, stream string) Client {
	return Client{
		ncl:    ncl,
		stream: stream,
	}
}

func NewForTesting(t *testing.T, cleanStreams ...string) (Client, nats.JetStreamContext) {
	c, err := nats.Connect(nats.DefaultURL,
		nats.ReconnectJitter(time.Millisecond, time.Millisecond))
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

	const stream = "stream"

	clean := func() {
		for sinfo := range ncl.StreamsInfo() {
			name := sinfo.Config.Name
			if strings.HasPrefix(name, stream) {
				_ = ncl.PurgeStream(name)
				_ = ncl.DeleteStream(name)
			}
			for _, stream := range cleanStreams {
				if strings.HasPrefix(name, stream) {
					_ = ncl.PurgeStream(name)
					_ = ncl.DeleteStream(name)
				}
			}
		}
	}

	clean()
	t.Cleanup(func() {
		clean()
		c.Close()
	})

	jc := New(ncl, stream)

	_, err = ncl.AddStream(jc.StreamConfig())
	jtest.RequireNil(t, err)

	return jc, ncl
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

func (c Client) insertEvent(ctx context.Context, typ internal.EventType, key internal.Key, message proto.Message) (*nats.PubAck, error) {
	msg, err := newMsg(c.stream, typ, key, message)
	if err != nil {
		return nil, err
	}

	ack, err := c.ncl.PublishMsg(msg, nats.Context(ctx))
	if err != nil {
		return nil, err
	}

	return ack, nil
}

// CompleteRun inserts a RunCompleted event for key.
// It is a method on the internal.Client interface.
func (c Client) CompleteRun(ctx context.Context, key internal.Key) error {
	msg, err := newMsg(c.stream, internal.RunCompleted, key, nil)
	if err != nil {
		return err
	}

	_, err = c.ncl.PublishMsg(msg, nats.Context(ctx))
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

	msg, err := newMsg(c.stream, internal.RunCreated, key, message)
	if err != nil {
		return false, err
	}

	ack, err := c.ncl.PublishMsg(msg, nats.Context(ctx))
	if err != nil {
		return false, err
	}

	return !ack.Duplicate, nil
}

func (c Client) SignalRun(ctx context.Context, namespace, workflow, run string, signal string, message proto.Message, extID string) (bool, error) {
	key := internal.SignalKey(namespace, workflow, run, signal, extID)

	msg, err := newMsg(c.stream, internal.RunSignal, key, message)
	if err != nil {
		return false, err
	}

	ack, err := c.ncl.PublishMsg(msg, nats.Context(ctx))
	if err != nil {
		return false, err
	}

	return !ack.Duplicate, nil
}

func (c Client) ListBootstrapEvents(ctx context.Context, key internal.Key, to string) ([]reflex.Event, error) {

	stream := func(sub string, after string, to string) ([]reflex.Event, error) {
		s, err := rjet.NewStream(c.ncl, c.stream, rjet.WithSubjectFilter(sub), rjet.WithForeignIDParser(parseForeignID))
		if err != nil {
			return nil, err
		}

		// Add timeout just in case 'to' isn't found.
		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()

		sc, err := s.Stream(ctx, after)
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

			if !reflex.IsType(e.Type, internal.NoopEvent) {
				res = append(res, *e)
			}

			if e.ID == to {
				return res, nil
			}
		}
	}

	if to == "" {
		key.Target = "noop"
		key.Sequence = strconv.FormatInt(time.Now().UnixNano(), 10)
		ack, err := c.insertEvent(ctx, internal.NoopEvent, key, nil)
		if err != nil {
			return nil, err
		} else if ack.Duplicate {
			return nil, errors.New("bug: bootstrap events noop duplicate", j.KS("key", key.Encode()))
		}

		to = strconv.FormatUint(ack.Sequence, 10)
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
	noop := internal.SignalKey(key.Namespace, key.Workflow, key.Run, "noop", strconv.FormatInt(time.Now().UnixNano(), 10))
	ack, err := c.insertEvent(ctx, internal.NoopEvent, noop, nil)
	if err != nil {
		return nil, err
	} else if ack.Duplicate {
		return nil, errors.New("bug: bootstrap signal noop duplicate", j.KS("key", noop.Encode()))
	}

	sub = joinsub(
		prefix,
		c.stream,
		key.Namespace,
		key.Workflow,
		key.Run,
		strconv.Itoa(-1),
		">")

	sl, err := stream(sub, el[0].ID, strconv.FormatUint(ack.Sequence, 10))
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
	var subl = []string{prefix, c.stream}

	func(sl ...string) {
		for i := 0; i < len(sl); i++ {
			if sl[i] == "" {
				return
			}
			subl = append(subl, sl[i])
		}
	}(namespace, workflow, run)

	subl = append(subl, ">")

	s, err := rjet.NewStream(c.ncl, c.stream, rjet.WithSubjectFilter(joinsub(subl...)), rjet.WithForeignIDParser(parseForeignID))
	if err != nil {
		panic("unexpected error" + err.Error())
	}

	return s.Stream
}

func (c Client) Internal() internal.Client {
	return c
}

func newMsg(stream string, typ reflex.EventType, key internal.Key, message proto.Message) (*nats.Msg, error) {
	apb, err := internal.ToAny(message)
	if err != nil {
		return nil, err
	}

	data, err := internal.Marshal(apb)
	if err != nil {
		return nil, err
	}

	k := key.Encode()
	h := nats.Header{}
	rjet.SetTypeHeader(h, typ)
	h.Set(keyHdr, k)
	h.Set(nats.MsgIdHdr, fmt.Sprintf("%s-%d", k, typ.ReflexType())) // See https://docs.nats.io/jetstream/model_deep_dive#message-deduplication.

	return &nats.Msg{
		Subject: keyToSubject(stream, key),
		Header:  h,
		Data:    data,
	}, nil
}

func keyToSubject(stream string, k internal.Key) string {
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
	for i := 0; i < len(strs); i++ {
		if strs[i] == "" {
			strs[i] = "_"
		}
	}
	return strings.Join(strs, ".")
}

func parseForeignID(msg *nats.Msg) string {
	return msg.Header.Get(keyHdr)
}
