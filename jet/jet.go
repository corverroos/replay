package jet

import (
	"context"
	"errors"
	"fmt"
	"github.com/corverroos/replay/internal"
	"github.com/corverroos/rjet"
	"github.com/golang/protobuf/proto"
	"github.com/luno/reflex"
	"github.com/nats-io/nats.go"
	"strconv"
	"strings"
	"time"
)

const (
	prefix = "replay"
)

type Client struct {
	ncl    nats.JetStreamContext
	stream string
}

func (c Client) RequestActivity(ctx context.Context, key internal.Key, message proto.Message) error {
	b, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	sub, err := keyToSubject(key)
	if err != nil {
		return err
	}

	m := nats.Msg{
		Subject: sub,
		Data:    b,
	}

	_, err = c.ncl.PublishMsg(&m, nats.Context(ctx), msgID(key, internal.ActivityRequest))
	return err
}

func (c Client) RespondActivity(ctx context.Context, key internal.Key, message proto.Message) error {
	b, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	sub, err := keyToSubject(key)
	if err != nil {
		return err
	}

	m := nats.Msg{
		Subject: sub,
		Data:    b,
	}

	_, err = c.ncl.PublishMsg(&m, nats.Context(ctx), msgID(key, internal.ActivityResponse))
	return err
}

func (c Client) EmitOutput(ctx context.Context, key internal.Key, message proto.Message) error {
	b, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	sub, err := keyToSubject(key)
	if err != nil {
		return err
	}

	m := nats.Msg{
		Subject: sub,
		Data:    b,
	}

	_, err = c.ncl.PublishMsg(&m, nats.Context(ctx), msgID(key, internal.RunOutput))
	return err
}

func (c Client) CompleteRun(ctx context.Context, key internal.Key) error {
	sub, err := keyToSubject(key)
	if err != nil {
		return err
	}

	m := nats.Msg{
		Subject: sub,
	}

	_, err = c.ncl.PublishMsg(&m, nats.Context(ctx), msgID(key, internal.RunCompleted))
	return err
}

func (c Client) ListBootstrapEvents(ctx context.Context, key internal.Key, to string) ([]reflex.Event, error) {
	sub, err := keyToSubject(key)
	if err != nil {
		return nil, err
	}

	fmt.Printf("JCR: sub=%+v\n", sub)
	sub += ".*"

	s, err := rjet.NewStream(c.ncl, c.stream, rjet.WithSubjectFilter(sub))
	if err != nil {
		return nil, err
	}

	// Add timeout just in case before isn't found.
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	sc, err := s.Stream(ctx, "")
	if err != nil {
		return nil, err
	}

	var res []reflex.Event
	for {
		e, err := sc.Recv()
		if err != nil {
			return nil, err
		}

		res = append(res, *e)

		if e.ID == to {
			return res, nil
		}
	}
}

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
	key := internal.MinKey(namespace, workflow, run, iter)

	b, err := proto.Marshal(message)
	if err != nil {
		return false, err
	}

	sub, err := keyToSubject(key)
	if err != nil {
		return false, err
	}

	m := nats.Msg{
		Subject: sub,
		Data:    b,
	}

	ack, err := c.ncl.PublishMsg(&m, nats.Context(ctx), msgID(key, internal.RunCompleted))
	if err != nil {
		return false, err
	}

	return !ack.Duplicate, nil
}

func (c Client) SignalRun(ctx context.Context, namespace, workflow, run string, signal string, message proto.Message, extID string) (bool, error) {
	return false, errors.New("signal not supported yet")
}

func (c Client) Stream(namespace, workflow, run string) reflex.StreamFunc {
	sub := strings.Join([]string{
		prefix,
		namespace,
		workflow,
		run,
		"*",
	}, ".")

	s, err := rjet.NewStream(c.ncl, c.stream, rjet.WithSubjectFilter(sub))
	if err != nil {
		panic("unexpected error" + err.Error())
	}

	return s.Stream
}

func (c Client) Internal() internal.Client {
	return c
}

// msgID returns a unique nats msg id publish option for the key and type used for deduplication.
// See https://docs.nats.io/jetstream/model_deep_dive#message-deduplication.
func msgID(key internal.Key, typ internal.EventType) nats.PubOpt {
	return nats.MsgId(fmt.Sprintf("%s-%d", key.Encode(), typ))
}

func keyToSubject(k internal.Key) (string, error) {
	// TODO(corver): Refactor replay API to use typed key instead.

	return strings.Join([]string{
		prefix,
		k.Namespace,
		k.Workflow,
		k.Run,
		strconv.Itoa(k.Iteration),
		k.Target,
		k.Sequence,
	}, "."), nil
}
