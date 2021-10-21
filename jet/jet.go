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
)

const (
	prefix = "replay"
)

type Client struct {
	ncl    nats.JetStreamContext
	stream string
}

func (c Client) RequestActivity(ctx context.Context, key string, message proto.Message) error {
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

func (c Client) RespondActivity(ctx context.Context, key string, message proto.Message) error {
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

func (c Client) EmitOutput(ctx context.Context, key string, message proto.Message) error {
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

func (c Client) CompleteRun(ctx context.Context, key string) error {
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

func (c Client) ListBootstrapEvents(ctx context.Context, key string) ([]reflex.Event, error) {
	// TODO(corver): Add 'before' eventID.
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

	sc, err := s.Stream(ctx, "", reflex.WithStreamToHead())
	if err != nil {
		return nil, err
	}

	var res []reflex.Event
	for {
		e, err := sc.Recv()
		if errors.Is(err, reflex.ErrHeadReached) {
			break
		} else if err != nil {
			return nil, err
		}

		res = append(res, *e)
	}

	return res, nil
}

func (c Client) RestartRun(ctx context.Context, key string, message proto.Message) error {
	k, err := internal.DecodeKey(key)
	if err != nil {
		return err
	}

	err = c.CompleteRun(ctx, key)
	if err != nil {
		return err
	}

	_, err = c.runWorkflow(ctx, k.Namespace, k.Workflow, k.Run, k.Iteration+1, message)
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
func msgID(key string, typ internal.EventType) nats.PubOpt {
	return nats.MsgId(fmt.Sprintf("%s-%d", key, typ))
}

func keyToSubject(key string) (string, error) {
	k, err := internal.DecodeKey(key)
	if err != nil {
		return "", err
	}

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
