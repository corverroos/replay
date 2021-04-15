package logical

import (
	"context"
	"database/sql"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/internal"
	"github.com/corverroos/replay/internal/db"
	"github.com/corverroos/replay/internal/signal"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
)

func New(dbc *sql.DB) replay.Client {
	return &Client{dbc: dbc}
}

type Client struct {
	dbc *sql.DB
}

func (c *Client) RunWorkflow(ctx context.Context, namespace, workflow, run string, message proto.Message) error {
	key := internal.MinKey(namespace, workflow, run, 0)
	return c.RunWorkflowInternal(ctx, key, message)
}

func (c *Client) RunWorkflowInternal(ctx context.Context, key string, message proto.Message) error {
	b, err := toBytes(message)
	if err != nil {
		return err
	}

	return db.Insert(ctx, c.dbc, key, internal.CreateRun, b)
}

func (c *Client) SignalRun(ctx context.Context, namespace, workflow, run string, s replay.Signal, message proto.Message, extID string) error {
	apb, err := internal.ToAny(message)
	if err != nil {
		return err
	}

	return signal.Insert(ctx, c.dbc, namespace, workflow, run, s.SignalType(), apb, extID)
}

func (c *Client) RequestActivity(ctx context.Context, key string, message proto.Message) error {
	b, err := toBytes(message)
	if err != nil {
		return err
	}

	return swallowErrDup(db.Insert(ctx, c.dbc, key, internal.ActivityRequest, b))
}

func (c *Client) RespondActivity(ctx context.Context, key string, message proto.Message) error {
	apb, err := internal.ToAny(message)
	if err != nil {
		return err
	}

	return c.RespondActivityRaw(ctx, key, apb)
}

func (c *Client) RespondActivityRaw(ctx context.Context, key string, message *any.Any) error {
	b, err := internal.Marshal(message)
	if err != nil {
		return err
	}

	return swallowErrDup(db.Insert(ctx, c.dbc, key, internal.ActivityResponse, b))
}

func (c *Client) CompleteRun(ctx context.Context, namespace, workflow, run string, iter int) error {
	return swallowErrDup(db.Insert(ctx, c.dbc, internal.MinKey(namespace, workflow, run, iter), internal.CompleteRun, nil))
}

func (c *Client) ListBootstrapEvents(ctx context.Context, namespace, workflow, run string, iter int) ([]reflex.Event, error) {
	return db.ListBootstrapEvents(ctx, c.dbc, namespace, workflow, run, iter)
}

func (c *Client) Stream(namespace string) reflex.StreamFunc {
	return db.ToStream(c.dbc, namespace)
}

func (c *Client) Internal() internal.Client {
	return c
}

func toBytes(message proto.Message) ([]byte, error) {
	apb, err := internal.ToAny(message)
	if err != nil {
		return nil, err
	} else if apb == nil {
		return nil, nil
	}

	return proto.Marshal(apb)
}

func swallowErrDup(err error) error {
	if errors.Is(err, replay.ErrDuplicate) {
		return nil
	} else if err != nil {
		return err
	}
	return nil
}
