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

var _ replay.Client = (*Client)(nil)

func New(dbc *sql.DB) *Client {
	return &Client{dbc: dbc}
}

type Client struct {
	dbc *sql.DB
}

func (c *Client) RunWorkflow(ctx context.Context, workflow, run string, message proto.Message) error {
	b, err := toBytes(message)
	if err != nil {
		return err
	}

	return db.Insert(ctx, c.dbc, internal.ShortKey(workflow, run), internal.CreateRun, b)
}

func (c *Client) SignalRun(ctx context.Context, workflow, run string, s replay.Signal, message proto.Message, extID string) error {
	apb, err := internal.ToAny(message)
	if err != nil {
		return err
	}

	return signal.Insert(ctx, c.dbc, workflow, run, s.SignalType(), apb, extID)
}

func (c *Client) RequestActivity(ctx context.Context, key string, message proto.Message) error {
	b, err := toBytes(message)
	if err != nil {
		return err
	}

	return swallowErrDup(db.Insert(ctx, c.dbc, key, internal.ActivityRequest, b))
}

func (c *Client) CompleteActivity(ctx context.Context, key string, message proto.Message) error {
	apb, err := internal.ToAny(message)
	if err != nil {
		return err
	}

	return c.CompleteActivityRaw(ctx, key, apb)
}

func (c *Client) CompleteActivityRaw(ctx context.Context, key string, message *any.Any) error {
	b, err := internal.Marshal(message)
	if err != nil {
		return err
	}

	return swallowErrDup(db.Insert(ctx, c.dbc, key, internal.ActivityResponse, b))
}

func (c *Client) CompleteRun(ctx context.Context, workflow, run string) error {
	return swallowErrDup(db.Insert(ctx, c.dbc, internal.ShortKey(workflow, run), internal.CompleteRun, nil))
}

func (c *Client) ListBootstrapEvents(ctx context.Context, workflow, run string) ([]reflex.Event, error) {
	return db.ListBootstrapEvents(ctx, c.dbc, workflow, run)
}

func (c *Client) Stream(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error) {
	return db.ToStream(c.dbc)(ctx, after, opts...)
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
