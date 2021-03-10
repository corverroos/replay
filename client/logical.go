package client

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/corverroos/replay"
	"github.com/corverroos/replay/db"
	"github.com/corverroos/replay/internal"
	"github.com/golang/protobuf/proto"
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

func (c *Client) RunWorkflow(ctx context.Context, workflow, run string, args proto.Message) error {
	return db.Insert(ctx, c.dbc, workflow, run, "", 0, db.CreateRun, args)
}

func (c *Client) RequestActivity(ctx context.Context, workflow, run string, activity string, index int, message proto.Message) error {
	return swallowErrDup(db.Insert(ctx, c.dbc, workflow, run, activity, index, db.ActivityRequest, message))
}

func (c *Client) CompleteActivity(ctx context.Context, workflow, run string, activity string, index int, message proto.Message) error {
	return swallowErrDup(db.Insert(ctx, c.dbc, workflow, run, activity, index, db.ActivityResponse, message))
}

func (c *Client) RequestAsyncActivity(ctx context.Context, workflow, run string, activity string, index int, message proto.Message) error {
	return swallowErrDup(db.Insert(ctx, c.dbc, workflow, run, activity, index, db.AsyncActivityRequest, message))
}

func (c *Client) CompleteAsyncActivity(ctx context.Context, token string , message proto.Message) error {
	foreignID, err := internal.UnmarshalAsyncToken(token)
	if err != nil {
		return err
	}

	var id db.EventID
	if err := json.Unmarshal([]byte(foreignID), &id); err != nil {
		return err
	}

	return db.Insert(ctx, c.dbc, id.Workflow, id.Run, id.Activity, id.Index, db.AsyncActivityResponse, message)
}

func (c *Client) CompleteRun(ctx context.Context, workflow, run string) error {
	return swallowErrDup(db.Insert(ctx, c.dbc, workflow, run, "", 0, db.CompleteRun, nil))
}

func (c *Client) ListBootstrapEvents(ctx context.Context, workflow, run string) ([]reflex.Event, error) {
	return db.ListBootstrapEvents(ctx, c.dbc, workflow, run)
}

func (c *Client) Stream(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error) {
	return db.ToStream(c.dbc)(ctx, after, opts...)
}

func swallowErrDup(err error) error {
	if errors.Is(err, db.ErrDuplicate) {
		return nil
	} else if err != nil {
		return err
	}
	return nil
}
