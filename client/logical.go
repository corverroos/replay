package client

import (
	"context"
	"database/sql"
	"github.com/corverroos/replay/db"
	"github.com/golang/protobuf/proto"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
)

func New(dbc *sql.DB) *Client {
	return &Client{dbc: dbc}
}

type Client struct {
	dbc *sql.DB
}

func (c *Client) RunWorkflow(ctx context.Context, workflow, run string, args proto.Message) error {
	return db.Insert(ctx, c.dbc, workflow, run, "", 0, db.CreateRun, args)
}

func (c *Client) RequestActivity(ctx context.Context, workflow, run string, activity string, index int, args proto.Message) error {
	return swallowErrDup(db.Insert(ctx, c.dbc, workflow, run, activity, index, db.ActivityRequest, args))
}

func (c *Client) CompleteActivity(ctx context.Context, workflow, run string, activity string, index int, response proto.Message) error {
	return swallowErrDup(db.Insert(ctx, c.dbc, workflow, run, activity, index, db.ActivityResponse, response))
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
