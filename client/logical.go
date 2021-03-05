package client

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"github.com/corverroos/replay/db"
	"github.com/golang/protobuf/proto"
	"github.com/luno/reflex"
	"math/big"
)

func New(dbc *sql.DB) *Client {
	return &Client{dbc: dbc}
}

type Client struct {
	dbc *sql.DB
}

func (c *Client) CreateRun(ctx context.Context, workflow string, args proto.Message) error {
	return db.Insert(ctx, c.dbc, workflow, randInt63(), "", 0, db.CreateRun, args)
}

func (c *Client) RequestActivity(ctx context.Context, workflow, run string, activity string, index int, args proto.Message) error {
	return db.Insert(ctx, c.dbc, workflow, run, activity, index, db.ActivityRequest, args)
}

func (c *Client) CompleteActivity(ctx context.Context, workflow, run string, activity string, index int, response proto.Message) error {
	return db.Insert(ctx, c.dbc, workflow, run, activity, index, db.ActivityResponse, response)
}

func (c *Client) CompleteRun(ctx context.Context, workflow, run string) error {
	return db.Insert(ctx, c.dbc, workflow, run, "", 0, db.CompleteRun, nil)
}

func (c *Client) ListBootstrapEvents(ctx context.Context, workflow, run string) ([]reflex.Event, error) {
	return db.ListBootstrapEvents(ctx, c.dbc, workflow, run)
}

func (c *Client) Stream(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error) {
	return db.ToStream(c.dbc)(ctx, after, opts...)
}

// 63-bit numbers are useful because you don't have to worry about
// int64 vs uint64.
func randInt63() (string) {
	max := big.NewInt(1)
	max.Lsh(max, 63)
	n, _ := rand.Int(rand.Reader, max)
	return fmt.Sprint(n.Int64())
}
