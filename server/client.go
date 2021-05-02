package server

import (
	"context"
	"database/sql"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rsql"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/internal"
	"github.com/corverroos/replay/internal/db"
	"github.com/corverroos/replay/internal/signal"
)

var _ replay.Client = (*DBClient)(nil)

// NewDBClient returns a new server side database client.
//
// TODO(corver): Add support for custom events table schemas.
func NewDBClient(dbc *sql.DB, opts ...rsql.EventsOption) *DBClient {
	return &DBClient{
		dbc:    dbc,
		events: db.DefaultEvents().Clone(opts...),
	}
}

// DBClient defines the server-side database client used by the replay server. This client can
// also be used to "embed" replay in a user application since it also implements internal.Client and replay.Client.
type DBClient struct {
	dbc    *sql.DB
	events *rsql.EventsTable
}

func (c *DBClient) Events() *rsql.EventsTable {
	return c.events
}

func (c *DBClient) RunWorkflow(ctx context.Context, namespace, workflow, run string, message proto.Message) (bool, error) {
	apb, err := internal.ToAny(message)
	if err != nil {
		return false, err
	}

	key := internal.MinKey(namespace, workflow, run, 0)

	return c.runWorkflowServer(ctx, key, apb)
}

func (c *DBClient) runWorkflowServer(ctx context.Context, key string, message *any.Any) (bool, error) {
	b, err := internal.Marshal(message)
	if err != nil {
		return false, err
	}

	return swallowErrDup(db.Insert(ctx, c.dbc, c.events, key, internal.CreateRun, b))
}

func (c *DBClient) SignalRun(ctx context.Context, namespace, workflow, run string, s replay.Signal, message proto.Message, extID string) (bool, error) {
	apb, err := internal.ToAny(message)
	if err != nil {
		return false, err
	}

	return c.signalRunServer(ctx, namespace, workflow, run, s.SignalType(), apb, extID)
}

func (c *DBClient) signalRunServer(ctx context.Context, namespace, workflow, run string, signalType int, message *any.Any, extID string) (bool, error) {
	return swallowErrDup(signal.Insert(ctx, c.dbc, namespace, workflow, run, signalType, message, extID))
}

func (c *DBClient) RequestActivity(ctx context.Context, key string, message proto.Message) error {
	apb, err := internal.ToAny(message)
	if err != nil {
		return err
	}

	return c.requestActivityServer(ctx, key, apb)
}

func (c *DBClient) requestActivityServer(ctx context.Context, key string, message *any.Any) error {
	b, err := internal.Marshal(message)
	if err != nil {
		return err
	}

	_, err = swallowErrDup(db.Insert(ctx, c.dbc, c.events, key, internal.ActivityRequest, b))
	return err
}

func (c *DBClient) RespondActivity(ctx context.Context, key string, message proto.Message) error {
	apb, err := internal.ToAny(message)
	if err != nil {
		return err
	}

	return c.RespondActivityServer(ctx, key, apb)
}

func (c *DBClient) RespondActivityServer(ctx context.Context, key string, message *any.Any) error {
	b, err := internal.Marshal(message)
	if err != nil {
		return err
	}

	_, err = swallowErrDup(db.Insert(ctx, c.dbc, c.events, key, internal.ActivityResponse, b))
	return err
}

func (c *DBClient) CompleteRun(ctx context.Context, key string) error {
	_, err := swallowErrDup(db.Insert(ctx, c.dbc, c.events, key, internal.CompleteRun, nil))
	return err
}

func (c *DBClient) RestartRun(ctx context.Context, key string, message proto.Message) error {
	apb, err := internal.ToAny(message)
	if err != nil {
		return err
	}

	return c.restartRunServer(ctx, key, apb)
}

func (c *DBClient) restartRunServer(ctx context.Context, key string, message proto.Message) error {
	b, err := internal.Marshal(message)
	if err != nil {
		return err
	}

	_, err = swallowErrDup(db.RestartRun(ctx, c.dbc, c.events, key, b))
	return err
}

func (c *DBClient) ListBootstrapEvents(ctx context.Context, key string) ([]reflex.Event, error) {
	return db.ListBootstrapEvents(ctx, c.dbc, c.events, key)
}

func (c *DBClient) Stream(namespace string) reflex.StreamFunc {
	return db.ToStream(c.dbc, c.events, namespace)
}

func (c *DBClient) Internal() internal.Client {
	return c
}

// swallowErrDup returns true if no error, or false if the error is ErrDuplicate or the error.
func swallowErrDup(err error) (bool, error) {
	if errors.Is(err, internal.ErrDuplicate) {
		// NoReturnErr: Swallow
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}
