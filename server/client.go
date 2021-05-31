package server

import (
	"context"
	"database/sql"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rsql"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/internal"
	"github.com/corverroos/replay/internal/db"
	"github.com/corverroos/replay/internal/signal"
	"github.com/corverroos/replay/internal/sleep"
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

// StartLoops starts server-side background loops.
func (c *DBClient) StartLoops(getCtx func() context.Context, cstore reflex.CursorStore, cursorPrefix string) {
	sleep.Register(getCtx, c, cstore, c.dbc, cursorPrefix)
	signal.Register(getCtx, c, cstore, c.dbc, cursorPrefix)
	db.FillGaps(c.dbc, c.events)
}

func (c *DBClient) RunWorkflow(ctx context.Context, namespace, workflow, run string, message proto.Message) (bool, error) {
	if err := validateNames(namespace, workflow, run); err != nil {
		return false, err
	}

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

	return swallowErrDup(db.Insert(ctx, c.dbc, c.events, key, internal.RunCreated, b))
}

func (c *DBClient) EmitOutput(ctx context.Context, key string, message proto.Message) error {
	apb, err := internal.ToAny(message)
	if err != nil {
		return err
	}

	return c.emitOutputServer(ctx, key, apb)
}

func (c *DBClient) emitOutputServer(ctx context.Context, key string, message *any.Any) error {
	b, err := internal.Marshal(message)
	if err != nil {
		return err
	}

	_, err = swallowErrDup(db.Insert(ctx, c.dbc, c.events, key, internal.RunOutput, b))
	return err
}

func (c *DBClient) SignalRun(ctx context.Context, namespace, workflow, run string, s replay.Signal, message proto.Message, extID string) (bool, error) {
	if err := validateNames(namespace, workflow, run); err != nil {
		return false, err
	}

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
	_, err := swallowErrDup(db.Insert(ctx, c.dbc, c.events, key, internal.RunCompleted, nil))
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

func (c *DBClient) Stream(namespace, workflow, run string) reflex.StreamFunc {
	return db.ToStream(c.dbc, c.events, namespace, workflow, run)
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

func validateNames(names ...string) error {
	for _, name := range names {
		if name == "" {
			return errors.New("replay names may not be empty")
		}

		if strings.Contains(name, "/") {
			return errors.New("replay names may not contain '/'", j.KS("name", name))
		}
	}

	return nil
}
