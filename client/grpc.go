// Package client provides a grpc client for the replay grpc server. The client implements replay.Client
// that can by used by the user to run workflows and signal runs. It also provides an internal.Client that
// is used by the replay sdk to execute workflows and activities.
package client

import (
	"context"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/reflex"
	"github.com/luno/reflex/reflexpb"
	"google.golang.org/grpc"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/internal"
	pb "github.com/corverroos/replay/internal/replaypb"
)

func New(cc *grpc.ClientConn) replay.Client {
	return &Client{clpb: pb.NewReplayClient(cc)}
}

type Client struct {
	clpb pb.ReplayClient
}

func (c *Client) RunWorkflow(ctx context.Context, namespace, workflow, run string, message proto.Message) (bool, error) {
	if err := validateNames(namespace, workflow, run); err != nil {
		return false, err
	}

	anyMsg, err := internal.ToAny(message)
	if err != nil {
		return false, err
	}

	_, err = c.clpb.RunWorkflow(ctx, &pb.RunRequest{
		Key:     internal.MinKey(namespace, workflow, run, 0).Encode(),
		Message: anyMsg,
	})
	if errors.Is(err, internal.ErrDuplicate) {
		// NoReturnErr: Return false if duplicate call.
		return false, nil
	} else if err != nil {
		return false, err
	}

	return true, nil
}

func (c *Client) SignalRun(ctx context.Context, namespace, workflow, run string, signal string, message proto.Message, extID string) (bool, error) {
	if err := validateNames(namespace, workflow, run); err != nil {
		return false, err
	}

	anyMsg, err := internal.ToAny(message)
	if err != nil {
		return false, err
	}

	_, err = c.clpb.SignalRun(ctx, &pb.SignalRequest{
		Namespace:  namespace,
		Workflow:   workflow,
		Run:        run,
		Message:    anyMsg,
		Signal:     signal,
		ExternalId: extID,
	})
	if errors.Is(err, internal.ErrDuplicate) {
		// NoReturnErr: Return false if duplicate call.
		return false, nil
	} else if err != nil {
		return false, err
	}

	return true, nil
}

func (c *Client) RequestActivity(ctx context.Context, key internal.Key, message proto.Message) error {
	anyMsg, err := internal.ToAny(message)
	if err != nil {
		return err
	}

	_, err = c.clpb.RequestActivity(ctx, &pb.KeyMessage{
		Key:     key.Encode(),
		Message: anyMsg,
	})
	return err
}

func (c *Client) RespondActivity(ctx context.Context, key internal.Key, message proto.Message) error {
	anyMsg, err := internal.ToAny(message)
	if err != nil {
		return err
	}

	_, err = c.clpb.RespondActivity(ctx, &pb.KeyMessage{
		Key:     key.Encode(),
		Message: anyMsg,
	})
	return err
}

func (c *Client) EmitOutput(ctx context.Context, key internal.Key, message proto.Message) error {
	anyMsg, err := internal.ToAny(message)
	if err != nil {
		return err
	}

	_, err = c.clpb.EmitOutput(ctx, &pb.KeyMessage{
		Key:     key.Encode(),
		Message: anyMsg,
	})
	return err
}

func (c *Client) CompleteRun(ctx context.Context, key internal.Key) error {
	_, err := c.clpb.CompleteRun(ctx, &pb.CompleteRequest{
		Key: key.Encode(),
	})
	return err
}

func (c *Client) RestartRun(ctx context.Context, key internal.Key, message proto.Message) error {
	anyMsg, err := internal.ToAny(message)
	if err != nil {
		return err
	}

	_, err = c.clpb.RestartRun(ctx, &pb.RunRequest{
		Key:     key.Encode(),
		Message: anyMsg,
	})
	return err
}

func (c *Client) ListBootstrapEvents(ctx context.Context, key internal.Key, before string) ([]reflex.Event, error) {
	rl, err := c.clpb.ListBootstrapEvents(ctx, &pb.ListBootstrapRequest{
		Key:    key.Encode(),
		Before: before,
	})
	if err != nil {
		return nil, err
	}

	var res []reflex.Event
	for _, epb := range rl.Events {
		e, err := internal.EventFromProto(epb)
		if err != nil {
			return nil, err
		}

		res = append(res, *e)
	}

	return res, nil
}

func (c *Client) Stream(namespace, workflow, run string) reflex.StreamFunc {
	return reflex.WrapStreamPB(func(ctx context.Context, req *reflexpb.StreamRequest) (reflex.StreamClientPB, error) {
		return c.clpb.Stream(ctx, &pb.StreamRequest{
			Req:       req,
			Namespace: namespace,
			Workflow:  workflow,
			Run:       run,
		})
	})
}

func (c *Client) Internal() internal.Client {
	return c
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
