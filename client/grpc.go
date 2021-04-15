package client

import (
	"context"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/internal"
	pb "github.com/corverroos/replay/internal/replaypb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/luno/reflex"
	"github.com/luno/reflex/reflexpb"
	"google.golang.org/grpc"
)

func New(cc *grpc.ClientConn) replay.Client {
	return &Client{clpb: pb.NewReplayClient(cc)}
}

type Client struct {
	clpb pb.ReplayClient
}

func (c *Client) RunWorkflow(ctx context.Context, namespace, workflow, run string, message proto.Message) error {
	key := internal.MinKey(namespace, workflow, run, 0)

	return c.RunWorkflowInternal(ctx, key, message)
}

func (c *Client) RunWorkflowInternal(ctx context.Context, key string, message proto.Message) error {
	anyMsg, err := internal.ToAny(message)
	if err != nil {
		return err
	}

	_, err = c.clpb.RunWorkflow(ctx, &pb.RunRequest{
		Key:     key,
		Message: anyMsg,
	})
	return err
}

func (c *Client) SignalRun(ctx context.Context, namespace, workflow, run string, s replay.Signal, message proto.Message, extID string) error {
	anyMsg, err := internal.ToAny(message)
	if err != nil {
		return err
	}

	_, err = c.clpb.SignalRun(ctx, &pb.SignalRequest{
		Namespace:  namespace,
		Workflow:   workflow,
		Run:        run,
		Message:    anyMsg,
		SignalType: int32(s.SignalType()),
		ExternalId: extID,
	})
	return err
}

func (c *Client) RequestActivity(ctx context.Context, key string, message proto.Message) error {
	anyMsg, err := internal.ToAny(message)
	if err != nil {
		return err
	}

	_, err = c.clpb.RequestActivity(ctx, &pb.ActivityMessage{
		Key:     key,
		Message: anyMsg,
	})
	return err
}

func (c *Client) RespondActivityRaw(ctx context.Context, key string, message *any.Any) error {
	_, err := c.clpb.RespondActivity(ctx, &pb.ActivityMessage{
		Key:     key,
		Message: message,
	})
	return err
}

func (c *Client) RespondActivity(ctx context.Context, key string, message proto.Message) error {
	anyMsg, err := internal.ToAny(message)
	if err != nil {
		return err
	}

	return c.RespondActivityRaw(ctx, key, anyMsg)
}

func (c *Client) CompleteRun(ctx context.Context, namespace, workflow, run string, iter int) error {
	_, err := c.clpb.CompleteRun(ctx, &pb.CompleteRequest{
		Key: internal.MinKey(namespace, workflow, run, iter),
	})
	return err
}

func (c *Client) ListBootstrapEvents(ctx context.Context, namespace, workflow, run string, iter int) ([]reflex.Event, error) {
	rl, err := c.clpb.ListBootstrapEvents(ctx, &pb.ListBootstrapRequest{
		Key: internal.MinKey(namespace, workflow, run, iter),
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

func (c *Client) Stream(namespace string) reflex.StreamFunc {

	return reflex.WrapStreamPB(func(ctx context.Context, req *reflexpb.StreamRequest) (reflex.StreamClientPB, error) {
		return c.clpb.Stream(ctx, &pb.StreamRequest{
			Namespace: namespace,
			Req:       req,
		})
	})
}

func (c *Client) Internal() internal.Client {
	return c
}
