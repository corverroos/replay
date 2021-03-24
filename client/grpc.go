package client

import (
	"context"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/internal"
	pb "github.com/corverroos/replay/internal/replaypb"
	"github.com/golang/protobuf/proto"
	"github.com/luno/reflex"
	"github.com/luno/reflex/reflexpb"
	"google.golang.org/grpc"
)

var _ replay.Client = (*Client)(nil)

func New(cc *grpc.ClientConn) *Client {
	return &Client{clpb: pb.NewReplayClient(cc)}
}

type Client struct {
	clpb pb.ReplayClient
}

func (c *Client) RunWorkflow(ctx context.Context, workflow, run string, message proto.Message) error {
	anyMsg, err := internal.ToAny(message)
	if err != nil {
		return err
	}

	_, err = c.clpb.RunWorkflow(ctx, &pb.RunRequest{
		Workflow: workflow,
		Run:      run,
		Message:  anyMsg,
	})
	return err
}

func (c *Client) SignalRun(ctx context.Context, workflow, run string, s replay.Signal, message proto.Message, extID string) error {
	anyMsg, err := internal.ToAny(message)
	if err != nil {
		return err
	}

	_, err = c.clpb.SignalRun(ctx, &pb.SignalRequest{
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

	_, err = c.clpb.RequestActivity(ctx, &pb.ActivityRequest{
		Key:     key,
		Message: anyMsg,
	})
	return err
}

func (c *Client) CompleteActivity(ctx context.Context, key string, message proto.Message) error {
	anyMsg, err := internal.ToAny(message)
	if err != nil {
		return err
	}

	_, err = c.clpb.CompleteActivity(ctx, &pb.ActivityRequest{
		Key:     key,
		Message: anyMsg,
	})
	return err
}

func (c *Client) CompleteRun(ctx context.Context, workflow, run string) error {
	_, err := c.clpb.CompleteRun(ctx, &pb.CompleteRequest{
		Workflow: workflow,
		Run:      run,
	})
	return err
}

func (c *Client) ListBootstrapEvents(ctx context.Context, workflow, run string) ([]reflex.Event, error) {
	rl, err := c.clpb.ListBootstrapEvents(ctx, &pb.ListBootstrapRequest{
		Workflow: workflow,
		Run:      run,
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

func (c *Client) Stream(ctx context.Context, after string,
	opts ...reflex.StreamOption) (reflex.StreamClient, error) {

	sFn := reflex.WrapStreamPB(func(ctx context.Context, req *reflexpb.StreamRequest) (reflex.StreamClientPB, error) {
		return c.clpb.Stream(ctx, req)
	})

	return sFn(ctx, after, opts...)
}
