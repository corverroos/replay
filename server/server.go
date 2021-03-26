package server

import (
	"context"
	"database/sql"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/internal"
	"github.com/corverroos/replay/internal/db"
	pb "github.com/corverroos/replay/internal/replaypb"
	"github.com/corverroos/replay/internal/signal"
	"github.com/corverroos/replay/internal/sleep"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"github.com/luno/reflex/reflexpb"
	"google.golang.org/grpc"
)

var _ pb.ReplayServer = (*Server)(nil)

// Server implements the addresses grpc server.
type Server struct {
	rserver *reflex.Server
	dbc     *sql.DB
}

func (s *Server) RunWorkflow(ctx context.Context, req *pb.RunRequest) (*pb.Empty, error) {
	b, err := internal.Marshal(req.Message)
	if err != nil {
		return nil, err
	}

	return new(pb.Empty), db.Insert(ctx, s.dbc, internal.ShortKey(req.Workflow, req.Run), internal.CreateRun, b)
}

func (s *Server) SignalRun(ctx context.Context, req *pb.SignalRequest) (*pb.Empty, error) {
	return new(pb.Empty), signal.Insert(ctx, s.dbc, req.Workflow, req.Run, int(req.SignalType), req.Message, req.ExternalId)
}

func (s *Server) RequestActivity(ctx context.Context, req *pb.ActivityRequest) (*pb.Empty, error) {
	b, err := internal.Marshal(req.Message)
	if err != nil {
		return nil, err
	}

	return swallowErrDup(db.Insert(ctx, s.dbc, req.Key, internal.ActivityRequest, b))
}

func (s *Server) CompleteActivity(ctx context.Context, req *pb.ActivityRequest) (*pb.Empty, error) {
	b, err := internal.Marshal(req.Message)
	if err != nil {
		return nil, err
	}

	return swallowErrDup(db.Insert(ctx, s.dbc, req.Key, internal.ActivityResponse, b))
}

func (s *Server) CompleteRun(ctx context.Context, req *pb.CompleteRequest) (*pb.Empty, error) {
	return swallowErrDup(db.Insert(ctx, s.dbc, internal.ShortKey(req.Workflow, req.Run), internal.CompleteRun, nil))
}

func (s *Server) ListBootstrapEvents(ctx context.Context, req *pb.ListBootstrapRequest) (*pb.Events, error) {
	el, err := db.ListBootstrapEvents(ctx, s.dbc, req.Workflow, req.Run)
	if err != nil {
		return nil, err
	}

	var resp pb.Events
	for _, event := range el {
		e, err := internal.EventToProto(&event)
		if err != nil {
			return nil, err
		}

		resp.Events = append(resp.Events, e)
	}

	return &resp, nil
}

func (s *Server) Stream(req *reflexpb.StreamRequest, srv pb.Replay_StreamServer) error {
	return s.rserver.Stream(db.ToStream(s.dbc), req, srv)
}

func New(dbc *sql.DB) *Server {
	return &Server{
		dbc:     dbc,
		rserver: reflex.NewServer(),
	}
}

func (s *Server) Stop() {
	s.rserver.Stop()
}

func Register(s *grpc.Server, srv pb.ReplayServer) {
	pb.RegisterReplayServer(s, srv)
}

func StartLoops(ctx context.Context, cl replay.Client, cstore reflex.CursorStore, dbc *sql.DB) {
	sleep.Register(ctx, cl, cstore, dbc)
	signal.Register(ctx, cl, cstore, dbc)
	db.FillGaps(dbc)
}

func swallowErrDup(err error) (*pb.Empty, error) {
	if errors.Is(err, replay.ErrDuplicate) {
		return new(pb.Empty), nil
	} else if err != nil {
		return nil, err
	}
	return new(pb.Empty), nil
}
