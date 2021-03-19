package server

import (
	"context"
	"database/sql"

	"github.com/corverroos/replay/db"
	"github.com/corverroos/replay/internal"
	pb "github.com/corverroos/replay/replaypb"
	"github.com/corverroos/replay/signal"
	"github.com/golang/protobuf/proto"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"github.com/luno/reflex/reflexpb"
)

var _ pb.ReplayServer = (*Server)(nil)

// Server implements the addresses grpc server.
type Server struct {
	rserver *reflex.Server
	dbc     *sql.DB
}

func (s *Server) RunWorkflow(ctx context.Context, req *pb.RunRequest) (*pb.Empty, error) {
	b, err := proto.Marshal(req.Message)
	if err != nil {
		return nil, err
	}

	return new(pb.Empty), db.Insert(ctx, s.dbc, internal.ShortKey(req.Workflow, req.Run), db.CreateRun, b)
}

func (s *Server) SignalRun(ctx context.Context, req *pb.SignalRequest) (*pb.Empty, error) {
	return new(pb.Empty), signal.Insert(ctx, s.dbc, req.Workflow, req.Run, int(req.SignalType), req.Message, req.ExternalId)
}

func (s *Server) RequestActivity(ctx context.Context, req *pb.ActivityRequest) (*pb.Empty, error) {
	b, err := proto.Marshal(req.Message)
	if err != nil {
		return nil, err
	}

	return swallowErrDup(db.Insert(ctx, s.dbc, req.Key, db.ActivityRequest, b))
}

func (s *Server) CompleteActivity(ctx context.Context, req *pb.ActivityRequest) (*pb.Empty, error) {
	b, err := proto.Marshal(req.Message)
	if err != nil {
		return nil, err
	}

	return swallowErrDup(db.Insert(ctx, s.dbc, req.Key, db.ActivityResponse, b))
}

func (s *Server) CompleteRun(ctx context.Context, req *pb.CompleteRequest) (*pb.Empty, error) {
	return swallowErrDup(db.Insert(ctx, s.dbc, internal.ShortKey(req.Workflow, req.Run), db.CompleteRun, nil))
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

func swallowErrDup(err error) (*pb.Empty, error) {
	if errors.Is(err, db.ErrDuplicate) {
		return new(pb.Empty), nil
	} else if err != nil {
		return nil, err
	}
	return new(pb.Empty), nil
}
