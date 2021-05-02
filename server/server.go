// Package server provides the replay grpc server, a server side logical client and entrypoint to start server side
// background loops. It is used when running a replay server or when embedding replay into a user application.
package server

import (
	"context"
	"database/sql"

	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"google.golang.org/grpc"

	"github.com/corverroos/replay/internal"
	"github.com/corverroos/replay/internal/db"
	pb "github.com/corverroos/replay/internal/replaypb"
	"github.com/corverroos/replay/internal/signal"
	"github.com/corverroos/replay/internal/sleep"
)

var _ pb.ReplayServer = (*Server)(nil)

// Server implements the replay grpc server. It uses the server.DBClient's server methods.
type Server struct {
	rserver *reflex.Server
	cl      *DBClient
}

func (s *Server) RunWorkflow(ctx context.Context, req *pb.RunRequest) (*pb.OK, error) {
	ok, err := s.cl.runWorkflowServer(ctx, req.Key, req.Message)
	return &pb.OK{Ok: ok}, err
}

func (s *Server) SignalRun(ctx context.Context, req *pb.SignalRequest) (*pb.OK, error) {
	ok, err := s.cl.signalRunServer(ctx, req.Namespace, req.Workflow, req.Run, int(req.SignalType), req.Message, req.ExternalId)
	return &pb.OK{Ok: ok}, err
}

func (s *Server) RequestActivity(ctx context.Context, req *pb.ActivityMessage) (*pb.Empty, error) {
	return swallowErrDupPB(s.cl.requestActivityServer(ctx, req.Key, req.Message))
}

func (s *Server) RespondActivity(ctx context.Context, req *pb.ActivityMessage) (*pb.Empty, error) {
	return swallowErrDupPB(s.cl.RespondActivityServer(ctx, req.Key, req.Message))
}

func (s *Server) CompleteRun(ctx context.Context, req *pb.CompleteRequest) (*pb.Empty, error) {
	return new(pb.Empty), s.cl.CompleteRun(ctx, req.Key)
}

func (s *Server) RestartRun(ctx context.Context, req *pb.RunRequest) (*pb.Empty, error) {
	return new(pb.Empty), s.cl.restartRunServer(ctx, req.Key, req.Message)
}

func (s *Server) ListBootstrapEvents(ctx context.Context, req *pb.ListBootstrapRequest) (*pb.Events, error) {
	el, err := s.cl.ListBootstrapEvents(ctx, req.Key)
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

func (s *Server) Stream(req *pb.StreamRequest, srv pb.Replay_StreamServer) error {
	return s.rserver.Stream(s.cl.Stream(req.Namespace), req.Req, srv)
}

func New(cl *DBClient) *Server {
	return &Server{
		cl:      cl,
		rserver: reflex.NewServer(),
	}
}

func (s *Server) Stop() {
	s.rserver.Stop()
}

// Register registers the replay server with the grpc server.
func Register(s *grpc.Server, srv pb.ReplayServer) {
	pb.RegisterReplayServer(s, srv)
}

// StartLoops starts server-side background loops.
func StartLoops(getCtx func() context.Context, cl *DBClient, cstore reflex.CursorStore, dbc *sql.DB) {
	sleep.Register(getCtx, cl, cstore, dbc)
	signal.Register(getCtx, cl, cstore, dbc)
	db.FillGaps(dbc, cl.Events())
}

func swallowErrDupPB(err error) (*pb.Empty, error) {
	if errors.Is(err, internal.ErrDuplicate) {
		// NoReturnErr: Swallow
		return new(pb.Empty), nil
	} else if err != nil {
		return nil, err
	}
	return new(pb.Empty), nil
}
