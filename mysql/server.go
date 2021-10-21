// Package server provides the replay grpc server, a server side logical client and entrypoint to start server side
// background loops. It is used when running a replay server or when embedding replay into a user application.
package mysql

import (
	"context"
	"github.com/luno/reflex"
	"google.golang.org/grpc"

	"github.com/corverroos/replay/internal"
	pb "github.com/corverroos/replay/internal/replaypb"
)

var _ pb.ReplayServer = (*Server)(nil)

// NewServer returns a new server with the provided namespace to DBClient mapping.
func NewServer(clFunc func(namespace string) (*DBClient, error)) *Server {
	return &Server{
		clFunc:  clFunc,
		rserver: reflex.NewServer(),
	}
}

// Server implements the replay grpc server.
//
// It supports mapping of namespaces to DBClients that allows custom persistence layer configuration per
// namespace e.g., access policies, data retention policies, hardware specs.
//
// It uses the server.DBClient's server methods.
type Server struct {
	pb.ReplayServer
	rserver *reflex.Server
	clFunc  func(namespace string) (*DBClient, error)
}

func (s *Server) clientFromKey(key string) (*DBClient, error) {
	k, err := internal.DecodeKey(key)
	if err != nil {
		return nil, err
	}

	return s.clFunc(k.Namespace)
}

func (s *Server) RunWorkflow(ctx context.Context, req *pb.RunRequest) (*pb.OK, error) {
	cl, err := s.clientFromKey(req.Key)
	if err != nil {
		return nil, err
	}

	ok, err := cl.runWorkflowServer(ctx, req.Key, req.Message)
	return &pb.OK{Ok: ok}, err
}

func (s *Server) SignalRun(ctx context.Context, req *pb.SignalRequest) (*pb.OK, error) {
	cl, err := s.clFunc(req.Namespace)
	if err != nil {
		return nil, err
	}

	ok, err := cl.signalRunServer(ctx, req.Namespace, req.Workflow, req.Run, req.Signal, req.Message, req.ExternalId)
	return &pb.OK{Ok: ok}, err
}

func (s *Server) RequestActivity(ctx context.Context, req *pb.KeyMessage) (*pb.Empty, error) {
	cl, err := s.clientFromKey(req.Key)
	if err != nil {
		return nil, err
	}

	return new(pb.Empty), cl.requestActivityServer(ctx, req.Key, req.Message)
}

func (s *Server) RespondActivity(ctx context.Context, req *pb.KeyMessage) (*pb.Empty, error) {
	cl, err := s.clientFromKey(req.Key)
	if err != nil {
		return nil, err
	}

	return new(pb.Empty), cl.RespondActivityServer(ctx, req.Key, req.Message)
}

func (s *Server) EmitOutput(ctx context.Context, req *pb.KeyMessage) (*pb.Empty, error) {
	cl, err := s.clientFromKey(req.Key)
	if err != nil {
		return nil, err
	}

	return new(pb.Empty), cl.emitOutputServer(ctx, req.Key, req.Message)
}

func (s *Server) CompleteRun(ctx context.Context, req *pb.CompleteRequest) (*pb.Empty, error) {
	cl, err := s.clientFromKey(req.Key)
	if err != nil {
		return nil, err
	}

	return new(pb.Empty), cl.CompleteRun(ctx, req.Key)
}

func (s *Server) RestartRun(ctx context.Context, req *pb.RunRequest) (*pb.Empty, error) {
	cl, err := s.clientFromKey(req.Key)
	if err != nil {
		return nil, err
	}

	return new(pb.Empty), cl.restartRunServer(ctx, req.Key, req.Message)
}

func (s *Server) ListBootstrapEvents(ctx context.Context, req *pb.ListBootstrapRequest) (*pb.Events, error) {
	cl, err := s.clientFromKey(req.Key)
	if err != nil {
		return nil, err
	}

	el, err := cl.ListBootstrapEvents(ctx, req.Key, req.Before)
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
	cl, err := s.clFunc(req.Namespace)
	if err != nil {
		return err
	}

	return s.rserver.Stream(cl.Stream(req.Namespace, req.Workflow, req.Run), req.Req, srv)
}

func (s *Server) Stop() {
	s.rserver.Stop()
}

// Register registers the replay server with the grpc server.
func Register(s *grpc.Server, srv pb.ReplayServer) {
	pb.RegisterReplayServer(s, srv)
}
