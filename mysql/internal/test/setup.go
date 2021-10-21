package test

import (
	"context"
	"database/sql"
	"github.com/corverroos/replay/mysql"
	"net"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/corverroos/replay"
	"github.com/corverroos/replay/client"
	pb "github.com/corverroos/replay/internal/replaypb"
)

// SetupGRPC starts a replay grpc server and returns a connected client.
func SetupGRPC(t *testing.T) (replay.Client, *sql.DB) {
	scl, dbc := Setup(t)
	srv, addr := newServer(t, scl)

	t.Cleanup(srv.Stop)

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	jtest.RequireNil(t, err)

	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	cl := client.New(conn)

	// Wait until connected to avoid startup race.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	for {
		if conn.GetState() == connectivity.Ready {
			break
		}

		if !conn.WaitForStateChange(ctx, conn.GetState()) {
			require.Fail(t, "grpc client connect timeout")
		}
	}

	return cl, dbc
}

// newServer starts and returns a replay grpc server and its address.
func newServer(t *testing.T, cl *mysql.DBClient) (*mysql.Server, string) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	jtest.RequireNil(t, err)

	grpcServer := grpc.NewServer()

	srv := mysql.NewServer(func(_ string) (*mysql.DBClient, error) {
		return cl, nil
	})

	pb.RegisterReplayServer(grpcServer, srv)

	go func() {
		err := grpcServer.Serve(l)
		jtest.RequireNil(t, err)
	}()

	return srv, l.Addr().String()
}
