package grpc_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	grpctransport "github.com/pulse-stream/pulse/internal/infrastructure/grpc"
	"github.com/pulse-stream/pulse/internal/infrastructure/timeutil"
	"github.com/pulse-stream/pulse/internal/testutil"
	"github.com/pulse-stream/pulse/pkg/api/pulse/v1/pulsepb"
)

func TestDefaultOptions(t *testing.T) {
	opts := grpctransport.DefaultOptions()
	require.Equal(t, 64<<20, opts.MaxRecvMsgSize)
	require.Equal(t, 64<<20, opts.MaxSendMsgSize)
	require.Equal(t, 10*time.Second, opts.GraceTimeout)
	require.Nil(t, opts.Logger)
}

// TestStartAndConnectionsTracksLiveConnections pins that Start begins serving
// in the background (returning without blocking) and that Connections counts
// a client connection up while it is open and back down once it closes.
func TestStartAndConnectionsTracksLiveConnections(t *testing.T) {
	app := testutil.Start(t, testutil.Options{}).Broker()
	srv := grpctransport.NewServer(app, timeutil.SystemClock{}, grpctransport.Options{GraceTimeout: 2 * time.Second})
	t.Cleanup(func() { srv.GracefulStop(context.Background()) })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, srv.Start(ln))

	require.Eventually(t, func() bool { return srv.Connections() == 0 }, time.Second, 10*time.Millisecond)

	conn, err := grpclib.NewClient(ln.Addr().String(), grpclib.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	client := pulsepb.NewBrokerClient(conn)
	_, err = client.BrokerInfo(context.Background(), &pulsepb.BrokerInfoRequest{})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return srv.Connections() > 0 }, time.Second, 10*time.Millisecond,
		"Connections() never reflected the open client connection")

	require.NoError(t, conn.Close())
	require.Eventually(t, func() bool { return srv.Connections() == 0 }, time.Second, 10*time.Millisecond,
		"Connections() never dropped back to 0 after the client closed")
}
