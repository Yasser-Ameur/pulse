package grpc_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	grpctransport "github.com/Yasser-Ameur/pulse/internal/infrastructure/grpc"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/timeutil"
	"github.com/Yasser-Ameur/pulse/internal/testutil"
	"github.com/Yasser-Ameur/pulse/pkg/api/pulse/v1/pulsepb"
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

// TestGracefulStopForcesStopOnExpiredContext pins the force-close branch: a
// stream still in flight (a follow subscribe, which never finishes on its
// own) must not block GracefulStop past the caller's context.
func TestGracefulStopForcesStopOnExpiredContext(t *testing.T) {
	app := testutil.Start(t, testutil.Options{}).Broker()
	srv := grpctransport.NewServer(app, timeutil.SystemClock{}, grpctransport.Options{GraceTimeout: 30 * time.Second})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, srv.Start(ln))

	conn, err := grpclib.NewClient(ln.Addr().String(), grpclib.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = pulsepb.NewBrokerClient(conn).CreateTopic(context.Background(), &pulsepb.CreateTopicRequest{Name: "orders", Partitions: 1})
	require.NoError(t, err)

	// Follow on an existing, empty partition blocks waiting for new records,
	// giving GracefulStop a genuinely in-flight RPC to force-close.
	stream, err := pulsepb.NewPubSubClient(conn).Subscribe(context.Background(), &pulsepb.SubscribeRequest{
		Topic: "orders", Partition: 0, Follow: true,
	})
	require.NoError(t, err)
	// Block until the server has actually accepted the stream, so
	// GracefulStop below has a real in-flight RPC to force-close.
	require.Eventually(t, func() bool { return srv.Connections() > 0 }, time.Second, 10*time.Millisecond)

	done := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		srv.GracefulStop(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("GracefulStop did not force-stop within its context deadline")
	}
	_, _ = stream.Recv()
}
