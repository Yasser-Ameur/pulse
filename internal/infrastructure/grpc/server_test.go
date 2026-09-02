package grpc

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Yasser-Ameur/pulse/pkg/api/pulse/v1/pulsepb"
)

// panicOnceBroker panics on its first BrokerInfo call and succeeds on every
// call after, so a test can observe that a handler panic is recovered as
// codes.Internal without taking down the server.
type panicOnceBroker struct {
	pulsepb.UnimplementedBrokerServer
	calls int
}

func (b *panicOnceBroker) BrokerInfo(context.Context, *pulsepb.BrokerInfoRequest) (*pulsepb.BrokerInfoResponse, error) {
	b.calls++
	if b.calls == 1 {
		panic("boom")
	}
	return &pulsepb.BrokerInfoResponse{}, nil
}

// TestUnaryInterceptorRecoversPanic pins that a handler panic is converted to
// codes.Internal and that the server keeps accepting and serving RPCs
// afterwards, using the exact interceptor NewServer installs.
func TestUnaryInterceptorRecoversPanic(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	t.Cleanup(func() { _ = lis.Close() })

	s := grpc.NewServer(grpc.ChainUnaryInterceptor(unaryInterceptor(nil)))
	pulsepb.RegisterBrokerServer(s, &panicOnceBroker{})
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := pulsepb.NewBrokerClient(conn)

	_, err = client.BrokerInfo(context.Background(), &pulsepb.BrokerInfoRequest{})
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("first call code = %v, want %v (err=%v)", got, codes.Internal, err)
	}

	if _, err := client.BrokerInfo(context.Background(), &pulsepb.BrokerInfoRequest{}); err != nil {
		t.Fatalf("second call error = %v, want nil (server should keep serving)", err)
	}
}

// panicOncePubSub panics on its first Subscribe stream and serves one empty
// batch on every stream after, mirroring panicOnceBroker for the stream path.
type panicOncePubSub struct {
	pulsepb.UnimplementedPubSubServer
	calls int
}

func (p *panicOncePubSub) Subscribe(_ *pulsepb.SubscribeRequest, stream grpc.ServerStreamingServer[pulsepb.SubscribeResponse]) error {
	p.calls++
	if p.calls == 1 {
		panic("boom")
	}
	return stream.Send(&pulsepb.SubscribeResponse{})
}

// TestStreamInterceptorRecoversPanic pins the same contract for streaming
// handlers, using the exact interceptor NewServer installs.
func TestStreamInterceptorRecoversPanic(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	t.Cleanup(func() { _ = lis.Close() })

	s := grpc.NewServer(grpc.ChainStreamInterceptor(streamInterceptor(nil)))
	pulsepb.RegisterPubSubServer(s, &panicOncePubSub{})
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := pulsepb.NewPubSubClient(conn)

	stream, err := client.Subscribe(context.Background(), &pulsepb.SubscribeRequest{})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	_, err = stream.Recv()
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("first stream code = %v, want %v (err=%v)", got, codes.Internal, err)
	}

	stream, err = client.Subscribe(context.Background(), &pulsepb.SubscribeRequest{})
	if err != nil {
		t.Fatalf("second Subscribe() error = %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("second stream Recv() error = %v, want nil (server should keep serving)", err)
	}
}
