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

	"github.com/pulse-stream/pulse/pkg/api/pulse/v1/pulsepb"
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
