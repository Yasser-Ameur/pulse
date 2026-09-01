package grpc

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/pulse-stream/pulse/pkg/api/pulse/v1/pulsepb"
)

// authStubBroker answers BrokerInfo unconditionally, for exercising the auth
// interceptors without a real broker.
type authStubBroker struct {
	pulsepb.UnimplementedBrokerServer
}

func (authStubBroker) BrokerInfo(context.Context, *pulsepb.BrokerInfoRequest) (*pulsepb.BrokerInfoResponse, error) {
	return &pulsepb.BrokerInfoResponse{}, nil
}

// authStubPubSub answers Subscribe with a single response, for exercising the
// stream auth interceptor without a real broker.
type authStubPubSub struct {
	pulsepb.UnimplementedPubSubServer
}

func (authStubPubSub) Subscribe(_ *pulsepb.SubscribeRequest, stream grpc.ServerStreamingServer[pulsepb.SubscribeResponse]) error {
	return stream.Send(&pulsepb.SubscribeResponse{})
}

// dialAuthServer starts a bufconn server with the given token set installed
// on both interceptor chains, plus a health service, and returns clients
// dialed against it.
func dialAuthServer(t *testing.T, tokens []string) (pulsepb.BrokerClient, pulsepb.PubSubClient, healthv1.HealthClient) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	t.Cleanup(func() { _ = lis.Close() })

	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(unaryInterceptor(nil), unaryAuthInterceptor(tokens)),
		grpc.ChainStreamInterceptor(streamInterceptor(nil), streamAuthInterceptor(tokens)),
	)
	pulsepb.RegisterBrokerServer(s, authStubBroker{})
	pulsepb.RegisterPubSubServer(s, authStubPubSub{})
	hs := health.NewServer()
	hs.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	healthv1.RegisterHealthServer(s, hs)
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
	return pulsepb.NewBrokerClient(conn), pulsepb.NewPubSubClient(conn), healthv1.NewHealthClient(conn)
}

func withToken(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func TestUnaryAuthRejectsWithoutToken(t *testing.T) {
	client, _, _ := dialAuthServer(t, []string{"secret"})
	_, err := client.BrokerInfo(context.Background(), &pulsepb.BrokerInfoRequest{})
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("code = %v, want %v (err=%v)", got, codes.Unauthenticated, err)
	}
}

func TestUnaryAuthAcceptsValidToken(t *testing.T) {
	client, _, _ := dialAuthServer(t, []string{"secret"})
	ctx := withToken(context.Background(), "secret")
	if _, err := client.BrokerInfo(ctx, &pulsepb.BrokerInfoRequest{}); err != nil {
		t.Fatalf("BrokerInfo() error = %v, want nil", err)
	}
}

func TestStreamAuthRejectsWithoutToken(t *testing.T) {
	_, pubsub, _ := dialAuthServer(t, []string{"secret"})
	stream, err := pubsub.Subscribe(context.Background(), &pulsepb.SubscribeRequest{})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	_, err = stream.Recv()
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("code = %v, want %v (err=%v)", got, codes.Unauthenticated, err)
	}
}

func TestStreamAuthAcceptsValidToken(t *testing.T) {
	_, pubsub, _ := dialAuthServer(t, []string{"secret"})
	ctx := withToken(context.Background(), "secret")
	stream, err := pubsub.Subscribe(ctx, &pulsepb.SubscribeRequest{})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv() error = %v, want nil", err)
	}
}

func TestHealthCheckExemptFromAuth(t *testing.T) {
	_, _, healthClient := dialAuthServer(t, []string{"secret"})
	resp, err := healthClient.Check(context.Background(), &healthv1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check() error = %v, want nil (health should be exempt)", err)
	}
	if resp.Status != healthv1.HealthCheckResponse_SERVING {
		t.Fatalf("status = %v, want SERVING", resp.Status)
	}
}
