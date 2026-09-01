package client_test

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/pulse-stream/pulse/pkg/api/pulse/v1/pulsepb"
	"github.com/pulse-stream/pulse/pkg/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// authInterceptor requires metadata key "authorization" to equal
// "Bearer good-token", mirroring the broker's own token-auth contract.
func authInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || len(md.Get("authorization")) != 1 || md.Get("authorization")[0] != "Bearer good-token" {
		return nil, status.Error(codes.Unauthenticated, "missing or wrong token")
	}
	return handler(ctx, req)
}

func dialViaBufconn(t *testing.T, dialer grpc.DialOption, opts ...client.Option) *client.Client {
	t.Helper()
	dialOpts := append([]client.Option{client.WithDialOptions(dialer, grpc.WithTransportCredentials(insecure.NewCredentials()))}, opts...)
	c, err := client.Dial("passthrough:///bufnet", dialOpts...)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestWithTokenSendsBearerMetadata pins that WithToken attaches the
// "authorization: Bearer <token>" gRPC metadata on every call, and that a
// missing or wrong token maps to ErrUnauthenticated.
func TestWithTokenSendsBearerMetadata(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	t.Cleanup(func() { _ = lis.Close() })
	s := grpc.NewServer(grpc.ChainUnaryInterceptor(authInterceptor))
	pulsepb.RegisterBrokerServer(s, &pulsepb.UnimplementedBrokerServer{})
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	dialer := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) })

	t.Run("good token", func(t *testing.T) {
		c := dialViaBufconn(t, dialer, client.WithToken("good-token"))
		_, err := c.Info(context.Background())
		if status.Code(err) == codes.Unauthenticated {
			t.Fatalf("Info() with a good token was rejected: %v", err)
		}
	})

	t.Run("missing token", func(t *testing.T) {
		c := dialViaBufconn(t, dialer)
		_, err := c.Info(context.Background())
		if !errors.Is(err, client.ErrUnauthenticated) {
			t.Fatalf("Info() error = %v, want ErrUnauthenticated", err)
		}
	})

	t.Run("wrong token", func(t *testing.T) {
		c := dialViaBufconn(t, dialer, client.WithToken("bad-token"))
		_, err := c.Info(context.Background())
		if !errors.Is(err, client.ErrUnauthenticated) {
			t.Fatalf("Info() error = %v, want ErrUnauthenticated", err)
		}
	})
}
