package grpc

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/pulse-stream/pulse/pkg/api/pulse/v1/pulsepb"
)

// TestUnaryAuthRejectsMalformedAuthorizationHeader pins the interceptor's
// other rejection branches beyond "header absent": present metadata with no
// authorization key, and an authorization value that is not the "Bearer "
// scheme.
func TestUnaryAuthRejectsMalformedAuthorizationHeader(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
	}{
		{
			name: "metadata present but no authorization key",
			ctx:  metadata.AppendToOutgoingContext(context.Background(), "x-other", "value"),
		},
		{
			name: "wrong scheme",
			ctx:  metadata.AppendToOutgoingContext(context.Background(), "authorization", "Basic secret"),
		},
		{
			name: "empty bearer token",
			ctx:  metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, _, _ := dialAuthServer(t, []string{"secret"})
			_, err := client.BrokerInfo(tt.ctx, &pulsepb.BrokerInfoRequest{})
			if got := status.Code(err); got != codes.Unauthenticated {
				t.Fatalf("code = %v, want %v (err=%v)", got, codes.Unauthenticated, err)
			}
		})
	}
}
