package client_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pulse-stream/pulse/internal/testutil"
	"github.com/pulse-stream/pulse/pkg/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestSubscribeResumesOnDisconnect pins the Follow resume contract: once the
// stream has delivered at least one record, a transport failure must not
// return an error to the caller. Subscribe keeps redialing (with backoff)
// until the caller's context ends, at which point it returns that context's
// error.
func TestSubscribeResumesOnDisconnect(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	c, err := client.Dial(inst.Addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	if _, err := c.CreateTopic(ctx, "resume", client.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := c.Publish(ctx, "resume", 0, client.Message{Payload: []byte("first")}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	subCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	received := make(chan client.Record, 1)
	subErr := make(chan error, 1)
	go func() {
		subErr <- c.Subscribe(subCtx, "resume", 0, client.SubscribeOptions{Follow: true}, func(r client.Record) error {
			received <- r
			return nil
		})
	}()

	select {
	case <-received:
	case err := <-subErr:
		t.Fatalf("subscribe ended before delivering a record: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the first record")
	}

	// Cutting the transport must not surface as an error: Follow keeps
	// retrying against the now-unreachable address.
	inst.Stop(t)
	select {
	case err := <-subErr:
		t.Fatalf("subscribe returned after disconnect instead of resuming: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-subErr:
		if !errors.Is(err, context.Canceled) && status.Code(err) != codes.Canceled {
			t.Fatalf("subscribe error after cancel = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("subscribe did not stop after cancel")
	}
}
