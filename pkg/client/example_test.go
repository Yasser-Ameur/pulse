package client_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pulse-stream/pulse/internal/testutil"
	"github.com/pulse-stream/pulse/pkg/client"
)

// Example demonstrates the publish/subscribe/ack cycle against a broker
// started here with testutil for a self-contained, compilable example. It
// carries no "Output:" comment, so go test compiles it but does not run it;
// a real caller dials whatever address its broker is actually listening on.
// A broker that requires auth would add client.WithToken(token) here; a
// topic with several partitions would route with client.PartitionForKey
// instead of the fixed partition 0 used below.
func Example() {
	inst := testutil.Start(new(testing.T), testutil.Options{})

	c, err := client.Dial(inst.Addr)
	if err != nil {
		panic(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	if _, err := c.CreateTopic(ctx, "orders", client.TopicConfig{Partitions: 1}); err != nil {
		panic(err)
	}

	if _, err := c.Publish(ctx, "orders", 0, client.Message{
		Key:     "user-42",
		Payload: []byte(`{"event":"order_placed"}`),
	}); err != nil {
		panic(err)
	}

	// A stored cursor is the NEXT offset to consume, so ack one past the
	// last record processed, not that record's own offset.
	var next int64
	err = c.Subscribe(ctx, "orders", 0, client.SubscribeOptions{Consumer: "quickstart"}, func(r client.Record) error {
		fmt.Printf("%d\t%s\n", r.Offset, r.Message.Payload)
		next = r.Offset + 1
		return nil
	})
	if err != nil {
		panic(err)
	}

	if _, err := c.Ack(ctx, "quickstart", "orders", 0, next); err != nil {
		panic(err)
	}
}
