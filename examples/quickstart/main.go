// Command quickstart exercises the pulse.v1 protocol end to end against a
// running broker: create a topic, publish a batch, replay it, and advance a
// consumer cursor. Start the broker first (make build && bin/pulse-server).
//
// It depends only on the public github.com/Yasser-Ameur/pulse/pkg/client
// package, so it builds as-is against any Pulse broker without importing
// anything internal.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/Yasser-Ameur/pulse/pkg/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9090", "broker address")
	flag.Parse()
	if err := run(*addr); err != nil {
		log.Fatal(err)
	}
}

func run(addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c, err := client.Dial(addr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer func() { _ = c.Close() }()

	const topicName = "quickstart"
	if _, err := c.CreateTopic(ctx, topicName, client.TopicConfig{Partitions: 1}); err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("create topic: %w", err)
		}
	}

	offsets, err := c.Publish(ctx, topicName, 0,
		client.Message{Key: "user-42", Payload: []byte(`{"event":"order_placed","sku":"a1"}`), ContentType: "application/json"},
		client.Message{Key: "user-42", Payload: []byte(`{"event":"order_shipped","sku":"a1"}`), ContentType: "application/json"},
	)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	log.Printf("published offsets %v", offsets)

	const consumerID = "quickstart"
	// A stored cursor is the NEXT offset to consume, so acknowledge one past
	// the last record processed (docs/Guarantees.md §3). Acking the last
	// delivered offset would redeliver that record on every resume.
	var next int64
	err = c.Subscribe(ctx, topicName, 0, client.SubscribeOptions{Consumer: consumerID}, func(r client.Record) error {
		fmt.Printf("%d\t%s\n", r.Offset, r.Message.Payload)
		next = r.Offset + 1
		return nil
	})
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	if _, err := c.Ack(ctx, consumerID, topicName, 0, next); err != nil {
		return fmt.Errorf("ack: %w", err)
	}
	log.Printf("acked; this consumer resumes at offset %d", next)
	return nil
}
