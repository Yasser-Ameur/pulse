// Command quickstart exercises the pulse.v1 protocol end to end against a
// running broker: create a topic, publish a batch, replay it, and advance a
// consumer cursor. Start the broker first (make build && bin/pulse-server).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/pulse-stream/pulse/internal/domain/consumer"
	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/domain/partition"
	"github.com/pulse-stream/pulse/internal/domain/topic"
	"github.com/pulse-stream/pulse/internal/infrastructure/grpc/client"
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

	name, err := topic.NewName("quickstart")
	if err != nil {
		return fmt.Errorf("topic name: %w", err)
	}
	if _, err := c.CreateTopic(ctx, name.String(), topic.DefaultConfig(), 1); err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("create topic: %w", err)
		}
	}

	offsets, err := c.Publish(ctx, name, partition.ID(0), []message.Message{
		{Key: "user-42", Payload: []byte(`{"event":"order_placed","sku":"a1"}`), ContentType: "application/json"},
		{Key: "user-42", Payload: []byte(`{"event":"order_shipped","sku":"a1"}`), ContentType: "application/json"},
	})
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	log.Printf("published offsets %v", offsets)

	sub := consumer.Subscription{
		Consumer:  consumer.ID("quickstart"),
		Topic:     name,
		Partition: partition.ID(0),
	}
	// A stored cursor is the NEXT offset to consume, so acknowledge one past
	// the last record processed (docs/Guarantees.md §3). Acking the last
	// delivered offset would redeliver that record on every resume.
	var next offset.Offset
	err = c.Subscribe(ctx, sub, func(r message.Record) error {
		fmt.Printf("%d\t%s\n", r.Offset, r.Message.Payload)
		next = r.Offset + 1
		return nil
	})
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	if _, err := c.Ack(ctx, sub.Consumer, name, sub.Partition, next); err != nil {
		return fmt.Errorf("ack: %w", err)
	}
	log.Printf("acked; this consumer resumes at offset %d", next)
	return nil
}
