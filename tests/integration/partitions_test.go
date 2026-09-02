// Package integration: multi-partition topic behavior over the real gRPC
// transport - per-partition isolation and ordering, independent cursors, and
// recovery of every partition across a restart.
package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/Yasser-Ameur/pulse/internal/testutil"
	"github.com/Yasser-Ameur/pulse/pkg/client"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMultiPartitionTopic(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	c := dial(t, inst)
	ctx := context.Background()

	tp, err := c.CreateTopic(ctx, "orders", client.TopicConfig{Partitions: 3})
	require.NoError(t, err)
	require.Equal(t, "orders", tp.Name)

	// Publish distinct, per-partition payloads, several records each, so
	// ordering and isolation are both checkable.
	for p := int32(0); p < 3; p++ {
		for i := 0; i < 3; i++ {
			_, err := c.Publish(ctx, "orders", p, client.Message{
				Payload: []byte(fmt.Sprintf("p%d-m%d", p, i)),
			})
			require.NoError(t, err)
		}
	}

	// Each partition sees only its own records, in order.
	for p := int32(0); p < 3; p++ {
		got := consume(t, c, "orders", p, client.SubscribeOptions{})
		require.Len(t, got, 3, "partition %d", p)
		for i, r := range got {
			require.Equal(t, fmt.Sprintf("p%d-m%d", p, i), string(r.Message.Payload))
			require.Equal(t, int64(i), r.Offset)
		}
	}

	// Acking one partition does not affect another's cursor.
	const consumer = "consumer-a"
	cursor, err := c.Ack(ctx, consumer, "orders", 0, 2)
	require.NoError(t, err)
	require.Equal(t, int64(2), cursor)

	got1 := consume(t, c, "orders", 1, client.SubscribeOptions{Consumer: consumer})
	require.Len(t, got1, 3, "partition 1 cursor should be untouched by partition 0's ack")

	got0 := consume(t, c, "orders", 0, client.SubscribeOptions{Consumer: consumer})
	require.Len(t, got0, 1, "partition 0 should resume from its own cursor")
	require.Equal(t, "p0-m2", string(got0[0].Message.Payload))

	// Restart recovers every partition with its records and per-partition
	// cursors intact.
	inst = inst.Restart(t)
	c = dial(t, inst)

	for p := int32(0); p < 3; p++ {
		got := consume(t, c, "orders", p, client.SubscribeOptions{})
		require.Len(t, got, 3, "partition %d after restart", p)
	}
	gotResumed := consume(t, c, "orders", 0, client.SubscribeOptions{Consumer: consumer})
	require.Len(t, gotResumed, 1, "partition 0 cursor should survive restart")
	require.Equal(t, "p0-m2", string(gotResumed[0].Message.Payload))

	// Publishing to a partition beyond the topic's count is rejected.
	_, err = c.Publish(ctx, "orders", 3, client.Message{Payload: []byte("nope")})
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}
