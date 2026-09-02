package client_test

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Yasser-Ameur/pulse/internal/testutil"
	"github.com/Yasser-Ameur/pulse/pkg/client"
)

func dial(t *testing.T, addr string, opts ...client.Option) *client.Client {
	t.Helper()
	c, err := client.Dial(addr, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestClientListDeleteTopic(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	c := dial(t, inst.Addr)
	ctx := context.Background()

	_, err := c.CreateTopic(ctx, "orders", client.TopicConfig{Partitions: 1})
	require.NoError(t, err)

	topics, err := c.ListTopics(ctx)
	require.NoError(t, err)
	require.Len(t, topics, 1)
	require.Equal(t, "orders", topics[0].Name)

	require.NoError(t, c.DeleteTopic(ctx, "orders"))

	topics, err = c.ListTopics(ctx)
	require.NoError(t, err)
	require.Empty(t, topics)
}

func TestClientDeleteTopicNotFoundMapsSentinel(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	c := dial(t, inst.Addr)

	err := c.DeleteTopic(context.Background(), "missing")
	require.ErrorIs(t, err, client.ErrNotFound)
}

func TestClientAckAdvancesCursor(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	c := dial(t, inst.Addr)
	ctx := context.Background()

	_, err := c.CreateTopic(ctx, "orders", client.TopicConfig{Partitions: 1})
	require.NoError(t, err)
	_, err = c.Publish(ctx, "orders", 0, client.Message{Payload: []byte("x")})
	require.NoError(t, err)

	cursor, err := c.Ack(ctx, "c1", "orders", 0, 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), cursor)
}

func TestClientInfoReportsRunningState(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	c := dial(t, inst.Addr)

	info, err := c.Info(context.Background())
	require.NoError(t, err)
	require.Equal(t, "running", info.State)
	require.Equal(t, "test", info.Version)
}

func TestWithCallTimeoutAppliesWhenContextHasNoDeadline(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	c := dial(t, inst.Addr, client.WithCallTimeout(time.Nanosecond))

	// A near-zero call timeout with no caller deadline should time out before
	// the broker can answer, proving WithCallTimeout was actually applied.
	_, err := c.Info(context.Background())
	require.Error(t, err)
}

func TestWithMaxMsgBytesIsAcceptedAtDial(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	c, err := client.Dial(inst.Addr, client.WithMaxMsgBytes(1<<20))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	_, err = c.Info(context.Background())
	require.NoError(t, err)
}

// TestWithTLSDialingPlaintextBrokerFailsHandshake proves WithTLS's config is
// actually wired into the dial rather than silently ignored: a TLS client
// against a plaintext-only broker must fail at the handshake, not connect.
func TestWithTLSDialingPlaintextBrokerFailsHandshake(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	c, err := client.Dial(inst.Addr, client.WithTLS(&tls.Config{InsecureSkipVerify: true}), client.WithCallTimeout(2*time.Second))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	_, err = c.Info(context.Background())
	require.Error(t, err)
}
