// Package integration exercises the full broker over the real gRPC transport:
// publish → consume → acknowledge → cursor resume, durability across restart,
// live follow streaming, and concurrent producers.
package integration

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pulse-stream/pulse/internal/application/services"
	grpctransport "github.com/pulse-stream/pulse/internal/infrastructure/grpc"
	"github.com/pulse-stream/pulse/internal/infrastructure/logging"
	"github.com/pulse-stream/pulse/internal/infrastructure/metrics"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/log"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/metadata"
	"github.com/pulse-stream/pulse/internal/infrastructure/timeutil"
	"github.com/pulse-stream/pulse/internal/testutil"
	"github.com/pulse-stream/pulse/pkg/api/pulse/v1/pulsepb"
	"github.com/pulse-stream/pulse/pkg/client"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const consumerA = "consumer-a"

func dial(t *testing.T, inst *testutil.Instance) *client.Client {
	t.Helper()
	c, err := client.Dial(inst.Addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestPublishConsumeAckResume(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	c := dial(t, inst)
	ctx := context.Background()

	tp, err := c.CreateTopic(ctx, "orders", client.TopicConfig{Partitions: 1})
	require.NoError(t, err)
	require.Equal(t, "orders", tp.Name)

	msgs := []client.Message{
		{Key: "a", Payload: []byte("one"), ContentType: "text/plain"},
		{Key: "b", Payload: []byte("two")},
		{Key: "c", Payload: []byte("three")},
	}
	offs, err := c.Publish(ctx, "orders", 0, msgs...)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2}, offs)

	// Replay from the start sees every message in order.
	got := consume(t, c, "orders", 0, client.SubscribeOptions{})
	require.Len(t, got, 3)
	for i, want := range []string{"one", "two", "three"} {
		require.Equal(t, want, string(got[i].Message.Payload), "offset %d", got[i].Offset)
	}

	// Ack to offset 1; the stored cursor is the next offset to consume
	// (Protocol.md §Cursor resume), so a fresh subscription resumes at 1.
	cursor, err := c.Ack(ctx, consumerA, "orders", 0, 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), cursor)

	got = consume(t, c, "orders", 0, client.SubscribeOptions{Consumer: consumerA})
	require.Len(t, got, 2)
	require.Equal(t, "two", string(got[0].Message.Payload))
	require.Equal(t, "three", string(got[1].Message.Payload))
}

func TestDurabilityAcrossRestart(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	c := dial(t, inst)
	ctx := context.Background()

	_, err := c.CreateTopic(ctx, "events", client.TopicConfig{Partitions: 1})
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		_, err := c.Publish(ctx, "events", 0, client.Message{Payload: []byte(fmt.Sprintf("m%d", i))})
		require.NoError(t, err)
	}

	// Restart the broker over the same data directory.
	inst = inst.Restart(t)
	c = dial(t, inst)

	info, err := c.Info(ctx)
	require.NoError(t, err)
	require.Equal(t, "running", info.State)

	got := consume(t, c, "events", 0, client.SubscribeOptions{})
	require.Len(t, got, 5)
	for i, r := range got {
		require.Equal(t, int64(i), r.Offset)
		require.Equal(t, fmt.Sprintf("m%d", i), string(r.Message.Payload))
	}
}

func TestFollowStreamsLiveMessages(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	c := dial(t, inst)
	ctx := context.Background()

	_, err := c.CreateTopic(ctx, "tail", client.TopicConfig{Partitions: 1})
	require.NoError(t, err)

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	recv := make(chan client.Record, 16)
	subErr := make(chan error, 1)
	go func() {
		subErr <- c.Subscribe(subCtx, "tail", 0, client.SubscribeOptions{Follow: true},
			func(r client.Record) error { recv <- r; return nil })
	}()

	// The subscriber must first catch up to the log end (empty here), then
	// receive the live publish.
	time.Sleep(200 * time.Millisecond)
	_, err = c.Publish(ctx, "tail", 0, client.Message{Payload: []byte("live")})
	require.NoError(t, err)

	select {
	case r := <-recv:
		require.Equal(t, "live", string(r.Message.Payload))
		require.Equal(t, int64(0), r.Offset)
	case err := <-subErr:
		t.Fatalf("subscribe failed: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for live message")
	}

	cancel()
	select {
	case err := <-subErr:
		// Canceling the client context surfaces as a Canceled RPC error (or
		// clean EOF); either means the stream stopped.
		if err != nil && status.Code(err) != codes.Canceled {
			t.Fatalf("subscribe error after cancel: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("subscribe did not stop after cancel")
	}
}

// TestShutdownCancelsFollowerImmediately pins docs/Concurrency.md §6 step 4:
// Broker.Shutdown cancels every live follow stream immediately instead of
// waiting for it to notice via the gRPC transport's own grace window. It
// builds the broker directly (rather than through testutil.Instance, whose
// Stop calls GracefulStop before Shutdown) so it can call Shutdown on its own
// and observe that it alone unblocks the follower.
func TestShutdownCancelsFollowerImmediately(t *testing.T) {
	dir := t.TempDir()
	logger := logging.NewNopLogger()
	meta, err := metadata.OpenPebble(dir + "/meta")
	require.NoError(t, err)
	factory := log.NewFactory(dir, log.Config{}, logger)
	app := services.NewBroker(services.BrokerOptions{
		MetadataStore: meta,
		LogFactory:    factory,
		Clock:         timeutil.SystemClock{},
		Logger:        logger,
		Metrics:       metrics.NoopRecorder{},
		ListenAddr:    "127.0.0.1:0",
		Version:       "test",
		ReadLimit:     512,
		ReadMaxBytes:  1 << 20,
	})
	require.NoError(t, app.Start(context.Background()))

	srv := grpctransport.NewServer(app, timeutil.SystemClock{}, grpctransport.Options{
		GraceTimeout: 5 * time.Second,
		Logger:       logger,
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() { _ = srv.Serve(ln) }()

	c, err := client.Dial(ln.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	ctx := context.Background()
	_, err = c.CreateTopic(ctx, "drain", client.TopicConfig{Partitions: 1})
	require.NoError(t, err)

	// This step observes the raw pulsepb stream directly rather than through
	// pkg/client.Subscribe: pkg/client now resumes a Follow stream on
	// codes.Unavailable, which would hide the very signal this test pins.
	rawConn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = rawConn.Close() })
	stream, err := pulsepb.NewPubSubClient(rawConn).Subscribe(context.Background(), &pulsepb.SubscribeRequest{
		Topic: "drain", Follow: true,
	})
	require.NoError(t, err)

	subErr := make(chan error, 1)
	go func() {
		for {
			if _, err := stream.Recv(); err != nil {
				subErr <- err
				return
			}
		}
	}()
	time.Sleep(200 * time.Millisecond) // let the follower reach its blocking wait

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	require.NoError(t, app.Shutdown(shutdownCtx))
	require.Less(t, time.Since(start), 2*time.Second, "Shutdown should not wait for the grace period")

	select {
	case err := <-subErr:
		require.Equal(t, codes.Unavailable, status.Code(err), "subscribe error = %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("subscribe did not stop within 2s of Shutdown")
	}
}

func TestConcurrentProducers(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	c := dial(t, inst)
	ctx := context.Background()

	_, err := c.CreateTopic(ctx, "race", client.TopicConfig{Partitions: 1})
	require.NoError(t, err)

	const (
		producers   = 8
		perProducer = 25
	)

	var wg sync.WaitGroup
	errCh := make(chan error, producers)
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cc, err := client.Dial(inst.Addr)
			if err != nil {
				errCh <- err
				return
			}
			defer func() { _ = cc.Close() }()
			for i := 0; i < perProducer; i++ {
				if _, err := cc.Publish(ctx, "race", 0, client.Message{
					Payload: []byte(fmt.Sprintf("p%d-%d", id, i)),
				}); err != nil {
					errCh <- err
					return
				}
			}
		}(p)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("publish: %v", err)
	}

	got := consume(t, c, "race", 0, client.SubscribeOptions{})
	require.Len(t, got, producers*perProducer)
	seen := make(map[int64]bool, len(got))
	for _, r := range got {
		require.False(t, seen[r.Offset], "duplicate offset %d", r.Offset)
		seen[r.Offset] = true
	}
	// Every producer's messages are present exactly once.
	payloads := make(map[string]int)
	for _, r := range got {
		payloads[string(r.Message.Payload)]++
	}
	require.Len(t, payloads, producers*perProducer)
	for _, n := range payloads {
		require.Equal(t, 1, n)
	}
}

// consume replays a non-follow subscription, collecting all records.
func consume(t *testing.T, c *client.Client, topic string, partition int32, opts client.SubscribeOptions) []client.Record {
	t.Helper()
	var got []client.Record
	err := c.Subscribe(context.Background(), topic, partition, opts, func(r client.Record) error {
		got = append(got, r)
		return nil
	})
	require.NoError(t, err)
	return got
}
